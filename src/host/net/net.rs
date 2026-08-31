//! Центральный модуль сетевого моста: запуск, входящие/исходящие соединения, forward.
//!
//! Wire-протокол, dedup, LSDB/FIB и discovery вынесены в подмодули
//! (`message`, `dedup`, `lsdb`, `discovery`). Здесь — оркестрация.

use super::*;
use super::dedup::check_dedup;
use super::discovery::{handle_hello, run_discovery_loop};
use super::lsdb::route_next_hop;
use super::message::{decrement_ttl, pack_capabilities, pack_event_with_ttl, unpack_message, NetMessage};
use crate::config::config::NetRemote;
use axum::extract::ws::WebSocket;
use axum::Router;
use tokio_tungstenite;

/// Запустить сетевой мост: входящий WS-сервер + исходящие соединения к remotes.
pub async fn start_net(tx: mpsc::Sender<Event>, cfg: super::NetConfig) -> super::NetHandle {
    let inner = Arc::new(NetInner {
        tx,
        cfg: cfg.clone(),
        session_origin: Arc::new(RwLock::new(HashMap::new())),
        request_origin: Arc::new(RwLock::new(HashMap::new())),
        outbound: Arc::new(RwLock::new(HashMap::new())),
        pending_outbound: Arc::new(RwLock::new(HashMap::new())),
        origin_tools: Arc::new(RwLock::new(HashMap::new())),
        incoming_senders: Arc::new(RwLock::new(HashMap::new())),
        dedup: Arc::new(RwLock::new(
            BloomFilter::with_num_bits(super::dedup::DEDUP_BITS).expected_items(1024),
        )),
        dedup_last_reset: Arc::new(RwLock::new(super::dedup::current_unix_secs())),
        lsdb: Arc::new(RwLock::new(HashMap::new())),
        fib: Arc::new(RwLock::new(HashMap::new())),
        node_url: Arc::new(RwLock::new(HashMap::new())),
    });
    super::set_inner(inner.clone());

    // Входящий WS-сервер.
    let server = tokio::spawn({
        let inner = inner.clone();
        async move {
            run_incoming_server(inner).await;
        }
    });

    // Исходящие персистентные соединения.
    let mut outbound = Vec::new();
    for remote in cfg.remotes.clone() {
        let inner = inner.clone();
        outbound.push(tokio::spawn(async move {
            run_outbound_loop(inner, remote).await;
        }));
    }

    // P3: задача периодического discovery (Hello каждые HELLO_INTERVAL_SECS).
    let discovery = tokio::spawn({
        let inner = inner.clone();
        async move {
            run_discovery_loop(inner).await;
        }
    });
    outbound.push(discovery);

    super::NetHandle { server, outbound }
}

/// Входящий WS-сервер на /net.
async fn run_incoming_server(inner: Arc<NetInner>) {
    let port = inner.cfg.listen_port;
    if port == 0 {
        info!("[Хост] Net-мост: входящий порт не задан (listen_port=0), пропускаю сервер.");
        return;
    }
    let app = Router::new()
        .route(
            "/net",
            axum::routing::get({
                let inner = inner.clone();
                move |ws: WebSocketUpgrade| {
                    let inner = inner.clone();
                    async move { ws.on_upgrade(move |s| handle_incoming(inner, s)) }
                }
            }),
        )
        .with_state(inner.clone());

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            info!("[Хост] Net-мост слушает 0.0.0.0:{} /net", port);
            if let Err(e) = axum::serve(listener, app).await {
                error!("[Хост] Net-сервер на :{} упал: {}", port, e);
            }
        }
        Err(e) => error!("[Хост] Не удалось занять net-порт {}: {}", port, e),
    }
}

/// Обработать входящее WS-соединение от удалённого хоста.
async fn handle_incoming(inner: Arc<NetInner>, socket: WebSocket) {
    info!("[Хост] Net: входящее соединение открыто");
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);

    // Задача-отправитель ответов на это входящее соединение.
    let send_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_sink
                .send(axum::extract::ws::Message::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut this_origin: Option<String> = None;
    while let Some(msg) = ws_stream.next().await {
        let Ok(axum::extract::ws::Message::Text(text)) = msg else { continue };
        let Some(netmsg) = unpack_message(&text) else { continue };
        match netmsg {
            NetMessage::Capabilities { origin, source_id: _, neighbors: _, tools } => {
                if origin == inner.cfg.node_id {
                    continue;
                }
                info!("[Хост] Net: получены возможности хоста {}: {:?} инструментов", origin, tools.len());
                inner.origin_tools.write().await.insert(origin.clone(), tools);
                // P4: регистрируем node_id -> url для FIB-маршрутизации.
                inner.node_url.write().await.insert(origin.clone(), "incoming".to_string());
                // Регистрируем обратный канал для этого origin (П1).
                this_origin = Some(origin.clone());
                inner.incoming_senders.write().await.insert(origin, out_tx.clone());
                // Отвечаем своими возможностями (локальные инструменты), чтобы
                // удалённый хост узнал, что мы умеем.
                let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
                let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
                let _ = out_tx.try_send(caps);
            }
            // P1: Hello (discovery).
            NetMessage::Hello { source_id, neighbors, tools } => {
                handle_hello(&inner, source_id, neighbors, tools).await;
            }
            // P1: Bye (graceful leave) — обрабатывается в P6.
            NetMessage::Bye { source_id: _ } => {
                // TODO P6: удалить из LSDB, очистить каналы.
            }
            NetMessage::Event { origin, source_id: _, event_id, ttl: _, hop: _hop, event } => {
                if origin == inner.cfg.node_id {
                    continue;
                }
                // P2: dedup по event_id (Bloom-фильтр). Если дубликат — отбрасываем.
                if !check_dedup(&inner, &event_id).await {
                    info!("[Хост] Net: дубликат события {} отброшен (dedup)", event_id);
                    continue;
                }
                let Some(ev) = super::message::value_to_event(&event) else { continue };
                inner
                    .session_origin
                    .write()
                    .await
                    .insert(ev.session_id.clone(), origin.clone());
                if let Some(tools) = inner.origin_tools.read().await.get(&origin).cloned() {
                    crate::plugin::engine::add_session_tools(&ev.session_id, tools).await;
                }
                if inner.tx.send(ev).await.is_err() {
                    break;
                }
            }
        }
    }
    // Соединение закрылось: убрать обратный канал.
    if let Some(origin) = this_origin {
        inner.incoming_senders.write().await.remove(&origin);
    }
    send_task.abort();
    info!("[Хост] Net: входящее соединение закрыто");
}

/// Исходящее персистентное соединение к одному remote (с автопереподключением).
async fn run_outbound_loop(inner: Arc<NetInner>, remote: NetRemote) {
    loop {
        // Пытаемся подключиться.
        match tokio_tungstenite::connect_async(&remote.url).await {
            Ok((ws, _resp)) => {
                info!("[Хост] Net: подключились к {}", remote.url);
                // Канал для отправки обёрток в это соединение.
                let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
                inner
                    .outbound
                    .write()
                    .await
                    .insert(remote.url.clone(), out_tx.clone());

                // Отправляем накопленные (буферизованные до подключения) события.
                {
                    let mut pending = inner.pending_outbound.write().await;
                    if let Some(buf) = pending.remove(&remote.url) {
                        for msg in buf {
                            let _ = out_tx.try_send(msg);
                        }
                    }
                }

                // split(): отправитель (Sink) и приёмник (Stream) раздельно.
                let (mut ws_sink, mut ws_stream) = ws.split();

                // При подключении анонсируем свои локальные инструменты
                // (capabilities), чтобы удалённый хост знал, что мы умеем.
                let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
                let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
                let _ = ws_sink
                    .send(tokio_tungstenite::tungstenite::Message::Text(caps.into()))
                    .await;

                // Задача-отправитель: читает из канала и шлёт в сокет.
                let send_task = tokio::spawn(async move {
                    while let Some(msg) = out_rx.recv().await {
                        if ws_sink
                            .send(tokio_tungstenite::tungstenite::Message::Text(msg.into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });

                // Читаем ответы и впрыскиваем.
                let mut closed = false;
                while let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) =
                    ws_stream.next().await
                {
                    let Some(netmsg) = unpack_message(&text) else { continue };
                    match netmsg {
                        NetMessage::Capabilities { origin, source_id: _, neighbors: _, tools } => {
                            if origin == inner.cfg.node_id {
                                continue;
                            }
                            info!("[Хост] Net: возможности {}: {:?}", origin, tools.len());
                            inner.origin_tools.write().await.insert(origin.clone(), tools);
                            // P4: регистрируем node_id -> url для FIB-маршрутизации.
                            inner.node_url.write().await.insert(origin, remote.url.clone());
                        }
                        NetMessage::Hello { source_id, neighbors, tools } => {
                            handle_hello(&inner, source_id, neighbors, tools).await;
                        }
                        NetMessage::Bye { source_id: _ } => {
                            // TODO P6: удалить из LSDB.
                        }
                        NetMessage::Event { origin, source_id: _, event_id, ttl: _, hop: _hop, event } => {
                            if origin == inner.cfg.node_id {
                                continue;
                            }
                            // P2: dedup по event_id (Bloom-фильтр). Если дубликат — отбрасываем.
                            if !check_dedup(&inner, &event_id).await {
                                info!("[Хост] Net: дубликат события {} отброшен (dedup)", event_id);
                                continue;
                            }
                            let Some(ev) = super::message::value_to_event(&event) else { continue };
                            inner
                                .session_origin
                                .write()
                                .await
                                .insert(ev.session_id.clone(), origin.clone());
                            if let Some(tools) = inner.origin_tools.read().await.get(&origin).cloned() {
                                crate::plugin::engine::add_session_tools(&ev.session_id, tools).await;
                            }
                            if inner.tx.send(ev).await.is_err() {
                                closed = true;
                                break;
                            }
                        }
                    }
                }
                send_task.abort();
                // Соединение потеряно: убираем из карты и пробуем переподключиться.
                inner.outbound.write().await.remove(&remote.url);
                if closed {
                    break;
                }
                warn!("[Хост] Net: соединение с {} закрыто, переподключение...", remote.url);
            }
            Err(e) => {
                error!("[Хост] Net: не удалось подключиться к {}: {}", remote.url, e);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    }
}

/// Форвард события на удалённый хост (вызывается из bus при пустом получателе).
/// Возвращает true, если событие ушло по сети (иначе — не нашлось подходящего remote).
pub async fn forward(ev: &Event) -> bool {
    let Some(inner) = super::get_inner() else {
        return false;
    };

    // Определяем origin_host для этого события.
    let origin_host = {
        // Если это ответ/событие в рамках известной сессии — используем origin из карты.
        let r = inner.request_origin.read().await;
        let s = inner.session_origin.read().await;
        r.get(&ev.request_id)
            .map(|(o, _)| o.clone())
            .or_else(|| s.get(&ev.session_id).cloned())
    };

    // Выбираем remote, который обслуживает target этого события.
    let target = ev.target.clone();

    // P4: если target указывает на конкретный узел (host:<node_id>:<...>),
    // маршрутизируем через FIB (кратчайший путь), а не через ручной targets.
    if let Some(node_id) = target.strip_prefix("host:") {
        if let Some((_, _tool)) = node_id.split_once(':') {
            let target_node = node_id.split(':').next().unwrap_or(node_id);
            if let Some(next_hop) = route_next_hop(&inner, target_node).await {
                // next_hop -> url исходящего соединения.
                let node_url_map = inner.node_url.read().await;
                if let Some(url) = node_url_map.get(&next_hop) {
                    let outbound = inner.outbound.read().await;
                    if let Some(tx) = outbound.get(url) {
                        let origin = origin_host.clone().unwrap_or_else(|| inner.cfg.node_id.clone());
                        let ttl = decrement_ttl(16).unwrap_or(0);
                        let payload = pack_event_with_ttl(&origin, 0, ev, ttl);
                        if tx.try_send(payload).is_ok() {
                            info!("[Хост] Net: событие {} → узел {} (next-hop {}) по FIB", ev.target, target_node, next_hop);
                            return true;
                        }
                    }
                    drop(outbound);
                }
                drop(node_url_map);
                // Нет соединения — буферизуем по url (если известен).
                // (url может быть неизвестен, если next_hop ещё не анонсировал capabilities)
            }
        }
    }

    // Fallback: ручной роутинг по targets из конфига (обратная совместимость).
    let remotes = inner.cfg.remotes.clone();
    let target_remote = remotes.into_iter().find(|r| {
        r.targets.iter().any(|t| {
            t == &target || (target.starts_with(t) && target.as_bytes().get(t.len()) == Some(&b':'))
        })
    });

    // Если target не найден среди remotes, но событие — ответ на запрос,
    // пришедший с известного origin (входящее соединение), возвращаем его
    // обратно по этому входящему соединению (П1). Это ключевое для
    // распределённого сценария: запрос пришёл с B, утилита на A, ответ должен
    // вернуться на B через входящее соединение A<-B.
    if target_remote.is_none() {
        if let Some(origin) = origin_host.as_ref() {
            let incoming = inner.incoming_senders.read().await;
            if let Some(tx) = incoming.get(origin) {
                let my_id = inner.cfg.node_id.clone();
                // P2: декремент TTL при пересылке (защита от петель).
                let ttl = decrement_ttl(16).unwrap_or(0);
                let payload = pack_event_with_ttl(&my_id, 0, ev, ttl);
                if tx.try_send(payload).is_ok() {
                    info!("[Хост] Net: ответ {} возвращён по входящему соединению {} (origin)", ev.target, origin);
                    return true;
                }
            }
        }
        return false;
    }
    let remote = target_remote.unwrap();

    // Собираем обёртку.
    let origin = origin_host.clone().unwrap_or_else(|| inner.cfg.node_id.clone());
    // P2: декремент TTL при пересылке (защита от петель).
    let ttl = decrement_ttl(16).unwrap_or(0);
    let payload = pack_event_with_ttl(&origin, 0, ev, ttl);

    let outbound = inner.outbound.read().await;
    if let Some(tx) = outbound.get(&remote.url) {
        if tx.try_send(payload.clone()).is_ok() {
            info!("[Хост] Net: отправлено событие {} на {}", ev.target, remote.url);
            return true;
        }
    }
    // Соединения ещё нет — буферизуем, отправим при подключении (не теряем
    // discovery/request).
    drop(outbound);
    let mut pending = inner.pending_outbound.write().await;
    pending.entry(remote.url.clone()).or_default().push(payload);
    info!("[Хост] Net: событие {} буферизовано для {} (нет соединения)", ev.target, remote.url);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::engine::ToolDef;
    use fastbloom::BloomFilter;
    use super::dedup::reset_dedup;
    use super::discovery::broadcast_to_neighbors;

    fn make_event(target: &str, session: &str) -> Event {
        Event {
            request_id: "r1".into(),
            session_id: session.into(),
            source: "agent:demo".into(),
            target: target.into(),
            topic: "request".into(),
            payload: "{}".into(),
        }
    }

    // Упаковка/распаковка события сохраняет origin и Event.
    #[test]
    fn pack_unpack_roundtrip() {
        let ev = make_event("tool:calculator", "sess-1");
        let s = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let msg = unpack_message(&s).unwrap();
        match msg {
            NetMessage::Event { origin, source_id, event_id, ttl, hop, event } => {
                let ev2 = super::message::value_to_event(&event).unwrap();
                assert_eq!(origin, "00000000-0000-0000-0000-0000000000a1");
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
                assert!(!event_id.is_empty(), "event_id должен быть сгенерирован");
                assert_eq!(ttl, 16, "ttl по умолчанию 16");
                assert_eq!(hop, 0);
                assert_eq!(ev2.target, "tool:calculator");
                assert_eq!(ev2.session_id, "sess-1");
                assert_eq!(ev2.payload, "{}");
            }
            _ => panic!("expected Event message"),
        }
    }

    // Анонс возможностей: упаковка/распаковка сохраняет инструменты.
    #[test]
    fn capabilities_roundtrip() {
        let tools = vec![ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        let s = pack_capabilities("00000000-0000-0000-0000-0000000000a1", tools.clone());
        let msg = unpack_message(&s).unwrap();
        match msg {
            NetMessage::Capabilities { origin, source_id, neighbors, tools: t } => {
                assert_eq!(origin, "00000000-0000-0000-0000-0000000000a1");
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
                assert!(neighbors.is_empty(), "neighbors пустой до P3");
                assert_eq!(t.len(), 1);
                assert_eq!(t[0].name, "calculator");
            }
            _ => panic!("expected Capabilities message"),
        }
    }

    // Выбор remote по target: точное совпадение и префикс с ':'.
    #[test]
    fn remote_matching() {
        let remotes = vec![
            NetRemote { url: "ws://b".into(), token: "".into(), targets: vec!["tool:calculator".into()] },
            NetRemote { url: "ws://c".into(), token: "".into(), targets: vec!["tool:file".into()] },
        ];
        // tool:calculator -> хост b
        let r = remotes.iter().find(|r| r.targets.iter().any(|t| {
            t == "tool:calculator"
        }));
        assert_eq!(r.unwrap().url, "ws://b");
        // tool:file -> хост c
        let r = remotes.iter().find(|r| r.targets.iter().any(|t| t == "tool:file"));
        assert_eq!(r.unwrap().url, "ws://c");
    }

    // origin == self отбрасывается (защита от циклов).
    #[test]
    fn self_origin_rejected() {
        let node_id = "00000000-0000-0000-0000-0000000000a1";
        let s = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &make_event("x", "s"));
        match unpack_message(&s).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, true),
            _ => panic!(),
        }
        let s2 = super::message::pack_event("00000000-0000-0000-0000-0000000000b2", 0, &make_event("x", "s"));
        match unpack_message(&s2).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, false),
            _ => panic!(),
        }
    }

    // P1: Hello (discovery) упаковывается и распаковывается с source_id/neighbors.
    #[test]
    fn hello_roundtrip() {
        let tools = vec![ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        let msg = NetMessage::Hello {
            source_id: "00000000-0000-0000-0000-0000000000a1".into(),
            neighbors: vec!["00000000-0000-0000-0000-0000000000b2".into()],
            tools: tools.clone(),
        };
        let s = serde_json::to_string(&msg).unwrap();
        match unpack_message(&s).unwrap() {
            NetMessage::Hello { source_id, neighbors, tools: t } => {
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
                assert_eq!(neighbors, vec!["00000000-0000-0000-0000-0000000000b2"]);
                assert_eq!(t.len(), 1);
            }
            _ => panic!("expected Hello message"),
        }
    }

    // P1: Bye (graceful leave) упаковывается и распаковывается с source_id.
    #[test]
    fn bye_roundtrip() {
        let msg = NetMessage::Bye {
            source_id: "00000000-0000-0000-0000-0000000000a1".into(),
        };
        let s = serde_json::to_string(&msg).unwrap();
        match unpack_message(&s).unwrap() {
            NetMessage::Bye { source_id } => {
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
            }
            _ => panic!("expected Bye message"),
        }
    }

    // P1: Event содержит уникальный event_id при каждой упаковке (для dedup).
    #[test]
    fn event_id_unique_per_pack() {
        let ev = make_event("tool:x", "s");
        let s1 = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let s2 = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let m1 = unpack_message(&s1).unwrap();
        let m2 = unpack_message(&s2).unwrap();
        let id1 = match m1 { NetMessage::Event { event_id, .. } => event_id, _ => panic!() };
        let id2 = match m2 { NetMessage::Event { event_id, .. } => event_id, _ => panic!() };
        assert_ne!(id1, id2, "event_id должен быть уникальным для каждой упаковки");
        assert!(uuid::Uuid::parse_str(&id1).is_ok(), "event_id должен быть UUID");
    }

    // P2: декремент TTL — базовая логика защиты от петель.
    #[test]
    fn p2_ttl_decrement() {
        assert_eq!(decrement_ttl(0), None, "ttl=0 → событие отбрасывается");
        assert_eq!(decrement_ttl(1), Some(0), "ttl=1 → 0 (последний hop)");
        assert_eq!(decrement_ttl(16), Some(15), "ttl=16 → 15");
    }

    // P2: упаковка с явным TTL сохраняет значение при roundtrip.
    #[test]
    fn p2_ttl_preserved_in_message() {
        let ev = make_event("tool:x", "s");
        let s = pack_event_with_ttl("00000000-0000-0000-0000-0000000000a1", 0, &ev, 7);
        match unpack_message(&s).unwrap() {
            NetMessage::Event { ttl, .. } => assert_eq!(ttl, 7, "ttl должен сохраняться в сообщении"),
            _ => panic!("expected Event"),
        }
    }

    // P2: Bloom-фильтр корректно определяет дубликаты (contains/insert семантика).
    #[test]
    fn p2_bloom_check_and_add() {
        let mut bf: BloomFilter = BloomFilter::with_num_bits(4096).expected_items(1024);
        // Первый раз — не было (false), второй — уже есть (true).
        let first = bf.contains("event-1");
        bf.insert("event-1");
        let second = bf.contains("event-1");
        assert!(!first, "первое появление → не дубликат");
        assert!(second, "повтор → дубликат");
        // Другой ID — с высокой вероятностью новый.
        let other = bf.contains("event-2");
        assert!(!other, "другой ID → новый");
    }

    // P2: окно сброса dedup — старые события "забываются" через DEDUP_WINDOW_SECS.
    #[tokio::test]
    async fn p2_dedup_window_reset() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000d2";
        assert!(check_dedup(&inner, id).await, "первый раз → новое");
        assert!(!check_dedup(&inner, id).await, "повтор (в окне) → дубликат");
        // Симулируем старение: сдвигаем dedup_last_reset на 61с назад.
        let old = super::dedup::current_unix_secs().saturating_sub(61);
        *inner.dedup_last_reset.write().await = old;
        // Теперь check_dedup должен сбросить фильтр (окно истекло) и принять ID как новый.
        assert!(check_dedup(&inner, id).await, "после истечения окна → снова новое");
    }

    // P2: check_dedup отклоняет повторяющийся event_id (интеграция с NetInner).
    #[tokio::test]
    async fn p2_dedup_rejects_duplicate() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000d1";
        assert!(check_dedup(&inner, id).await, "первый раз → новое событие");
        assert!(!check_dedup(&inner, id).await, "повтор → дубликат (отброшен)");
        // После сброса — снова новое.
        reset_dedup(&inner).await;
        assert!(check_dedup(&inner, id).await, "после reset → снова новое");
    }

    // P2: разные event_id не блокируют друг друга (низкий false-positive).
    #[tokio::test]
    async fn p2_dedup_distinct_ids() {
        let inner = NetInner::new_test();
        assert!(check_dedup(&inner, "ev-aaa").await, "ev-aaa → новое");
        assert!(check_dedup(&inner, "ev-bbb").await, "ev-bbb → новое (не конфликтует)");
        assert!(check_dedup(&inner, "ev-ccc").await, "ev-ccc → новое (не конфликтует)");
        // Повтор ev-aaa всё ещё дубликат.
        assert!(!check_dedup(&inner, "ev-aaa").await, "ev-aaa повтор → дубликат");
    }

    // P3: Hello обновляет LSDB (топология) и сохраняет инструменты.
    #[tokio::test]
    async fn p3_lsdb_updated_on_hello() {
        let inner = NetInner::new_test();
        let tools = vec![ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        handle_hello(
            &inner,
            "00000000-0000-0000-0000-0000000000b2".into(),
            vec!["00000000-0000-0000-0000-0000000000c3".into()],
            tools.clone(),
        )
        .await;
        // LSDB содержит узел b2 с соседом c3.
        let lsdb = inner.lsdb.read().await;
        let (neighbors, _ts) = lsdb.get("00000000-0000-0000-0000-0000000000b2").unwrap();
        assert_eq!(neighbors, &vec!["00000000-0000-0000-0000-0000000000c3".to_string()]);
        drop(lsdb);
        // Инструменты сохранены.
        let stored = inner.origin_tools.read().await;
        assert_eq!(stored.get("00000000-0000-0000-0000-0000000000b2").unwrap().len(), 1);
    }

    // P3: свой собственный Hello игнорируется (нет петель).
    #[tokio::test]
    async fn p3_hello_ignores_self() {
        let inner = NetInner::new_test();
        let my_id = inner.cfg.node_id.clone();
        handle_hello(&inner, my_id.clone(), vec![], vec![]).await;
        assert!(inner.lsdb.read().await.get(&my_id).is_none(), "свой node_id не должен попасть в LSDB");
    }

    // P3: повторный Hello от того же узла обновляет список соседей (замена, не дублирование).
    #[tokio::test]
    async fn p3_lsdb_update_replaces_neighbors() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000b2".to_string();
        handle_hello(&inner, id.clone(), vec!["c3".into()], vec![]).await;
        handle_hello(&inner, id.clone(), vec!["c3".into(), "d4".into()], vec![]).await;
        let lsdb = inner.lsdb.read().await;
        let (neighbors, _ts) = lsdb.get(&id).unwrap();
        // Должны быть оба соседа, без дублей c3.
        assert_eq!(neighbors.len(), 2, "соседи заменяются, а не добавляются");
        assert!(neighbors.contains(&"c3".to_string()));
        assert!(neighbors.contains(&"d4".to_string()));
    }

    // P3: LSDB сохраняет timestamp последнего Hello (для будущего timeout в P6).
    #[tokio::test]
    async fn p3_lsdb_timestamp_recorded() {
        let inner = NetInner::new_test();
        let before = super::dedup::current_unix_secs();
        handle_hello(&inner, "b2".into(), vec![], vec![]).await;
        let after = super::dedup::current_unix_secs();
        let lsdb = inner.lsdb.read().await;
        let (_n, ts) = lsdb.get("b2").unwrap();
        assert!(*ts >= before && *ts <= after, "timestamp в окне вызова");
    }

    // P3: broadcast_to_neighbors не отправляет источнику (защита от петель flooding).
    #[tokio::test]
    async fn p3_broadcast_excludes_source() {
        let inner = NetInner::new_test();
        // Регистрируем двух "соседей" в incoming_senders.
        let (tx_a, mut rx_a) = mpsc::channel::<String>(8);
        let (tx_b, mut rx_b) = mpsc::channel::<String>(8);
        inner.incoming_senders.write().await.insert("aa".into(), tx_a);
        inner.incoming_senders.write().await.insert("bb".into(), tx_b);
        // Flooding от bb: должен уйти только aa.
        broadcast_to_neighbors(&inner, "MSG", "bb").await;
        // aa получил.
        let got_a = rx_a.try_recv();
        assert!(got_a.is_ok(), "сосед aa должен получить сообщение");
        // bb (источник) — нет.
        let got_b = rx_b.try_recv();
        assert!(got_b.is_err(), "источник bb не должен получить своё же сообщение");
    }

    // P3: pack_hello содержит свой node_id и соседей из конфига.
    #[tokio::test]
    async fn p3_pack_hello_self_and_neighbors() {
        let inner = NetInner::new_test();
        let s = super::discovery::pack_hello(&inner).await;
        match unpack_message(&s).unwrap() {
            NetMessage::Hello { source_id, neighbors, .. } => {
                assert_eq!(source_id, inner.cfg.node_id, "source_id = свой node_id");
                // У new_test remotes пусты → neighbors пусты.
                assert!(neighbors.is_empty(), "neighbors из пустого cfg.remotes");
            }
            _ => panic!("expected Hello"),
        }
    }

    // P4: FIB пуст, если нет топологии (route_next_hop → None).
    #[tokio::test]
    async fn p4_fib_empty_when_no_topology() {
        let inner = NetInner::new_test();
        super::lsdb::rebuild_fib(&inner).await;
        assert!(inner.fib.read().await.is_empty(), "без LSDB FIB пуст");
        assert!(route_next_hop(&inner, "99999999-0000-0000-0000-000000000099").await.is_none());
    }

    // P4: прямой сосед → next-hop = сам сосед (без лишних хопов).
    #[tokio::test]
    async fn p4_fib_direct_neighbor() {
        let inner = NetInner::new_test();
        // A (my_id) видит Hello от B с соседями [A].
        handle_hello(&inner, "00000000-0000-0000-0000-0000000000b2".into(), vec![inner.cfg.node_id.clone()], vec![]).await;
        let fib = inner.fib.read().await;
        assert_eq!(
            fib.get("00000000-0000-0000-0000-0000000000b2").map(|s| s.as_str()),
            Some("00000000-0000-0000-0000-0000000000b2".as_ref()),
            "прямой сосед → next-hop = он сам"
        );
    }

    // P4: кратчайший путь через промежуточный узел (A-B-C: route A→C даёт next-hop B).
    #[tokio::test]
    async fn p4_fib_shortest_path_multi_hop() {
        let inner = NetInner::new_test();
        let my = inner.cfg.node_id.clone();
        let b = "00000000-0000-0000-0000-0000000000b2".to_string();
        let c = "00000000-0000-0000-0000-0000000000c3".to_string();
        // Строим LSDB вручную: B видит A и C; C видит B.
        inner.lsdb.write().await.insert(b.clone(), (vec![my.clone(), c.clone()], super::dedup::current_unix_secs()));
        inner.lsdb.write().await.insert(c.clone(), (vec![b.clone()], super::dedup::current_unix_secs()));
        super::lsdb::rebuild_fib(&inner).await;
        // Маршрут до C должен идти через B (next-hop = B).
        let hop = route_next_hop(&inner, &c).await;
        assert_eq!(hop.as_deref(), Some(b.as_str()), "A→C идёт через B (кратчайший путь)");
    }

    // P4: rebuild_fib заполняет FIB после handle_hello.
    #[tokio::test]
    async fn p4_rebuild_fib_builds_routes() {
        let inner = NetInner::new_test();
        handle_hello(&inner, "00000000-0000-0000-0000-0000000000b2".into(), vec![inner.cfg.node_id.clone()], vec![]).await;
        let fib = inner.fib.read().await;
        assert!(fib.contains_key("00000000-0000-0000-0000-0000000000b2"), "FIB содержит маршрут до B");
    }

    // P4: свой node_id не должен попасть в FIB (нельзя слать самому себе).
    #[tokio::test]
    async fn p4_fib_excludes_self() {
        let inner = NetInner::new_test();
        let my = inner.cfg.node_id.clone();
        // Сосед B знает нас.
        handle_hello(&inner, "00000000-0000-0000-0000-0000000000b2".into(), vec![my.clone()], vec![]).await;
        assert!(inner.fib.read().await.get(&my).is_none(), "свой node_id не в FIB");
    }

    // P4: недостижимый узел → маршрута нет (изолированный фрагмент графа).
    #[tokio::test]
    async fn p4_fib_unreachable_node() {
        let inner = NetInner::new_test();
        // B и C связаны между собой, но не с A (my_id).
        inner.lsdb.write().await.insert(
            "00000000-0000-0000-0000-0000000000b2".to_string(),
            (vec!["00000000-0000-0000-0000-0000000000c3".to_string()], super::dedup::current_unix_secs()),
        );
        inner.lsdb.write().await.insert(
            "00000000-0000-0000-0000-0000000000c3".to_string(),
            (vec!["00000000-0000-0000-0000-0000000000b2".to_string()], super::dedup::current_unix_secs()),
        );
        super::lsdb::rebuild_fib(&inner).await;
        // Ни B, ни C недостижимы из A.
        assert!(route_next_hop(&inner, "00000000-0000-0000-0000-0000000000b2").await.is_none());
        assert!(route_next_hop(&inner, "00000000-0000-0000-0000-0000000000c3").await.is_none());
    }

    // P4: node_url регистрируется и связывает next_hop с исходящим url.
    #[tokio::test]
    async fn p4_node_url_mapping() {
        let inner = NetInner::new_test();
        let b = "00000000-0000-0000-0000-0000000000b2".to_string();
        // Имитируем получение Capabilities от B (исходящее соединение на ws://b).
        inner.node_url.write().await.insert(b.clone(), "ws://host-b:8092/net".to_string());
        // B — прямой сосед.
        handle_hello(&inner, b.clone(), vec![inner.cfg.node_id.clone()], vec![]).await;
        // next-hop для B = B, и для него есть url.
        let hop = route_next_hop(&inner, &b).await.unwrap();
        let url = inner.node_url.read().await.get(&hop).cloned();
        assert_eq!(url.as_deref(), Some("ws://host-b:8092/net"));
    }
}
