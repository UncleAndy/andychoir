//! Входящий WS-сервер и обработка входящих соединений от удалённых хостов.

use super::*;
use super::dedup::check_dedup;
use super::discovery::handle_hello;
use super::message::{pack_capabilities, unpack_message, value_to_event, NetMessage};
use axum::extract::ws::WebSocket;
use axum::Router;

/// Входящий WS-сервер на /net.
pub(crate) async fn run_incoming_server(inner: Arc<NetInner>) {
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
pub(crate) async fn handle_incoming(inner: Arc<NetInner>, socket: WebSocket) {
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
    // B1: состояние аутентификации входящего соединения. До получения
    // валидного Auth мы НЕ обрабатываем Capabilities/Hello/Event и не
    // регистрируем узел в incoming_senders/origin_tools (fail-closed).
    // Если список токенов пуст — аутентификация отключена (открытый режим,
    // обратная совместимость с узлами без токена).
    let mut authed = inner.cfg.token.is_empty();
    while let Some(msg) = ws_stream.next().await {
        let Ok(axum::extract::ws::Message::Text(text)) = msg else { continue };
        let Some(netmsg) = unpack_message(&text) else { continue };
        match netmsg {
            // B1: первое сообщение должно быть Auth. Сверяем с NetConfig.token.
            NetMessage::Auth { token } => {
                if inner.cfg.token.iter().any(|t| t == &token) {
                    authed = true;
                    info!("[Хост] Net: входящее соединение аутентифицировано (токен совпал).");
                } else {
                    error!("[Хост] Net: отклонено соединение — неверный токен аутентификации.");
                    // Закрываем соединение без регистрации узла. Отправляем
                    // сообщение через канал задачи-отправителя (ws_sink уже
                    // перемещён туда), затем разрываем.
                    let _ = out_tx.try_send("{\"error\":\"auth failed\"}".to_string());
                    // Даём задаче-отправителю шанс отослать и завершаем цикл.
                    break;
                }
            }
            // B1: любое не-Auth сообщение до аутентификации — отбрасываем.
            _ if !authed => {
                warn!("[Хост] Net: проигнорировано сообщение до аутентификации (Auth требуется первым).");
                continue;
            }
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
            NetMessage::Hello { source_id, seq, neighbors, tools } => {
                handle_hello(&inner, source_id, neighbors, tools, seq).await;
            }
            // P1: Bye (graceful leave) — обрабатывается в P6.
            NetMessage::Bye { source_id } => {
                super::discovery::handle_bye(&inner, &source_id).await;
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
                let Some(ev) = value_to_event(&event) else { continue };
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

#[cfg(test)]
mod b1_auth_tests {
    use super::*;
    use crate::plugin::engine::ToolDef;
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    /// Найти свободный порт (для уникального bind в каждом тесте).
    async fn free_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    /// Поднять входящий сервер с заданным конфигом, вернуть inner.
    async fn spawn_server(cfg: NetConfig) -> Arc<NetInner> {
        let inner = NetInner::new_test_with_cfg(cfg);
        let inner_c = inner.clone();
        tokio::spawn(async move { run_incoming_server(inner_c).await });
        // Дать серверу забиндиться.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        inner
    }

    /// Подождать, пока origin появится (present=true) или гарантированно
    /// отсутствует (present=false) в incoming_senders — с таймаутом.
    async fn wait_registration(inner: &Arc<NetInner>, origin: &str, present: bool) -> bool {
        for _ in 0..50 {
            let has = inner.incoming_senders.read().await.contains_key(origin);
            if has == present {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        inner.incoming_senders.read().await.contains_key(origin) == present
    }

    fn caps_msg(origin: &str) -> NetMessage {
        NetMessage::Capabilities {
            origin: origin.to_string(),
            source_id: origin.to_string(),
            neighbors: vec![],
            tools: vec![ToolDef {
                name: "calculator".into(),
                description: "".into(),
                parameters_json: "{}".into(),
                session_local: false,
            }],
        }
    }

    #[tokio::test]
    async fn b1_rejects_without_auth() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec!["sekret".to_string()]; // аутентификация ВКЛЮЧЕНА
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        // Шлём Capabilities БЕЗ Auth — должно игнорироваться.
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;

        let registered = wait_registration(&inner, "node-B", false).await;
        assert!(registered, "без Auth узел НЕ должен регистрироваться");
        assert!(
            !inner.origin_tools.read().await.contains_key("node-B"),
            "origin_tools не должен содержать node-B без Auth"
        );
        let _ = ws.close(None).await;
    }

    #[tokio::test]
    async fn b1_rejects_wrong_token() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec!["sekret".to_string()];
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        // Неверный токен → сервер должен разорвать и не регистрировать.
        let _ = ws
            .send(Message::Text(
                serde_json::to_string(&NetMessage::Auth { token: "wrong".to_string() })
                    .unwrap()
                    .into(),
            ))
            .await;
        // Даже если клиент пошлёт Capabilities после — не должно зарегистрироваться.
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;

        let registered = wait_registration(&inner, "node-B", false).await;
        assert!(registered, "при неверном токене узел НЕ должен регистрироваться");
    }

    #[tokio::test]
    async fn b1_accepts_valid_token() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec!["sekret".to_string()];
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        // Валидный Auth, затем Capabilities.
        let _ = ws
            .send(Message::Text(
                serde_json::to_string(&NetMessage::Auth { token: "sekret".to_string() })
                    .unwrap()
                    .into(),
            ))
            .await;
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;

        let registered = wait_registration(&inner, "node-B", true).await;
        assert!(registered, "при верном токене узел ДОЛЖЕН зарегистрироваться");
        assert!(
            inner.origin_tools.read().await.contains_key("node-B"),
            "origin_tools должен содержать node-B после Auth"
        );
        let _ = ws.close(None).await;
    }

    #[tokio::test]
    async fn b1_open_mode_no_token_required() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec![]; // пустой список → открытый режим (обратная совместимость)
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        // Без Auth, но сервер в открытом режиме — должен принять.
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;

        let registered = wait_registration(&inner, "node-B", true).await;
        assert!(registered, "в открытом режиме (пустой token) узел ДОЛЖЕН регистрироваться без Auth");
        let _ = ws.close(None).await;
    }
}
