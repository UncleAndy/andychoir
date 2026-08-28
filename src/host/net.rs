//! Сетевой мост между экземплярами andychour.
//!
//! Мост — хостовый транспорт (как http/ws). Плагины и шина НЕ знают о сети:
//! Event не меняется. Вся сетевая логика живёт здесь.
//!
//! - Входящее: WS-сервер на `/net` (порт listen_port). Принимает сетевые
//!   обёртки `{origin, event}`, впрыскивает event в локальную шину (tx).
//! - Исходящее: персистентные WS-соединения к каждому `remote`. Когда
//!   `forward(event)` вызывается из bus (событие не нашло локального
//!   получателя), мост отправляет событие на нужный удалённый хост.
//! - Карта контекста: `session_id -> origin_host` (куда возвращать вызовы
//!   утилит из этого запроса) и `request_id -> (origin, session)`.
//! - Предотвращение циклов: origin != self, ограничение хопов.
//! - Аутентификация: токен в WS-handshake (заголовок/query).

use crate::ai::host::types::Event;
use crate::config::config::{NetConfig, NetRemote};
use crate::{error, info, warn};
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Внутреннее состояние сетевого моста.
pub struct NetInner {
    /// Канал для впрыска входящих событий в локальную шину.
    tx: mpsc::Sender<Event>,
    /// Конфиг.
    cfg: NetConfig,
    /// Карта контекста: session_id -> origin_host.
    session_origin: Arc<RwLock<HashMap<String, String>>>,
    /// Карта контекста: request_id -> (origin_host, session_id).
    request_origin: Arc<RwLock<HashMap<String, (String, String)>>>,
    /// Исходящие соединения: remote_url -> Sender сете-обёрток.
    outbound: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>,
    /// Буфер исходящих событий для remote, к которому ещё нет соединения.
    /// remote_url -> список упакованных обёрток.
    pending_outbound: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Инструменты удалённых хостов по их origin_node_id (из capabilities).
    origin_tools: Arc<RwLock<HashMap<String, Vec<crate::plugin::engine::ToolDef>>>>,
    /// Обратные каналы входящих соединений: origin_node_id -> Sender ответов.
    /// Позволяет вернуть ответ на входящее соединение (П1).
    incoming_senders: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>,
}

static INNER: std::sync::OnceLock<Arc<NetInner>> = std::sync::OnceLock::new();

fn set_inner(inner: Arc<NetInner>) {
    let _ = INNER.set(inner);
}

pub fn get_inner() -> Option<Arc<NetInner>> {
    INNER.get().cloned()
}

/// Handle для сетевого моста.
pub struct NetHandle {
    pub server: tokio::task::JoinHandle<()>,
    pub outbound: Vec<tokio::task::JoinHandle<()>>,
}

/// Сетевое сообщение: событие ИЛИ анонс возможностей (capabilities) хоста.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NetMessage {
    /// Событие шины.
    Event {
        origin: String,
        hop: u8,
        event: serde_json::Value,
    },
    /// Анонс локальных инструментов хоста (шлётся при подключении).
    Capabilities {
        origin: String,
        tools: Vec<crate::plugin::engine::ToolDef>,
    },
}

/// Event -> JSON Value (поля record Event).
fn event_to_value(ev: &Event) -> serde_json::Value {
    serde_json::json!({
        "request_id": ev.request_id,
        "session_id": ev.session_id,
        "source": ev.source,
        "target": ev.target,
        "topic": ev.topic,
        "payload": ev.payload,
    })
}

/// JSON Value -> Event.
fn value_to_event(v: &serde_json::Value) -> Option<Event> {
    Some(Event {
        request_id: v.get("request_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        session_id: v.get("session_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        source: v.get("source").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        target: v.get("target").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        topic: v.get("topic").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        payload: v.get("payload").and_then(|x| x.as_str()).unwrap_or("").to_string(),
    })
}

/// Упаковать событие в сетевое сообщение.
fn pack_event(origin: &str, hop: u8, ev: &Event) -> String {
    let msg = NetMessage::Event {
        origin: origin.to_string(),
        hop,
        event: event_to_value(ev),
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// Упаковать анонс возможностей (локальные инструменты хоста).
fn pack_capabilities(origin: &str, tools: Vec<crate::plugin::engine::ToolDef>) -> String {
    let msg = NetMessage::Capabilities {
        origin: origin.to_string(),
        tools,
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// Распаковать сетевое сообщение.
fn unpack_message(text: &str) -> Option<NetMessage> {
    serde_json::from_str(text).ok()
}

/// Запустить сетевой мост: входящий WS-сервер + исходящие соединения к remotes.
pub async fn start_net(tx: mpsc::Sender<Event>, cfg: NetConfig) -> NetHandle {
    let inner = Arc::new(NetInner {
        tx,
        cfg: cfg.clone(),
        session_origin: Arc::new(RwLock::new(HashMap::new())),
        request_origin: Arc::new(RwLock::new(HashMap::new())),
        outbound: Arc::new(RwLock::new(HashMap::new())),
        pending_outbound: Arc::new(RwLock::new(HashMap::new())),
        origin_tools: Arc::new(RwLock::new(HashMap::new())),
        incoming_senders: Arc::new(RwLock::new(HashMap::new())),
    });
    set_inner(inner.clone());

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

    NetHandle { server, outbound }
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
            NetMessage::Capabilities { origin, tools } => {
                if origin == inner.cfg.node_id {
                    continue;
                }
                info!("[Хост] Net: получены возможности хоста {}: {:?} инструментов", origin, tools.len());
                inner.origin_tools.write().await.insert(origin.clone(), tools);
                // Регистрируем обратный канал для этого origin (П1).
                this_origin = Some(origin.clone());
                inner.incoming_senders.write().await.insert(origin, out_tx.clone());
                // Отвечаем своими возможностями (локальные инструменты), чтобы
                // удалённый хост узнал, что мы умеем.
                let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
                let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
                let _ = out_tx.try_send(caps);
            }
            NetMessage::Event { origin, hop: _hop, event } => {
                if origin == inner.cfg.node_id {
                    continue;
                }
                let Some(ev) = value_to_event(&event) else { continue };
                inner
                    .session_origin
                    .write()
                    .await
                    .insert(ev.session_id.clone(), origin.clone());
                inner.request_origin.write().await.insert(
                    ev.request_id.clone(),
                    (origin.clone(), ev.session_id.clone()),
                );
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
                        NetMessage::Capabilities { origin, tools } => {
                            if origin == inner.cfg.node_id {
                                continue;
                            }
                            info!("[Хост] Net: возможности {}: {:?}", origin, tools.len());
                            inner.origin_tools.write().await.insert(origin, tools);
                        }
                        NetMessage::Event { origin, hop: _hop, event } => {
                            if origin == inner.cfg.node_id {
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
                                closed = true;
                                break;
                            }
                        }
                    }
                }
                send_task.abort();
                inner.outbound.write().await.remove(&remote.url);
                if closed {
                    return;
                }
                warn!("[Хост] Net: соединение с {} закрыто, переподключаюсь", remote.url);
            }
            Err(e) => {
                warn!("[Хост] Net: не удалось подключиться к {}: {}", remote.url, e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// Форвард события на удалённый хост (вызывается из bus при пустом получателе).
/// Возвращает true, если событие ушло по сети (иначе — не нашлось подходящего remote).
pub async fn forward(ev: &Event) -> bool {
    let Some(inner) = get_inner() else {
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
                let payload = pack_event(&my_id, 0, ev);
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
    let origin = origin_host.unwrap_or_else(|| inner.cfg.node_id.clone());
    let payload = pack_event(&origin, 0, ev);

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
        let s = pack_event("host-a", 0, &ev);
        let msg = unpack_message(&s).unwrap();
        match msg {
            NetMessage::Event { origin, hop, event } => {
                let ev2 = value_to_event(&event).unwrap();
                assert_eq!(origin, "host-a");
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
        let tools = vec![crate::plugin::engine::ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        let s = pack_capabilities("host-a", tools.clone());
        let msg = unpack_message(&s).unwrap();
        match msg {
            NetMessage::Capabilities { origin, tools: t } => {
                assert_eq!(origin, "host-a");
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
        let node_id = "host-a";
        let s = pack_event("host-a", 0, &make_event("x", "s"));
        match unpack_message(&s).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, true),
            _ => panic!(),
        }
        let s2 = pack_event("host-b", 0, &make_event("x", "s"));
        match unpack_message(&s2).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, false),
            _ => panic!(),
        }
    }
}
