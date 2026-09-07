//! Входящий WebSocket-сервер (transparent transport).
//!
//! Принцип «всё есть плагин»: хост не знает семантику WS-сообщений, он только
//! маршрутизирует их. Плагины `front:ws` регистрируют слушателей
//! `(port, path) -> target` через WIT `ws-server`.
//!
//! Одно WS-подключение может вести МНОГО сессий: клиент шлёт `session_id`
//! в каждом сообщении (формат как у HTTP-запроса: `{q|message|prompt, session_id}`).
//! Ответы всех сессий клиента идут по его сокету с пометкой `session_id` и
//! `request_id` в JSON.
//!
//! Маршрутизация: `pending[request_id] -> (socket_id, session_id)`. Когда
//! приходит `topic="response"` с этим request_id, хост отправляет ответ на
//! нужный сокет.

use crate::ai::host::types::Event;
use crate::plugin::engine;
use crate::{error, info};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::Router;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Внутреннее состояние WS-сервера.
#[derive(Clone)]
pub struct WsServerInner {
    /// Канал для публикации событий в шину.
    tx: mpsc::Sender<Event>,
    /// Ожидающие ответы: request_id -> (socket_id, session_id).
    pending: Arc<Mutex<HashMap<String, (String, String)>>>,
    /// Сокеты: socket_id -> канал отправки сообщений в WS.
    sockets: Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>,
}

/// Глобальный внутренний регистр (для deliver_ws_response из bus.rs).
static INNER: std::sync::OnceLock<Arc<WsServerInner>> = std::sync::OnceLock::new();

fn set_inner(inner: Arc<WsServerInner>) {
    let _ = INNER.set(inner);
}

pub fn get_inner() -> Option<Arc<WsServerInner>> {
    INNER.get().cloned()
}

/// Handle для WS-серверов.
pub struct WsServerHandle {
    pub handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Запустить WS-сервер на порту (для зарегистрированного слушателя).
fn spawn_server(inner: Arc<WsServerInner>, port: u16, path: String) -> tokio::task::JoinHandle<()> {
    let path = if path.is_empty() { "/ws".to_string() } else { path.clone() };
    let route_path = path.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route(
                &route_path,
                axum::routing::get({
                    let inner = inner.clone();
                    move |ws: WebSocketUpgrade| {
                        let inner = inner.clone();
                        async move { ws.on_upgrade(move |socket| handle_socket(inner, socket)) }
                    }
                }),
            )
            .with_state(inner.clone());

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                info!("[Хост] WS-сервер слушает 0.0.0.0:{} (path {})", port, path);
                if let Err(e) = axum::serve(listener, app).await {
                    error!("[Хост] WS-сервер на :{} упал: {}", port, e);
                }
            }
            Err(e) => {
                error!("[Хост] Не удалось занять WS-порт {}: {}", port, e);
            }
        }
    })
}

/// Обработать одно WS-подключение.
async fn handle_socket(inner: Arc<WsServerInner>, mut socket: WebSocket) {
    let socket_id = uuid::Uuid::new_v4().to_string();
    info!("[Хост] WS-подключение открыто: {}", socket_id);

    // Канал для отправки ответов в этот сокет (deliver_ws_response пишет сюда).
    let (out_tx, mut out_rx) = mpsc::channel::<String>(64);
    {
        let mut sockets = inner.sockets.lock().await;
        sockets.insert(socket_id.clone(), out_tx);
    }

    // Единый цикл через tokio::select!: слушаем И ответы из канала (отправка в
    // сокет), И входящие сообщения клиента (чтение). Так socket не блокируется
    // на recv и одновременно может отправлять (иначе deadlock с send-каналом).
    loop {
        tokio::select! {
            // Ответ, который надо отправить клиенту.
            maybe_out = out_rx.recv() => {
                let Some(msg) = maybe_out else {
                    // Канал закрыт (socket удалён) — выходим.
                    break;
                };
                if socket.send(Message::Text(msg.into())).await.is_err() {
                    info!("[Хост] WS: ошибка отправки в сокет {}", socket_id);
                    break;
                }
            }
            // Входящее сообщение клиента.
            maybe_in = socket.recv() => {
                let Some(msg) = maybe_in else {
                    break; // сокет закрыт
                };
                let Ok(Message::Text(text)) = msg else { continue };
                info!("[Хост] WS-сообщение от {}: {}", socket_id, text);
                // Парсим {q|message|prompt, session_id}.
                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = socket
                            .send(Message::Text(serde_json::json!({ "error": "invalid JSON message" }).to_string()))
                            .await;
                        continue;
                    }
                };
                let message = parsed
                    .get("q")
                    .or_else(|| parsed.get("message"))
                    .or_else(|| parsed.get("prompt"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let session_id = parsed
                    .get("session_id")
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

                let request_id = uuid::Uuid::new_v4().to_string();

                // Регистрируем session_id как локальную сессию хоста (нужно для
                // флага session_local инструментов: чужие сетевые запросы к
                // приватному инструменту допускаются только в рамках реально
                // существующей на этом хосте сессии).
                engine::register_local_session(&session_id).await;

                // Запоминаем: request_id -> (socket_id, session_id).
                {
                    let mut pending = inner.pending.lock().await;
                    pending.insert(request_id.clone(), (socket_id.clone(), session_id.clone()));
                }

                // Формируем Event и публикуем в шину.
                let event = Event {
                    request_id: request_id.clone(),
                    session_id,
                    source: "host:ws".to_string(),
                    target: "front:ws".to_string(),
                    topic: "request".to_string(),
                    payload: message,
                };

                if inner.tx.send(event).await.is_err() {
                    info!("[Хост] WS: bus closed при отправке request_id={}", request_id);
                    let _ = socket
                        .send(Message::Text(
                            serde_json::json!({ "error": "bus closed", "request_id": request_id }).to_string(),
                        ))
                        .await;
                } else {
                    info!("[Хост] WS: событие опубликовано в шину (request_id={})", request_id);
                }
            }
        }
    }

    // Сокет закрылся: чистим pending и реестр.
    let mut sockets = inner.sockets.lock().await;
    sockets.remove(&socket_id);
    let mut pending = inner.pending.lock().await;
    // Снимаем регистрацию всех сессий, привязанных к этому сокету.
    for (sid, _) in pending.iter().filter(|(_, (s, _))| s == &socket_id) {
        engine::unregister_local_session(sid).await;
    }
    pending.retain(|_, (sid, _)| sid != &socket_id);
    info!("[Хост] WS-подключение закрыто: {}", socket_id);
}

/// Запустить WS-серверы на всех портах из текущих слушателей.
pub async fn start_ws_servers(tx: mpsc::Sender<Event>) -> WsServerHandle {
    let inner = Arc::new(WsServerInner {
        tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        sockets: Arc::new(Mutex::new(HashMap::new())),
    });
    set_inner(inner.clone());

    let listeners = engine::ws_listeners_snapshot();
    let mut handles = Vec::new();
    let mut seen: std::collections::HashSet<u16> = std::collections::HashSet::new();
    for l in listeners {
        if seen.insert(l.port) {
            handles.push(spawn_server(inner.clone(), l.port, l.path));
        }
    }
    WsServerHandle { handles }
}

/// Сформировать JSON-ответ клиенту (request_id + session_id + response).
/// Чистая функция — для юнит-тестирования.
pub fn build_ws_response_json(request_id: &str, session_id: &str, response: &str) -> String {
    serde_json::json!({
        "request_id": request_id,
        "session_id": session_id,
        "response": response,
    })
    .to_string()
}

/// Вызывается из dispatch_event, когда пришёл response, ожидаемый WS-сокетом.
/// Отправляет ответ на нужный сокет с пометкой session_id и request_id.
pub async fn deliver_ws_response(request_id: &str, ev: Event) -> bool {
    let Some(inner) = get_inner() else {
        return false;
    };
    let entry = inner.pending.lock().await.remove(request_id);
    if let Some((socket_id, session_id)) = entry {
        info!(
            "[Хост] WS: ответ доставлен сокету {} для request_id={} (session={})",
            socket_id, request_id, session_id
        );
        let payload = build_ws_response_json(request_id, &session_id, &ev.payload);
        let sockets = inner.sockets.lock().await;
        if let Some(tx) = sockets.get(&socket_id) {
            let _ = tx.try_send(payload);
        } else {
            info!("[Хост] WS: сокет {} не найден", socket_id);
        }
        return true;
    }
    info!("[Хост] WS: нет pending для request_id={}", request_id);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // build_ws_response_json: возвращает JSON с request_id, session_id, response.
    #[test]
    fn ws_response_json_contains_ids() {
        let s = build_ws_response_json("req-1", "sess-A", "ответ");
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["request_id"], "req-1");
        assert_eq!(v["session_id"], "sess-A");
        assert_eq!(v["response"], "ответ");
    }

    // Две сессии на одном сокете: каждый ответ несёт свой session_id.
    #[test]
    fn ws_response_json_preserves_session_distinction() {
        let a = build_ws_response_json("r1", "sess-A", "x");
        let b = build_ws_response_json("r2", "sess-B", "y");
        let va: serde_json::Value = serde_json::from_str(&a).unwrap();
        let vb: serde_json::Value = serde_json::from_str(&b).unwrap();
        assert_eq!(va["session_id"], "sess-A");
        assert_eq!(vb["session_id"], "sess-B");
        assert_ne!(va["request_id"], vb["request_id"]);
    }
}
