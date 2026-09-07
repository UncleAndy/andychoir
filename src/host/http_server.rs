//! Входящий HTTP-сервер (transparent transport).
//!
//! Принцип «всё есть плагин»: хост не знает семантику HTTP-запросов, он только
//! маршрутизирует их. Плагины `front:http` регистрируют слушателей
//! `(port, path) -> target` через WIT `http-server`. Когда клиент делает
//! HTTP-запрос на зарегистрированный адрес, хост:
//!   1. формирует `Event { source: "host:http", target: "<port>:<path>",
//!      topic: "request", payload: JSON(method,path,query,headers,body) }`;
//!   2. публикует его в шину;
//!   3. ждёт ответ (`topic:"response"` с тем же request_id) с таймаутом;
//!   4. возвращает ответ клиенту.

use crate::ai::host::types::Event;
use crate::plugin::engine;
use crate::{error, info};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Router;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, oneshot};

/// Внутреннее состояние HTTP-сервера.
#[derive(Clone)]
pub struct HttpServerInner {
    /// Канал для публикации событий в шину.
    tx: mpsc::Sender<Event>,
    /// Карта ожидающих HTTP-ответов: request_id -> канал ответа.
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Event>>>>,
}

/// Глобальный внутренний регистр (для deliver_http_response из bus.rs).
static INNER: std::sync::OnceLock<Arc<HttpServerInner>> = std::sync::OnceLock::new();

fn set_inner(inner: Arc<HttpServerInner>) {
    let _ = INNER.set(inner);
}

pub fn get_inner() -> Option<Arc<HttpServerInner>> {
    INNER.get().cloned()
}

/// Handle для HTTP-серверов.
pub struct HttpServerHandle {
    pub handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Сформировать Event из HTTP-запроса. Возвращает (request_id, event).
pub fn build_http_event(
    listener: &engine::HttpListener,
    method: &str,
    uri_path: &str,
    query: &str,
    headers: &HeaderMap,
    body: &str,
) -> (String, Event) {
    let request_id = uuid::Uuid::new_v4().to_string();

    let session_id = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let hdrs: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();

    let payload = serde_json::json!({
        "method": method,
        "path": uri_path,
        "query": query,
        "headers": hdrs,
        "body": body,
    });

    let event = Event {
        request_id: request_id.clone(),
        session_id,
        source: "host:http".to_string(),
        // target: "<имя http-фронта>:<port>:<uri>" — на это подписан front:http.
        // (По соглашению имя http-фронта = "front:http". listener.target (агент)
        // плагин использует при пересылке запроса агенту.)
        target: format!("front:http:{}:{}", listener.port, listener.path),
        topic: "request".to_string(),
        payload: payload.to_string(),
    };

    (request_id, event)
}

/// Обработать один HTTP-запрос.
async fn handle_request(
    State(inner): State<Arc<HttpServerInner>>,
    req: Request<Body>,
) -> Response {
    // Определяем слушателя по пути. Порт проверяем опционально: поскольку
    // на каждом порту свой axum-сервер, fallback-обработчик получает только
    // запросы на свой порт. Ищем слушателя по path.
    let path = req.uri().path().to_string();
    info!("[Хост] HTTP-запрос на path={} (method={})", path, req.method());

    // Ищем слушателя в глобальном реестре по пути.
    let listeners = engine::http_listeners_snapshot();
    info!("[Хост] HTTP-слушателей в реестре: {} (ищем {})", listeners.len(), path);
    let listener = listeners.iter().find(|l| l.path == path).cloned();

    let Some(listener) = listener else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("no listener for this path"))
            .unwrap();
    };

    let method = req.method().to_string();
    let uri = req.uri().clone();
    let query = uri.query().unwrap_or("").to_string();
    let headers = req.headers().clone();

    let body_bytes = match axum::body::to_bytes(req.into_body(), 4 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("failed to read body"))
                .unwrap();
        }
    };
    let body = String::from_utf8_lossy(&body_bytes).to_string();

    let (_rid, event) = build_http_event(&listener, &method, &path, &query, &headers, &body);
    let session_id = event.session_id.clone();

    // Регистрируем session_id как локальную сессию хоста и помечаем начало
    // активного запроса (доступ к session_local-инструментам разрешён только
    // пока есть незавершённый запрос от фронта). Фронт-агностичная точка —
    // та же, что у ws/console. request_id генерируется централизованно.
    let request_id = engine::begin_frontend_request(&session_id).await;

    let (resp_tx, resp_rx) = oneshot::channel::<Event>();
    {
        let mut pending = inner.pending.lock().await;
        pending.insert(request_id.clone(), resp_tx);
    }

    if inner.tx.send(event).await.is_err() {
        inner.pending.lock().await.remove(&request_id);
        engine::unregister_local_session(&session_id).await;
        engine::end_frontend_request(&session_id, &request_id).await;
        info!("[Хост] HTTP: bus closed при отправке request_id={}", request_id);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("bus closed"))
            .unwrap();
    }
    info!("[Хост] HTTP: событие опубликовано в шину, ждём ответ (request_id={})", request_id);

    let timeout = std::time::Duration::from_secs(30);
    let resp = match tokio::time::timeout(timeout, resp_rx).await {
        Ok(Ok(ev)) => ev,
        _ => {
            inner.pending.lock().await.remove(&request_id);
            engine::unregister_local_session(&session_id).await;
            engine::mark_request_inactive(&session_id, &request_id).await;
            return Response::builder()
                .status(StatusCode::GATEWAY_TIMEOUT)
                .body(Body::from("timeout waiting for response"))
                .unwrap();
        }
    };

    engine::unregister_local_session(&session_id).await;
    engine::mark_request_inactive(&session_id, &request_id).await;
    build_http_response(&resp)
}

/// Сформировать HTTP-ответ клиенту из ответного события плагина.
///
/// Клиенту отдаётся ПОЛНЫЙ JSON-ответ плагина: `{status, headers, session_id,
/// body}` — а не только поле `body`, чтобы клиент получал `session_id` для
/// работы сессий. Заголовки из ответа плагина (напр. `x-session-id`)
/// переносятся в HTTP-ответ.
pub fn build_http_response(resp: &Event) -> Response {
    let parsed: serde_json::Value = serde_json::from_str(&resp.payload).unwrap_or_default();
    let status = parsed.get("status").and_then(|s| s.as_u64()).unwrap_or(200) as u16;

    // Отдаём клиенту ПОЛНЫЙ JSON-ответ плагина (status, headers, body, session_id),
    // а не только поле body — чтобы клиент получал session_id для работы сессий.
    let response_body = if parsed.is_object() {
        resp.payload.clone()
    } else {
        // Плагин вернул не-JSON — отдаём как есть.
        parsed
            .get("body")
            .and_then(|b| b.as_str())
            .unwrap_or("")
            .to_string()
    };

    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK))
        .header("content-type", "application/json; charset=utf-8");

    // Применяем заголовки из ответа плагина (напр. x-session-id).
    if let Some(headers) = parsed.get("headers").and_then(|h| h.as_array()) {
        for h in headers {
            if let Some(arr) = h.as_array() {
                if arr.len() == 2 {
                    if let (Some(k), Some(v)) = (arr[0].as_str(), arr[1].as_str()) {
                        builder = builder.header(k, v);
                    }
                }
            }
        }
    }

    builder.body(Body::from(response_body)).unwrap()
}

/// Запустить axum-сервер на порту.
fn spawn_server(inner: Arc<HttpServerInner>, port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = Router::new()
            .fallback(handle_request)
            .with_state(inner)
            .into_make_service_with_connect_info::<std::net::SocketAddr>();

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                info!("[Хост] HTTP-сервер слушает 0.0.0.0:{}", port);
                if let Err(e) = axum::serve(listener, app).await {
                    error!("[Хост] HTTP-сервер на :{} упал: {}", port, e);
                }
            }
            Err(e) => {
                error!("[Хост] Не удалось занять порт {}: {}", port, e);
            }
        }
    })
}

/// Запустить HTTP-серверы на всех портах из текущих слушателей.
/// Динамическое добавление портов при рантайм-регистрации — задача на
/// следующий шаг (пока стартовые слушатели из конфига front:http).
pub async fn start_http_servers(tx: mpsc::Sender<Event>) -> HttpServerHandle {
    let inner = Arc::new(HttpServerInner {
        tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
    });
    set_inner(inner.clone());

    let listeners = engine::http_listeners_snapshot();
    let mut handles = Vec::new();
    let mut seen: HashSet<u16> = HashSet::new();
    for l in listeners {
        if seen.insert(l.port) {
            handles.push(spawn_server(inner.clone(), l.port));
        }
    }

    HttpServerHandle { handles }
}

/// Вызывается из dispatch_event, когда пришёл response, ожидаемый HTTP-сервером.
pub async fn deliver_http_response(request_id: &str, ev: Event) -> bool {
    let Some(inner) = get_inner() else {
        return false;
    };
    let tx = inner.pending.lock().await.remove(request_id);
    if let Some(tx) = tx {
        let _ = tx.send(ev);
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn make_listener(port: u16, path: &str, target: &str) -> engine::HttpListener {
        engine::HttpListener {
            port,
            path: path.to_string(),
            target: target.to_string(),
        }
    }

    // build_http_event: target формируется как "<port>:<uri>", session_id из заголовка.
    #[test]
    fn build_http_event_target_format_and_session_from_header() {
        let listener = make_listener(8090, "/query", "agent:*");
        let mut headers = HeaderMap::new();
        headers.insert("x-session-id", HeaderValue::from_static("sess-123"));

        let (_rid, ev) = build_http_event(
            &listener, "POST", "/query", "", &headers, "{\"q\":\"hi\"}",
        );

        assert_eq!(ev.source, "host:http");
        assert_eq!(ev.target, "front:http:8090:/query");
        assert_eq!(ev.topic, "request");
        assert_eq!(ev.session_id, "sess-123");

        let p: serde_json::Value = serde_json::from_str(&ev.payload).unwrap();
        assert_eq!(p["method"], "POST");
        assert_eq!(p["path"], "/query");
        assert_eq!(p["body"], "{\"q\":\"hi\"}");
    }

    // build_http_event: без заголовка session_id генерируется новый (валидный uuid).
    #[test]
    fn build_http_event_generates_session_when_absent() {
        let listener = make_listener(8090, "/query", "agent:*");
        let headers = HeaderMap::new();
        let (_rid, ev) =
            build_http_event(&listener, "GET", "/query", "", &headers, "");
        // uuid v4: 36 символов, 4 дефиса.
        assert_eq!(ev.session_id.len(), 36);
        assert_eq!(ev.session_id.matches('-').count(), 4);
    }

    // build_http_response: отдаёт ПОЛНЫЙ JSON (с session_id), а не только body.
    #[tokio::test]
    async fn build_http_response_returns_full_json_with_session_id() {
        let payload = serde_json::json!({
            "status": 200,
            "headers": [["x-session-id", "sess-abc"]],
            "session_id": "sess-abc",
            "body": "Привет!",
        })
        .to_string();

        let ev = Event {
            request_id: "r1".into(),
            session_id: "sess-abc".into(),
            source: "front:http".into(),
            target: "host:http".into(),
            topic: "response".into(),
            payload,
        };

        let resp = build_http_response(&ev);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/json; charset=utf-8"
        );
        assert_eq!(
            resp.headers().get("x-session-id").unwrap(),
            "sess-abc"
        );

        let body = axum::body::to_bytes(resp.into_body(), 1024)
            .await
            .unwrap()
            .to_vec();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["session_id"], "sess-abc");
        assert_eq!(parsed["body"], "Привет!");
        assert_eq!(parsed["status"], 200);
    }

    // build_http_response: status из ответа плагина применяется к HTTP-статусу.
    #[tokio::test]
    async fn build_http_response_uses_status_from_payload() {
        let payload = serde_json::json!({
            "status": 404,
            "headers": [],
            "session_id": "s",
            "body": "nope",
        })
        .to_string();
        let ev = Event {
            request_id: "r2".into(),
            session_id: "s".into(),
            source: "front:http".into(),
            target: "host:http".into(),
            topic: "response".into(),
            payload,
        };
        let resp = build_http_response(&ev);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
