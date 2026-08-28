use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use uuid::Uuid;

/// Обработать событие от хоста.
///
/// Два вида событий:
/// 1. `host:http` + `request` — входящий HTTP-запрос от хоста. Мы публикуем
///    request агенту и сохраняем pending-состояние (не блокируемся).
/// 2. `agent:*` + `response` — ответ агента. Мы находим pending по request_id
///    и публикуем http-response хосту (который вернёт его клиенту через oneshot).
///
/// Не блокируемся внутри handle_event: wasmtime сериализует вызовы handle_event
/// на одном инстансе, поэтому ожидание здесь заблокировало бы обработку ответа.
pub async fn handle_event(ev: Event) {
    log_debug!("[WASM] front:http: получен ивент: {:?}", ev);

    // Ответ от агента: находим pending HTTP-запрос и отвечаем хосту.
    if ev.source.starts_with("agent:") && ev.topic == "response" {
        let pending = crate::get_pending_requests().remove(&ev.request_id);
        if let Some((_agent_req_id, pending_http)) = pending {
            let agent_result = ev.payload.clone();
            log_info!(
                "[WASM] front:http: ответ агента для HTTP req_id={}: {:?}",
                pending_http.http_request_id, agent_result
            );
            // Формируем http-response хосту. Возвращаем клиенту JSON с
            // ответом агента и session_id (чтобы клиент мог продолжить сессию).
            let http_response = serde_json::json!({
                "status": 200,
                "headers": [["x-session-id", pending_http.session_id]],
                "session_id": pending_http.session_id,
                "body": agent_result,
            });
            let resp_ev = Event {
                request_id: pending_http.http_request_id, // тот же, что ждёт HTTP-сервер
                session_id: pending_http.session_id,
                source: "front:http".to_string(),
                target: "host:http".to_string(),
                topic: "response".to_string(),
                payload: http_response.to_string(),
            };
            publish_event(&resp_ev);
        } else {
            log_warn!(
                "[WASM] front:http: ответ агента {} без pending (request_id={})",
                ev.source, ev.request_id
            );
        }
        return;
    }

    // Это HTTP-запрос от хоста (source="host:http", topic="request").
    if ev.source == "host:http" && ev.topic == "request" {
        let parsed: serde_json::Value = serde_json::from_str(&ev.payload).unwrap_or_default();
        let method = parsed.get("method").and_then(|m| m.as_str()).unwrap_or("GET").to_string();
        let path = parsed.get("path").and_then(|m| m.as_str()).unwrap_or("/").to_string();

        // Извлекаем промпт и session_id из тела запроса.
        let (prompt, body_session_id) = extract_request_fields(&parsed);
        // session_id: приоритет у явного поля в теле запроса (удобно для JSON-клиентов),
        // иначе используем тот, что хост извлёк из заголовка x-session-id (или сгенерировал).
        let effective_session_id = if body_session_id.is_empty() {
            ev.session_id.clone()
        } else {
            body_session_id
        };

        let agent_target = crate::get_agent_target().unwrap_or("agent:*".to_string());

        log_info!(
            "[WASM] front:http: {} {} -> агент {} (prompt={:?}, session_id={}, HTTP req_id={})",
            method, path, agent_target, prompt, effective_session_id, ev.request_id
        );

        // Генерируем request_id для агента.
        let agent_request_id = Uuid::new_v4().to_string();

        // Сохраняем pending: agent_request_id -> (http_request_id, session_id).
        crate::get_pending_requests().insert(
            agent_request_id.clone(),
            crate::PendingHttp {
                http_request_id: ev.request_id.clone(),
                session_id: effective_session_id.clone(),
            },
        );

        let agent_event = Event {
            request_id: agent_request_id.clone(),
            session_id: effective_session_id.clone(),
            source: "front:http".to_string(),
            target: agent_target,
            topic: "request".to_string(),
            payload: prompt.clone(),
        };
        publish_event(&agent_event);
        return;
    }

    // События прогресса (status) от агента front:http не обрабатывает —
    // они нужны только консольному фронтенду. Игнорируем тихо.
    if ev.topic == "status" {
        return;
    }

    log_warn!("[WASM] front:http: неизвестный ивент topic={} source={}", ev.topic, ev.source);
}

/// Извлечь промпт и session_id из JSON-тела HTTP-запроса.
///
/// Хост кладёт тело клиента в поле `body` как строку. Поэтому поля `q` /
/// `message` / `prompt` / `session_id` могут быть внутри этой строки (JSON),
/// а не на верхнем уровне. Ищем их и там, и там.
///
/// Промпт: если `body` — JSON-объект с `q`/`message`/`prompt` — берём его;
/// если `body` — произвольный текст (не JSON) — он и есть промпт;
/// если `body` — JSON без понятных полей — сериализуем обратно в строку.
/// session_id: поле `session_id` (если непустое), иначе пусто (вызывающий
/// подставит из заголовка/сгенерирует).
fn extract_request_fields(parsed: &serde_json::Value) -> (String, String) {
    // Тело клиента лежит в parsed["body"] как строка; пытаемся распарсить его как JSON.
    let body_val: Option<serde_json::Value> = parsed
        .get("body")
        .and_then(|b| b.as_str())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());

    let find_str = |key: &str| -> Option<String> {
        body_val
            .as_ref()
            .and_then(|v| v.get(key))
            .or_else(|| parsed.get(key))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };

    let prompt = if let Some(b) = body_val.as_ref() {
        find_str("q")
            .or_else(|| find_str("message"))
            .or_else(|| find_str("prompt"))
            .unwrap_or_else(|| b.to_string())
    } else {
        parsed.get("body").and_then(|b| b.as_str()).unwrap_or("").to_string()
    };

    let session_id = find_str("session_id").unwrap_or_default();
    (prompt, session_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Хост кладёт тело клиента в parsed["body"] как СТРОКУ (JSON-строку).
    fn make_parsed(body_inner: serde_json::Value) -> serde_json::Value {
        json!({
            "method": "POST",
            "path": "/query",
            "query": "",
            "headers": [],
            "body": body_inner.to_string(),
        })
    }

    #[test]
    fn extracts_session_id_from_body() {
        let p = make_parsed(json!({"q": "привет", "session_id": "sess-xyz"}));
        let (prompt, sid) = extract_request_fields(&p);
        assert_eq!(prompt, "привет");
        assert_eq!(sid, "sess-xyz");
    }

    #[test]
    fn prefers_q_field_inside_body() {
        let p = make_parsed(json!({"body": "сырой текст", "q": "игнор"}));
        // Явное поле q имеет приоритет над сырым body.
        let (prompt, _sid) = extract_request_fields(&p);
        assert_eq!(prompt, "игнор");
    }

    #[test]
    fn raw_text_body_is_prompt() {
        // body — не JSON, а просто текст.
        let p = json!({"method": "POST", "path": "/query", "query": "", "headers": [], "body": "привет как дела"});
        let (prompt, sid) = extract_request_fields(&p);
        assert_eq!(prompt, "привет как дела");
        assert!(sid.is_empty());
    }

    #[test]
    fn falls_back_to_message_and_prompt_fields() {
        assert_eq!(extract_request_fields(&make_parsed(json!({"message": "m"}))).0, "m");
        assert_eq!(extract_request_fields(&make_parsed(json!({"prompt": "p"}))).0, "p");
    }

    #[test]
    fn session_id_empty_when_absent() {
        let p = make_parsed(json!({"q": "hi"}));
        let (_prompt, sid) = extract_request_fields(&p);
        assert!(sid.is_empty());
    }
}
