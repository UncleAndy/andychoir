use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use uuid::Uuid;

/// Обработать событие от хоста.
///
/// Два вида событий:
/// 1. `host:ws` + `request` — входящее WS-сообщение от хоста. Публикуем request
///    агенту и сохраняем pending (не блокируемся).
/// 2. `agent:*` + `response` — ответ агента. Находим pending по request_id и
///    публикуем ws-response хосту (который отправит его на сокет клиента).
///
/// Не блокируемся внутри handle_event (wasmtime сериализует вызовы).
pub async fn handle_event(ev: Event) {
    log_debug!("[WASM] front:ws: получен ивент: {:?}", ev);

    // Ответ от агента: находим pending и отвечаем хосту.
    if ev.source.starts_with("agent:") && ev.topic == "response" {
        let pending = crate::get_pending_requests().remove(&ev.request_id);
        if let Some((_agent_req_id, pending_ws)) = pending {
            let agent_result = ev.payload.clone();
            log_info!(
                "[WASM] front:ws: ответ агента для WS req_id={}: {:?}",
                pending_ws.ws_request_id, agent_result
            );
            let resp_ev = Event {
                request_id: pending_ws.ws_request_id, // тот же, что ждёт хост-сокет
                session_id: pending_ws.session_id,
                source: "front:ws".to_string(),
                target: "host:ws".to_string(),
                topic: "response".to_string(),
                payload: agent_result,
            };
            publish_event(&resp_ev);
        } else {
            log_warn!(
                "[WASM] front:ws: ответ агента {} без pending (request_id={})",
                ev.source, ev.request_id
            );
        }
        return;
    }

    // Это WS-сообщение от хоста (source="host:ws", topic="request").
    if ev.source == "host:ws" && ev.topic == "request" {
        let message = ev.payload.clone();
        let agent_target = crate::get_agent_target().unwrap_or("agent:*".to_string());

        log_info!(
            "[WASM] front:ws: -> агент {} (message={:?}, session_id={}, WS req_id={})",
            agent_target, message, ev.session_id, ev.request_id
        );

        let agent_request_id = Uuid::new_v4().to_string();

        // Сохраняем pending: agent_request_id -> (ws_request_id, session_id).
        crate::get_pending_requests().insert(
            agent_request_id.clone(),
            crate::PendingWs {
                ws_request_id: ev.request_id.clone(),
                session_id: ev.session_id.clone(),
            },
        );

        let agent_event = Event {
            request_id: agent_request_id.clone(),
            session_id: ev.session_id.clone(),
            source: "front:ws".to_string(),
            target: agent_target,
            topic: "request".to_string(),
            payload: message,
        };
        publish_event(&agent_event);
        return;
    }

    // События прогресса (status) от агента front:ws не обрабатывает.
    if ev.topic == "status" {
        return;
    }

    log_warn!("[WASM] front:ws: неизвестный ивент topic={} source={}", ev.topic, ev.source);
}
