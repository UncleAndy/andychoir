use crate::ai::host::event_bus::publish_event;
use crate::ai::host::types::Event;
use crate::PLUGIN_NAME;

pub async fn handle_event(ev: Event) {
    if ev.topic != "request" {
        return;
    }

    log_debug!("[WASM] random-plugin: получен запрос {}", ev.request_id);

    // kind и params из payload ( payload — JSON {"kind": "...", "params": {...}} ).
    let parsed: serde_json::Value = serde_json::from_str(&ev.payload).unwrap_or(serde_json::json!({}));
    let kind = parsed["kind"].as_str().unwrap_or("").to_string();
    let params = parsed
        .get("params")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "{}".to_string());

    let payload = match crate::ai::host::host_control::random(kind, params).await {
        Ok(value) => value,
        Err(e) => {
            log_error!("[WASM] random-plugin: ошибка: {}", e);
            format!("error: {}", e)
        }
    };

    publish_event(&Event {
        request_id: ev.request_id,
        session_id: ev.session_id,
        source: PLUGIN_NAME.to_string(),
        target: ev.source,
        topic: "response".to_string(),
        payload,
    });
}
