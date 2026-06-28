use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::{PLUGIN_NAME};

pub async fn handle_event(ev: Event) {
    log_debug!("[WASM] Получен ивент от хоста: {:?}", ev);
    log_debug!("{}: {}", ev.topic, ev.payload);

    // Вычисляем выражение в payload и ответным сообщением отправляем ответ
    if ev.topic == "request" {
        let res = meval::eval_str(ev.payload)
            .map(|result| result.to_string());

        match res {
            Ok(ans) => {
                log_info!("[WASM] Calculator tool response: {}", ans);
                publish_event(&Event{
                    request_id: ev.request_id,
                    session_id: ev.session_id,
                    source: PLUGIN_NAME.to_string(),
                    target: ev.source,
                    topic: "response".to_string(),
                    payload: ans,
                });
            }
            Err(e) => {
                log_error!("[WASM] Calculator tool error: {}", e)
            }
        }
    }
}
