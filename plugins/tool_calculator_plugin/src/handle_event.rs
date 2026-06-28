use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::{CalculatorArgs, CONFIG, PLUGIN_NAME};

pub async fn handle_event(ev: Event) {
    log_debug!("[WASM] Получен ивент от хоста: {:?}", ev);
    log_debug!("{}: {}", ev.topic, ev.payload);

    match ev.topic.as_str() {
        "discovery" => {
            // Отправляем в ответ свой конфиг
            publish_event(&Event {
                request_id: ev.request_id,
                session_id: ev.session_id,
                source: PLUGIN_NAME.to_string(),
                target: ev.source,
                topic: "definition".to_string(),
                payload: serde_json::to_string(&CONFIG).unwrap(),
            });
        }
        // Вычисляем выражение в payload и ответным сообщением отправляем ответ
        "request" => {
            // Сначала надо распарсить запрос в формате JSON
            let request_res: Result<CalculatorArgs, serde_json::Error> = serde_json::from_str(&ev.payload);

            if request_res.is_err() {
                log_error!("[WASM] Failed to parse request: {}", request_res.err().unwrap());
                return;
            }

            let request = request_res.unwrap();

            let res = meval::eval_str(request.expression)
                .map(|result| result.to_string());

            match res {
                Ok(ans) => {
                    log_info!("[WASM] Calculator tool response: {}", ans);
                    publish_event(&Event {
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
        _ => {
            log_error!("[WASM] Calculator tool unknown topic: {}", ev.topic);
        }
    }
}
