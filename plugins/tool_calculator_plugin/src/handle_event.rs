use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::{CalculatorArgs, CONFIG, PLUGIN_NAME, ToolDefinition};

pub async fn handle_event(ev: Event) {
    log_debug!("[WASM] Получен ивент от хоста: {:?}", ev);
    log_debug!("{}: {}", ev.topic, ev.payload);

    match ev.topic.as_str() {
        "discovery" => {
            // Отправляем в ответ свой конфиг.
            // ВАЖНО: CONFIG — это Mutex<Option<ToolDefinition>; сериализуем
            // содержимое, а не сам Mutex (иначе будет "null").
            let cfg_guard = CONFIG.lock().unwrap();
            let tool_def: ToolDefinition = cfg_guard
                .clone()
                .unwrap_or_else(|| ToolDefinition {
                    name: "calculator".to_string(),
                    description: String::new(),
                    parameters: serde_json::Value::Null,
                });
            drop(cfg_guard);
            publish_event(&Event {
                request_id: ev.request_id,
                session_id: ev.session_id,
                source: PLUGIN_NAME.to_string(),
                target: ev.source,
                topic: "definition".to_string(),
                payload: serde_json::to_string(&tool_def).unwrap(),
            });
        }
        // Вычисляем выражение в payload и ответным сообщением отправляем ответ
        "request" => {
            // Сначала надо распарсить запрос в формате JSON
            let request_res: Result<CalculatorArgs, serde_json::Error> = serde_json::from_str(&ev.payload);

            let request = match request_res {
                Ok(req) => req,
                Err(e) => {
                    // Не зависаем у агента: при ошибке парсинга отвечаем сообщением.
                    log_error!("[WASM] Failed to parse request: {}", e);
                    publish_event(&Event {
                        request_id: ev.request_id,
                        session_id: ev.session_id,
                        source: PLUGIN_NAME.to_string(),
                        target: ev.source,
                        topic: "response".to_string(),
                        payload: format!("(ошибка: не удалось распарсить запрос: {})", e),
                    });
                    return;
                }
            };

            let res = meval::eval_str(request.expression);

            match res {
                Ok(ans) => {
                    log_info!("[WASM] Calculator tool response: {}", ans);
                    publish_event(&Event {
                        request_id: ev.request_id,
                        session_id: ev.session_id,
                        source: PLUGIN_NAME.to_string(),
                        target: ev.source,
                        topic: "response".to_string(),
                        payload: ans.to_string(),
                    });
                }
                Err(e) => {
                    // При ошибке вычисления тоже отвечаем (иначе агент зависнет).
                    log_error!("[WASM] Calculator tool error: {}", e);
                    publish_event(&Event {
                        request_id: ev.request_id,
                        session_id: ev.session_id,
                        source: PLUGIN_NAME.to_string(),
                        target: ev.source,
                        topic: "response".to_string(),
                        payload: format!("(ошибка вычисления: {})", e),
                    });
                }
            }
        }
        _ => {
            log_error!("[WASM] Calculator tool unknown topic: {}", ev.topic);
        }
    }
}
