use crate::ai::host::event_bus::publish_event;
use crate::ai::host::types::Event;
use crate::{PLUGIN_NAME, get_transports};

/// Обработка входящих событий.
///
/// Вызов инструмента MCP: агент шлёт request к `mcp:<server>:<tool>`
/// (source=agent, topic=request). Мы вызываем tools/call на транспорте сервера
/// и возвращаем response.
pub async fn handle_event(ev: Event) {
    // Вызов MCP-инструмента: target = "tool:mcp:<server>:<tool>", topic="request".
    if ev.topic == "request" && ev.target.starts_with("tool:mcp:") {
        let target = ev.target.clone();
        // Убираем префикс "tool:mcp:" -> "<server>:<tool>".
        let rest = &target["tool:mcp:".len()..];
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() < 2 {
            publish_response(&ev, "(ошибка: неверный формат target для MCP-инструмента)");
            return;
        }
        let server_name = parts[0];
        let tool_name = parts[1];

        // Находим транспорт сервера.
        let tid = {
            let transports = get_transports();
            match transports.get(server_name) {
                Some(t) => t.clone(),
                None => {
                    publish_response(&ev, &format!("(ошибка: MCP-сервер '{}' не подключён)", server_name));
                    return;
                }
            }
        };

        // Парсим аргументы: payload может быть JSON-объектом (arguments).
        let arguments = serde_json::from_str::<serde_json::Value>(&ev.payload)
            .unwrap_or(serde_json::json!({}));

        match crate::mcp::tools_call(&tid, tool_name, arguments).await {
            Ok(result) => {
                log_info!("[WASM] mcp: tools_call '{}' вернул: {}", ev.target, result);
                publish_response(&ev, &result);
            }
            Err(e) => {
                log_error!("[WASM] mcp: tools_call '{}' ошибка: {}", ev.target, e);
                publish_response(&ev, &format!("(ошибка MCP: {})", e));
            }
        }
    }
}

/// Сформировать и опубликовать response на запрос.
fn publish_response(orig: &Event, content: &str) {
    publish_event(&Event {
        request_id: orig.request_id.clone(),
        session_id: orig.session_id.clone(),
        source: PLUGIN_NAME.to_string(),
        target: orig.source.clone(),
        topic: "response".to_string(),
        payload: content.to_string(),
    });
}
