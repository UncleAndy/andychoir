use crate::{CONFIG, PLUGIN_NAME, ToolDefinition};

pub async fn init(_config_json: String) -> Vec<String> {
    let topics_to_subscribe = vec![
        PLUGIN_NAME.to_string(),
    ];

    let parsed_config = ToolDefinition{
        name: "calculator".to_string(),
        description: "Evaluates a single mathematical expression and returns the exact result. Use this tool only for numeric calculations that can be represented as one expression. Supported syntax includes arithmetic operators, parentheses, decimal numbers, unary minus, and the functions listed in the parameter description. Do not guess or approximate the answer.".to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "A single mathematical expression to evaluate. Supported syntax: arithmetic operators +, -, *, /, %, ^; parentheses ( and ); decimal numbers; unary minus; and the functions supported by the calculator implementation: sqrt, abs, exp, ln, sin, cos, tan, asin, acos, atan, atan2, sinh, cosh, tanh, asinh, acosh, atanh, floor, ceil, round, signum; constants: pi, e. Examples: 2 + 2 * (5 - 1), -(3.5 + 4) / 2. Do not include natural language, units, or multiple unrelated statements."
                }
            },
        }),
    };

    {
        let mut config_lock = CONFIG.lock().unwrap();
        *config_lock = Some(parsed_config);
    }


    log_debug!(
        "[WASM] Плагин {} инициализирован. Запрошено подписок: {}",
        PLUGIN_NAME,
        topics_to_subscribe.len()
    );

    // Сообщаем хосту о готовности (хост агрегирует и публикует host:"ready").
    crate::ai::host::event_bus::publish_event(&crate::ai::host::types::Event {
        request_id: "-".to_string(),
        session_id: "-".to_string(),
        source: PLUGIN_NAME.to_string(),
        target: "*".to_string(),
        topic: "status".to_string(),
        payload: "ready".to_string(),
    });

    // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
    topics_to_subscribe
}
