use crate::{CONFIG, PLUGIN_NAME, ToolDefinition};

pub async fn init(_config_json: String) -> Vec<String> {
    let topics_to_subscribe = vec![PLUGIN_NAME.to_string()];

    let parsed_config = ToolDefinition {
        name: "random".to_string(),
        description: "Generates random data and returns it as TEXT (always safe to pass to an LLM). \
            Supported kinds (via `kind`): \
            \"int\" (params: min,max), \"uint\" (min,max), \"float\" (min,max), \
            \"bool\", \"uuid\", \
            \"bytes\" (params: size, encoding=hex|base64|base59), \
            \"string\" (params: length, charset), \
            \"choice\" (params: items[]), \"shuffle\" (params: items[] -> JSON array). \
            Always returns a plain string (never raw binary)."
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "description": "What to generate: int|uint|float|bool|uuid|bytes|string|choice|shuffle"
                },
                "params": {
                    "type": "object",
                    "description": "Parameters for the chosen kind (see tool description)"
                }
            },
            "required": ["kind"]
        }),
    };

    {
        let mut config_lock = CONFIG.lock().unwrap();
        *config_lock = Some(parsed_config);
    }

    log_debug!("[WASM] Плагин {} инициализирован", PLUGIN_NAME);

    // Сообщаем хосту о готовности.
    crate::ai::host::event_bus::publish_event(&crate::ai::host::types::Event {
        request_id: "-".to_string(),
        session_id: "-".to_string(),
        source: PLUGIN_NAME.to_string(),
        target: "*".to_string(),
        topic: "status".to_string(),
        payload: "ready".to_string(),
    });

    topics_to_subscribe
}
