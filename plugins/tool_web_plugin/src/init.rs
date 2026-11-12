use crate::{PLUGIN_NAME, WebConfig, CONFIG};

pub async fn init(config_json: String) -> Vec<String> {
    match serde_json::from_str::<WebConfig>(&config_json) {
        Ok(cfg) => {
            crate::log_info!(
                "[WASM] web: endpoint '{}', max_results {}, max_bytes {}",
                cfg.endpoint,
                cfg.max_results,
                cfg.max_bytes
            );
            *CONFIG.lock().unwrap() = Some(cfg);
        }
        Err(e) => {
            // Fail loudly but stay loaded: tools will answer with a clear error
            // instead of hanging the agent.
            crate::log_error!("[WASM] web: cannot parse config: {e}");
            *CONFIG.lock().unwrap() = None;
        }
    }

    // Requests arrive as "tool:web:search" / "tool:web:fetch": nested
    // addressing matches the plugin name "tool:web" plus a sub-address.
    let topics_to_subscribe = vec![PLUGIN_NAME.to_string(), "tool:web:*".to_string()];

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
