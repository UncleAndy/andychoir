use crate::PLUGIN_NAME;

pub async fn init(_config_json: String) -> Vec<String> {
    // Requests arrive as "tool:env:time", "tool:env:get", "tool:env:system":
    // nested addressing matches the plugin name "tool:env" plus a sub-address,
    // so subscribing to the name and to the mask covers both forms.
    let topics_to_subscribe = vec![PLUGIN_NAME.to_string(), "tool:env:*".to_string()];

    log_debug!(
        "[WASM] Plugin {} initialized. Subscription requests: {}",
        PLUGIN_NAME,
        topics_to_subscribe.len()
    );

    // Report readiness; the host answers with a discovery request, and the
    // plugin publishes its three tool definitions in response.
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
