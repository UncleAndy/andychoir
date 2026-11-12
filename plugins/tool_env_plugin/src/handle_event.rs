use crate::ai::host::event_bus::publish_event;
use crate::ai::host::types::Event;
use crate::ai::host::host_control;
use crate::{EnvGetArgs, PLUGIN_NAME, tools};

pub async fn handle_event(ev: Event) {
    match ev.topic.as_str() {
        // The host asks for tool definitions right after readiness.
        "discovery" => {
            for def in tools::definitions() {
                publish_event(&Event {
                    request_id: ev.request_id.clone(),
                    session_id: ev.session_id.clone(),
                    source: PLUGIN_NAME.to_string(),
                    target: ev.source.clone(),
                    topic: "definition".to_string(),
                    payload: serde_json::json!({
                        "name": def.name,
                        "description": def.description,
                        "parameters": def.parameters,
                    })
                    .to_string(),
                });
                log_debug!("[WASM] env: definition published: {}", def.name);
            }
        }
        "request" => {
            log_info!("[WASM] env: request for '{}'", ev.target);
            let result = match ev.target.as_str() {
                "tool:env:time" => Ok(tools::now_iso_utc()),
                "tool:env:get" => env_get(&ev).await,
                "tool:env:system" => match host_control::system_info().await {
                    Ok(json) => Ok(json),
                    Err(e) => Err(e),
                },
                other => Err(format!("env: unknown tool '{other}'")),
            };
            let payload = match result {
                Ok(text) => text,
                Err(e) => {
                    log_error!("[WASM] env: {}", e);
                    format!("(ошибка: {e})")
                }
            };
            publish_event(&Event {
                request_id: ev.request_id.clone(),
                session_id: ev.session_id.clone(),
                source: PLUGIN_NAME.to_string(),
                target: ev.source.clone(),
                topic: "response".to_string(),
                payload,
            });
        }
        other => {
            log_debug!("[WASM] env: ignoring topic '{}'", other);
        }
    }
}

/// `env:get` — read one host environment variable through host-control.
async fn env_get(ev: &Event) -> Result<String, String> {
    let args: EnvGetArgs = serde_json::from_str(&ev.payload).map_err(|e| {
        format!("env:get: cannot parse arguments ({e}); expected {{\"name\": \"VAR\"}}")
    })?;
    match host_control::env_get(args.name.clone()).await {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Ok(format!("(переменная '{}' не задана)", args.name)),
        Err(e) => Err(e),
    }
}
