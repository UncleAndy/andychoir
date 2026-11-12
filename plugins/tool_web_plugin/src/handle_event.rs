use crate::ai::host::event_bus::publish_event;
use crate::ai::host::http;
use crate::ai::host::types::Event;
use crate::{FetchArgs, PLUGIN_NAME, SearchArgs, WebConfig, CONFIG, tools};

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
                log_debug!("[WASM] web: definition published: {}", def.name);
            }
        }
        "request" => {
            log_info!("[WASM] web: request for '{}'", ev.target);
            let result = match ev.target.as_str() {
                "tool:web:search" => search(&ev).await,
                "tool:web:fetch" => fetch(&ev).await,
                other => Err(format!("web: unknown tool '{other}'")),
            };
            let payload = match result {
                Ok(text) => text,
                Err(e) => format!("(ошибка: {e})"),
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
            log_debug!("[WASM] web: ignoring topic '{}'", other);
        }
    }
}

/// Cached plugin config.
///
/// The `init()` call and event handling do not necessarily share instance
/// state (this is why the host exposes `host-control.get-plugin-config`), so
/// the config is fetched through the host API on first use and cached here.
async fn config() -> Result<WebConfig, String> {
    if let Some(cfg) = CONFIG.lock().unwrap().clone() {
        return Ok(cfg);
    }
    let raw = crate::ai::host::host_control::get_plugin_config().await;
    let cfg: WebConfig = serde_json::from_str(&raw)
        .map_err(|e| format!("web: cannot parse plugin config: {e}"))?;
    *CONFIG.lock().unwrap() = Some(cfg.clone());
    Ok(cfg)
}

/// `web:search` - POST to the configured Tavily-compatible endpoint.
async fn search(ev: &Event) -> Result<String, String> {
    let args: SearchArgs = serde_json::from_str(&ev.payload).map_err(|e| {
        format!("web:search: cannot parse arguments ({e}); expected {{\"query\": \"...\"}}")
    })?;
    let cfg = config().await?;
    let max = args.max_results.unwrap_or(cfg.max_results);

    let mut body = serde_json::json!({ "query": args.query, "max_results": max });
    if let Some(key) = &cfg.api_key {
        // Tavily accepts the key inside the JSON body; the host proxy exposes
        // no custom headers on purpose.
        body["api_key"] = serde_json::json!(key);
    }

    let (status, text) = http::post_json(cfg.endpoint.clone(), body.to_string()).await;
    if status != 200 {
        return Err(format!(
            "web:search: HTTP {status}: {}",
            tools::truncate(&text, 300)
        ));
    }
    Ok(tools::format_results(&text, max))
}

/// `web:fetch` - GET a page through the host proxy.
async fn fetch(ev: &Event) -> Result<String, String> {
    let args: FetchArgs = serde_json::from_str(&ev.payload).map_err(|e| {
        format!("web:fetch: cannot parse arguments ({e}); expected {{\"url\": \"...\"}}")
    })?;
    if !(args.url.starts_with("http://") || args.url.starts_with("https://")) {
        return Err("web:fetch: only http(s) URLs are allowed".to_string());
    }
    let cfg = config().await?;
    let (status, text) = http::get_text(args.url.clone()).await;
    if status != 200 {
        return Err(format!(
            "web:fetch: HTTP {status}: {}",
            tools::truncate(&text, 300)
        ));
    }
    Ok(tools::truncate(&text, cfg.max_bytes))
}
