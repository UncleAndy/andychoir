use crate::ai::host::event_bus::publish_event;
use crate::ai::host::types::Event;
use crate::{McpClientConfig, PLUGIN_NAME, get_transports};

pub async fn init(config_json: String) -> Vec<String> {
    let config: McpClientConfig = match serde_json::from_str(&config_json) {
        Ok(c) => c,
        Err(e) => {
            log_error!("[WASM] mcp: не удалось распарсить конфиг: {}", e);
            return vec![PLUGIN_NAME.to_string()];
        }
    };
    log_info!("[WASM] mcp: инициализация, серверов: {}", config.servers.len());

    // Подписки: точное имя и маска (для входящих запросов к mcp:*).
    let topics = vec![PLUGIN_NAME.to_string(), "tool:mcp:*".to_string()];

    for server in &config.servers {
        // Открываем транспорт, initialize, tools/list.
        match crate::mcp::open(server).await {
            Ok(tid) => {
                get_transports().insert(server.name.clone(), tid.clone());
                match crate::mcp::initialize(&tid).await {
                    Ok(_) => {
                        log_info!("[WASM] mcp: сервер '{}' инициализирован", server.name);
                    }
                    Err(e) => {
                        log_error!("[WASM] mcp: initialize '{}': {}", server.name, e);
                        continue;
                    }
                }
                match crate::mcp::tools_list(&tid).await {
                    Ok(tools) => {
                        log_info!(
                            "[WASM] mcp: сервер '{}' предоставляет {} инструментов",
                            server.name,
                            tools.len()
                        );
                        for tool in tools {
                            let tool_name = tool
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string();
                            let description = tool
                                .get("description")
                                .and_then(|d| d.as_str())
                                .unwrap_or("")
                                .to_string();
                            let params = tool
                                .get("inputSchema")
                                .cloned()
                                .unwrap_or(serde_json::json!({ "type": "object" }));
                            // Имя инструмента: mcp:<server>:<tool>.
                            let full_name = format!("mcp:{}:{}", server.name, tool_name);
                            // Публикуем definition (перехват в bus регистрирует в LOCAL_TOOLS).
                            publish_event(&Event {
                                request_id: "-".to_string(),
                                session_id: "-".to_string(),
                                source: PLUGIN_NAME.to_string(),
                                target: "*".to_string(),
                                topic: "definition".to_string(),
                                payload: serde_json::json!({
                                    "name": full_name,
                                    "description": description,
                                    "parameters": params,
                                })
                                .to_string(),
                            });
                            log_info!("[WASM] mcp: зарегистрирован инструмент {}", full_name);
                        }
                    }
                    Err(e) => {
                        log_error!("[WASM] mcp: tools/list '{}': {}", server.name, e);
                    }
                }
            }
            Err(e) => {
                log_error!("[WASM] mcp: {}", e);
            }
        }
    }

    // Публикуем готовность.
    publish_event(&Event {
        request_id: "-".to_string(),
        session_id: "-".to_string(),
        source: PLUGIN_NAME.to_string(),
        target: "*".to_string(),
        topic: "status".to_string(),
        payload: "ready".to_string(),
    });

    topics
}
