//! Логика MCP-клиента (протокол JSON-RPC поверх транспорта хоста).
//! Плагин реализует протокол MCP, хост — только транспорт (stdio/HTTP).

use crate::McpServerConfig;

/// Собрать JSON-RPC запрос-строку.
fn jsonrpc_request(id: u64, method: &str, params: serde_json::Value) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
    .to_string()
}

/// Открыть stdio-подпроцесс сервера через хост.
pub async fn open(server: &McpServerConfig) -> Result<String, String> {
    let tid = crate::ai::host::mcp_transport::stdio_open(
        server.command.clone(),
        server.args.clone(),
    )
    .await;
    if tid == "-" {
        Err(format!("не удалось открыть MCP-сервер '{}' (команда: {})", server.name, server.command))
    } else {
        Ok(tid)
    }
}

/// Выполнить initialize. Возвращает результат (или Err).
pub async fn initialize(tid: &str) -> Result<serde_json::Value, String> {
    let params = serde_json::json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": { "name": "andychoir-mcp", "version": "0.1.0" },
    });
    let req = jsonrpc_request(1, "initialize", params);
    let resp = crate::ai::host::mcp_transport::request(tid.to_string(), req, 30000)
        .await
        .ok_or_else(|| "initialize: таймаут/нет ответа".to_string())?;
    serde_json::from_str(&resp).map_err(|e| format!("initialize: невалидный JSON-RPC: {}", e))
}

/// Список инструментов сервера (tools/list). Возвращает массив tools.
pub async fn tools_list(tid: &str) -> Result<Vec<serde_json::Value>, String> {
    let req = jsonrpc_request(2, "tools/list", serde_json::json!({}));
    let resp = crate::ai::host::mcp_transport::request(tid.to_string(), req, 30000)
        .await
        .ok_or_else(|| "tools/list: таймаут/нет ответа".to_string())?;
    let v: serde_json::Value =
        serde_json::from_str(&resp).map_err(|e| format!("tools/list: невалидный JSON-RPC: {}", e))?;
    let tools = v
        .get("result")
        .and_then(|r| r.get("tools"))
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(tools)
}

/// Вызвать инструмент (tools/call). Возвращает результат (строку).
pub async fn tools_call(tid: &str, name: &str, arguments: serde_json::Value) -> Result<String, String> {
    let params = serde_json::json!({ "name": name, "arguments": arguments });
    let req = jsonrpc_request(3, "tools/call", params);
    let resp = crate::ai::host::mcp_transport::request(tid.to_string(), req, 15000)
        .await
        .ok_or_else(|| format!("tools/call '{}': таймаут/нет ответа", name))?;
    let v: serde_json::Value =
        serde_json::from_str(&resp).map_err(|e| format!("tools/call: невалидный JSON-RPC: {}", e))?;
    // Результат: v["result"]["content"] = [{type, text}] или v["result"]["structuredContent"].
    if let Some(result) = v.get("result") {
        if let Some(sc) = result.get("structuredContent") {
            // Часто structuredContent = {"result": "<текст>"} — извлекаем текст.
            if let Some(s) = sc.get("result") {
                return Ok(s.to_string());
            }
            return Ok(sc.to_string());
        }
        if let Some(content) = result.get("content").and_then(|c| c.as_array()) {
            let texts: Vec<String> = content
                .iter()
                .filter_map(|c| c.get("text").and_then(|t| t.as_str()).map(|s| s.to_string()))
                .collect();
            if !texts.is_empty() {
                return Ok(texts.join("\n"));
            }
        }
        return Ok(result.to_string());
    }
    if let Some(err) = v.get("error") {
        return Err(format!("tools/call '{}': MCP error: {}", name, err));
    }
    Err(format!("tools/call '{}': нет result/error в ответе", name))
}
