use crate::{TOOLS, PLUGIN_NAME, ToolDefinition};

fn tool(name: &str, description: &str, params: serde_json::Value) -> ToolDefinition {
    ToolDefinition {
        name: name.to_string(),
        description: description.to_string(),
        parameters: params,
    }
}

pub async fn init(_config_json: String) -> Vec<String> {
    let topics_to_subscribe = vec![PLUGIN_NAME.to_string()];

    let path_param = serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": "string", "description": "Absolute path under the plugin's allowed filesystem root." }
        },
        "required": ["path"]
    });
    let path_contents_param = serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": "string", "description": "Absolute path under the plugin's allowed filesystem root." },
            "contents": { "type": "string", "description": "File contents." }
        },
        "required": ["path", "contents"]
    });
    let two_path_param = serde_json::json!({
        "type": "object",
        "properties": {
            "src": { "type": "string", "description": "Source path (under allowed root)." },
            "dst": { "type": "string", "description": "Destination path (under allowed root)." }
        },
        "required": ["src", "dst"]
    });
    let patch_param = serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": "string", "description": "File to patch (under allowed root)." },
            "old": { "type": "string", "description": "Exact substring to replace (must exist).", "format": "multi-line" },
            "new": { "type": "string", "description": "Replacement text.", "format": "multi-line" }
        },
        "required": ["path", "old", "new"]
    });

    let tools = vec![
        tool("fs-read", "Read a file under the allowed root. Returns file contents.", path_param.clone()),
        tool("fs-write", "Write/overwrite a file under the allowed root (requires rw).", path_contents_param.clone()),
        tool("fs-append", "Append to a file under the allowed root (requires rw).", path_contents_param.clone()),
        tool("fs-remove", "Delete a file under the allowed root (requires rw).", path_param.clone()),
        tool("fs-mkdir", "Create a directory recursively under the allowed root (requires rw).", path_param.clone()),
        tool("fs-rmdir", "Remove a directory recursively under the allowed root (requires rw).", path_param.clone()),
        tool("fs-move", "Move/rename a path under the allowed root (requires rw).", two_path_param.clone()),
        tool("fs-copy", "Copy a file under the allowed root (requires rw at destination).", two_path_param.clone()),
        tool("fs-list", "List directory contents under the allowed root (JSON array).", path_param.clone()),
        tool("fs-stat", "Get file/dir metadata under the allowed root (JSON).", path_param.clone()),
        tool("fs-patch", "Replace exact substring in a file (requires rw, fail-closed if 'old' not found).", patch_param.clone()),
    ];

    {
        let mut lock = TOOLS.lock().unwrap();
        *lock = Some(tools);
    }

    log_debug!("[WASM] Плагин {} инициализирован ({} инструментов).", PLUGIN_NAME, 11);

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
