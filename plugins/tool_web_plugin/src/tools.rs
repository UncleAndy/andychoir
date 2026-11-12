//! Tool definitions and response formatting.

use crate::ToolDefinition;

/// Static tool definitions, published on every discovery request.
pub fn definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "web:search".to_string(),
            description: "Searches the web through the configured search API and returns \
                          the top results as text (title, URL, snippet). Use it for current \
                          facts, news, or anything outside the model's own knowledge."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query."
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "How many results to return; defaults to the plugin config.",
                        "minimum": 1
                    }
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "web:fetch".to_string(),
            description: "Fetches one web page over HTTP GET and returns its text. Only hosts \
                          allowed by the host's network policy are reachable. Use it after \
                          web:search to read a promising result."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "Absolute http(s) URL to fetch."
                    }
                },
                "required": ["url"]
            }),
        },
    ]
}

/// Format a provider response (`results[]`) as compact text for the model.
pub fn format_results(body: &str, max_results: u32) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        // Not JSON: show the raw text so the model can still react.
        return truncate(body, 2000);
    };
    let results = value
        .get("results")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();
    if results.is_empty() {
        return "(no results)".to_string();
    }
    let mut out = String::new();
    for (i, item) in results.iter().take(max_results as usize).enumerate() {
        let title = item.get("title").and_then(|t| t.as_str()).unwrap_or("");
        let url = item.get("url").and_then(|u| u.as_str()).unwrap_or("");
        let content = item.get("content").and_then(|c| c.as_str()).unwrap_or("");
        out.push_str(&format!(
            "{}. {title}\n{url}\n{}\n\n",
            i + 1,
            truncate(content, 400)
        ));
    }
    out.trim_end().to_string()
}

/// Truncate to `max` bytes on a character boundary, marking the cut.
pub fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut cut = max;
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    let total = text.len();
    format!("{}\n...[truncated: {total} bytes]", &text[..cut])
}
