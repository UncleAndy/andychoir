wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use exports::ai::host::plugin_lifecycle::Guest;
use serde::Deserialize;

/// Конфигурация MCP-плагина: список серверов.
#[derive(Deserialize, Clone)]
pub struct McpClientConfig {
    pub name: String,
    pub servers: Vec<McpServerConfig>,
}

/// Один MCP-сервер.
#[derive(Deserialize, Clone)]
pub struct McpServerConfig {
    /// Имя сервера (используется в имени инструментов `mcp:<server>:<tool>`).
    pub name: String,
    /// Тип транспорта: "stdio" (сейчас) или "http" (потом).
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Команда для stdio-транспорта.
    pub command: String,
    /// Аргументы для stdio-транспорта.
    #[serde(default)]
    pub args: Vec<String>,
}

fn default_transport() -> String {
    "stdio".to_string()
}

const PLUGIN_NAME: &str = "tool:mcp";

#[allow(unused_macros)]
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        crate::ai::host::log::debug(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        crate::ai::host::log::error(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        crate::ai::host::log::warn(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        crate::ai::host::log::info(&format!($($arg)*))
    };
}

struct McpClientPluginImplementation;

// Транспорты по имени сервера: server_name -> transport_id.
static TRANSPORTS: std::sync::OnceLock<dashmap::DashMap<String, String>> = std::sync::OnceLock::new();
pub fn get_transports() -> &'static dashmap::DashMap<String, String> {
    TRANSPORTS.get_or_init(dashmap::DashMap::new)
}

mod init;
mod handle_event;
mod mcp;

impl Guest for McpClientPluginImplementation {
    async fn init(config_json: String) -> Vec<String> {
        init::init(config_json).await
    }

    async fn run() {
        // Ввод приходит через handle_event; фоновый цикл не нужен.
    }

    async fn handle_event(ev: ai::host::types::Event) {
        handle_event::handle_event(ev).await
    }
}

export!(McpClientPluginImplementation);
