wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use exports::ai::host::plugin_lifecycle::Guest;
use serde::Deserialize;

/// Tool description published to the host through topic "definition".
#[derive(Clone, Debug)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Plugin name: first word is the class, second is the instance name.
pub const PLUGIN_NAME: &str = "tool:web";

/// Plugin configuration (passed verbatim as JSON to `init`).
#[derive(Deserialize, Debug, Clone)]
pub struct WebConfig {
    /// Search API endpoint. Tavily-compatible: POST JSON, results in `results[]`.
    pub endpoint: String,
    /// Key sent inside the JSON body (Tavily style). Omit for local/keyless mocks.
    #[serde(default)]
    pub api_key: Option<String>,
    /// How many results to ask the provider for.
    #[serde(default = "default_max_results")]
    pub max_results: u32,
    /// Maximum bytes of page text returned by `web:fetch`.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
}

fn default_max_results() -> u32 {
    5
}

fn default_max_bytes() -> usize {
    100 * 1024
}

/// Arguments of `web:search`.
#[derive(Deserialize, Debug, Clone)]
pub struct SearchArgs {
    pub query: String,
    #[serde(default)]
    pub max_results: Option<u32>,
}

/// Arguments of `web:fetch`.
#[derive(Deserialize, Debug, Clone)]
pub struct FetchArgs {
    pub url: String,
}

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

pub static CONFIG: std::sync::Mutex<Option<WebConfig>> = std::sync::Mutex::new(None);

struct ToolWebPluginImplementation;

mod handle_event;
mod init;
mod run;
mod tools;

impl Guest for ToolWebPluginImplementation {
    async fn init(config_json: String) -> Vec<String> {
        init::init(config_json).await
    }

    async fn run() {
        run::run().await
    }

    async fn handle_event(ev: ai::host::types::Event) {
        handle_event::handle_event(ev).await
    }
}

export!(ToolWebPluginImplementation);
