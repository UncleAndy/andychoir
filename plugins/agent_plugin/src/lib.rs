wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::sync::{Mutex, OnceLock, RwLock};
use dashmap::DashMap;

#[derive(Deserialize, Clone)]
pub struct AgentPluginConfig {
    pub name: String, // Имя агента (например, его роль)
    pub model: ModelConfig, // Только имя модели (определяется в конфиге плагина модели)
    pub system_prompt: String,
    pub tools: Vec<String>, // Только имена инструментов
}

#[derive(Deserialize, Clone)]
pub struct ModelConfig {
    pub model_name: String,
    pub api_url: String,
    pub api_key: Option<String>,
}

const PLUGIN_CLASS: &str = "agent";

static CONFIG: Mutex<Option<AgentPluginConfig>> = Mutex::new(None);

#[allow(unused)]
static CLIENT: RwLock<Option<openai_api_rs::v1::api::OpenAIClient>> = RwLock::new(None);

static STATUS: RwLock<PluginInitStatus> = RwLock::new(PluginInitStatus::NotInitialized);

static TOOLS: OnceLock<DashMap<String, ToolDefinition>> = OnceLock::new();
pub fn get_tools() -> &'static DashMap<String, ToolDefinition> {
    TOOLS.get_or_init(DashMap::new)
}

/// Результаты вызовов инструментов по request_id запроса-вызова.
/// Заполняется в handle_event (когда приходит response от tool), читается
/// циклом tool-calling после wait_for_response.
static TOOL_RESULTS: OnceLock<DashMap<String, String>> = OnceLock::new();
pub fn get_tool_results() -> &'static DashMap<String, String> {
    TOOL_RESULTS.get_or_init(DashMap::new)
}

#[derive(PartialEq, Debug)]
pub enum PluginInitStatus {
    NotInitialized,
    NotInitializedYet,
    Initialized,
}

#[derive(Deserialize, Clone)]
pub struct ToolDefinition {
    name: String,
    #[allow(unused)]
    description: String,
    #[allow(unused)]
    parameters: serde_json::Value,
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

struct PluginImplementation;

mod init;
mod run;
mod handle_event;

impl Guest for PluginImplementation {
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

export!(PluginImplementation);
