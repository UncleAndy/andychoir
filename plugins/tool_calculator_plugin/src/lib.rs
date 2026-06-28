wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use std::sync::Mutex;
use exports::ai::host::plugin_lifecycle::Guest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

// Первое слово - класс плагина (например: "front", "agent", "tool" etc.)
// Второе слово - индивидуальное название плагина
const PLUGIN_NAME: &str = "tool:calculator";

static CONFIG: Mutex<Option<ToolDefinition>> = Mutex::new(None);

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CalculatorArgs {
    pub expression: String,
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

struct ToolCalculatorPluginImplementation;

mod init;
mod run;
mod handle_event;

impl Guest for ToolCalculatorPluginImplementation {
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

export!(ToolCalculatorPluginImplementation);
