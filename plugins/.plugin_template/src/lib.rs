wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::sync::Mutex;

#[derive(Deserialize)]
struct InnerPluginConfig {
    option_1: String
}

// Первое слово - класс плагина (например: "front", "agent", "tool" etc.)
// Второе слово - индивидуальное название плагина
const PLUGIN_NAME: &str = "example:name";

static CONFIG: Mutex<Option<InnerPluginConfig>> = Mutex::new(None);

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        crate::ai::host::console::print_line(&format!($($arg)*))
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
