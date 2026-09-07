wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::sync::Mutex;

/// Слушатель, объявляемый в конфиге плагина.
#[derive(Deserialize, Clone)]
struct WsListenerConfig {
    port: u16,
    path: String,
    /// Целевой плагин, которому будет уходить WS-сообщение (напр. "agent:*").
    #[serde(default = "default_target")]
    target: String,
    /// Адрес привязки (B4). Если не задан — хост использует "0.0.0.0".
    #[serde(default)]
    bind: Option<String>,
    /// Токен аутентификации (B4). Если задан — клиент обязан присылать
    /// поле `auth` в JSON; иначе соединение закрывается.
    #[serde(default)]
    auth_token: Option<String>,
}

fn default_target() -> String {
    "agent:*".to_string()
}

#[derive(Deserialize, Clone)]
struct FrontWsPluginConfig {
    listeners: Vec<WsListenerConfig>,
}

const PLUGIN_NAME: &str = "front:ws";

static CONFIG: Mutex<Option<FrontWsPluginConfig>> = Mutex::new(None);

/// Целевой агент (первый слушатель в конфиге) либо "agent:*".
pub fn get_agent_target() -> Option<String> {
    let cfg = CONFIG.lock().unwrap();
    cfg.as_ref()
        .and_then(|c| c.listeners.first())
        .map(|l| l.target.clone())
}

/// Ожидающий WS-запрос: ws_request_id (который ждёт хост-сокет) + session_id.
#[derive(Clone)]
pub struct PendingWs {
    pub ws_request_id: String,
    pub session_id: String,
}

/// Пара agent_request_id -> ожидающий WS-запрос.
static PENDING_REQUESTS: std::sync::OnceLock<dashmap::DashMap<String, PendingWs>> =
    std::sync::OnceLock::new();

pub fn get_pending_requests() -> &'static dashmap::DashMap<String, PendingWs> {
    PENDING_REQUESTS.get_or_init(dashmap::DashMap::new)
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

struct FrontWsPluginImplementation;

mod run;
mod handle_event;

impl Guest for FrontWsPluginImplementation {
    async fn init(config_json: String) -> Vec<String> {
        let parsed: FrontWsPluginConfig = serde_json::from_str(&config_json)
            .unwrap_or_else(|_| FrontWsPluginConfig { listeners: vec![] });

        {
            let mut cfg = CONFIG.lock().unwrap();
            *cfg = Some(parsed.clone());
        }

        // Регистрируем WS-слушатели у хоста.
        for l in &parsed.listeners {
            let ok = crate::ai::host::ws_server::listen_ws(
                crate::ai::host::ws_server::WsListener {
                    port: l.port,
                    path: l.path.clone(),
                    target: l.target.clone(),
                    bind: l.bind.clone(),
                    auth_token: l.auth_token.clone(),
                },
            )
            .await;
            log_info!(
                "[WASM] {}: регистрация WS-слушателя {}:{} target={} -> {}",
                PLUGIN_NAME,
                l.port,
                l.path,
                l.target,
                ok
            );
        }

        // Подписываемся на события ws-фронта (хост шлёт с target="front:ws:*").
        let subs = vec![PLUGIN_NAME.to_string(), "front:ws:*".to_string()];

        // Сообщаем хосту о готовности.
        crate::ai::host::event_bus::publish_event(&Event {
            request_id: "-".to_string(),
            session_id: "-".to_string(),
            source: PLUGIN_NAME.to_string(),
            target: "*".to_string(),
            topic: "status".to_string(),
            payload: "ready".to_string(),
        });

        subs
    }

    async fn run() {
        run::run().await
    }

    async fn handle_event(ev: ai::host::types::Event) {
        handle_event::handle_event(ev).await
    }
}

export!(FrontWsPluginImplementation);
