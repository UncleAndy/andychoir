wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::sync::Mutex;

/// Слушатель, объявляемый в конфиге плагина.
#[derive(Deserialize, Clone)]
struct ListenerConfig {
    port: u16,
    path: String,
    /// Целевой плагин, которому будет уходить HTTP-запрос (напр. "agent:*").
    #[serde(default = "default_target")]
    target: String,
    /// Адрес привязки (B4). Если не задан — хост использует "0.0.0.0".
    #[serde(default)]
    bind: Option<String>,
    /// Токен аутентификации (B4). Если задан — клиент обязан слать
    /// `Authorization: Bearer <token>`; иначе 401.
    #[serde(default)]
    auth_token: Option<String>,
}

fn default_target() -> String {
    "agent:*".to_string()
}

#[derive(Deserialize, Clone)]
struct FrontHttpPluginConfig {
    listeners: Vec<ListenerConfig>,
}

const PLUGIN_NAME: &str = "front:http";

static CONFIG: Mutex<Option<FrontHttpPluginConfig>> = Mutex::new(None);

/// Целевой агент (первый слушатель в конфиге) либо "agent:*".
pub fn get_agent_target() -> Option<String> {
    let cfg = CONFIG.lock().unwrap();
    cfg.as_ref()
        .and_then(|c| c.listeners.first())
        .map(|l| l.target.clone())
}

/// Ответы агента по request_id (заполняется в handle_event при response от agent).
/// ПАРАМЕТР: заменён на pending-механизм (неблокирующий).
#[derive(Clone)]
pub struct PendingHttp {
    pub http_request_id: String,
    pub session_id: String,
}

/// Пара agent_request_id -> ожидающий HTTP-запрос.
/// Заполняется при HTTP-запросе, очищается при ответе агента.
static PENDING_REQUESTS: std::sync::OnceLock<dashmap::DashMap<String, PendingHttp>> =
    std::sync::OnceLock::new();

pub fn get_pending_requests() -> &'static dashmap::DashMap<String, PendingHttp> {
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

struct FrontHttpPluginImplementation;

mod run;
mod handle_event;

impl Guest for FrontHttpPluginImplementation {
    async fn init(config_json: String) -> Vec<String> {
        let parsed: FrontHttpPluginConfig = serde_json::from_str(&config_json)
            .unwrap_or_else(|_| FrontHttpPluginConfig { listeners: vec![] });

        {
            let mut cfg = CONFIG.lock().unwrap();
            *cfg = Some(parsed.clone());
        }

        // Регистрируем слушатели у хоста.
        for l in &parsed.listeners {
            let ok = crate::ai::host::http_server::listen_http(
                crate::ai::host::http_server::Listener {
                    port: l.port,
                    path: l.path.clone(),
                    target: l.target.clone(),
                    bind: l.bind.clone(),
                    auth_token: l.auth_token.clone(),
                },
            )
            .await;
            log_info!(
                "[WASM] {}: регистрация слушателя {}:{} target={} -> {}",
                PLUGIN_NAME,
                l.port,
                l.path,
                l.target,
                ok
            );
        }

        // Подписываемся на события с нашим target (front:http:<port>:<path>).
        // Хост шлёт HTTP-запросы с target="<имя>:<port>:<path>", но плагин
        // один на все порты — подпишемся на "front:http:*".
        let subs = vec![PLUGIN_NAME.to_string(), "front:http:*".to_string()];

        // Сообщаем хосту о готовности (хост агрегирует и публикует host:ready).
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

export!(FrontHttpPluginImplementation);
