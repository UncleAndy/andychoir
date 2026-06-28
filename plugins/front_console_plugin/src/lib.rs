wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize)]
struct FrontConsolePluginConfig {
    // Плагин может принимать из конфига топики, которые ему нужно слушать
    subscriptions: Vec<String>,
    // Кому плагин будет отправлять сообщения
    #[allow(dead_code)]
    target: Vec<String>,
}

const PLUGIN_NAME: &str = "front:console";

static CONFIG: Mutex<Option<FrontConsolePluginConfig>> = Mutex::new(None);
static SESSIONS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

const PROMPT: &str = "prompt>";

macro_rules! println {
    ($($arg:tt)*) => {
        crate::ai::host::console::print_line(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        crate::ai::host::log::debug(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]macro_rules! error {
    ($($arg:tt)*) => {
        crate::ai::host::log::error(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]macro_rules! warn {
    ($($arg:tt)*) => {
        crate::ai::host::log::warn(&format!($($arg)*))
    };
}
#[allow(unused_macros)]
#[macro_export]macro_rules! info {
    ($($arg:tt)*) => {
        crate::ai::host::log::info(&format!($($arg)*))
    };
}

struct FrontConsolePluginImplementation;

impl Guest for FrontConsolePluginImplementation {
    async fn init(config_json: String) -> Vec<String> {
        // 1. Парсим конфигурацию
        let parsed_config: FrontConsolePluginConfig = serde_json::from_str(&config_json)
            .unwrap_or_else(|_| FrontConsolePluginConfig {
                subscriptions: vec![
                    PLUGIN_NAME.to_string(),
                    "info".to_string(),
                    "error".to_string(),
                ],
                target: vec!["choir".to_string()],
            });

        let topics_to_subscribe = parsed_config.subscriptions.clone();

        // 2. Инициализируем стейт
        {
            let mut config_lock = CONFIG.lock().unwrap();
            *config_lock = Some(parsed_config);

            let mut sessions_lock = SESSIONS.lock().unwrap();
            *sessions_lock = Some(HashMap::new());
        }

        info!(
            "[WASM] Плагин инициализирован. Запрошено подписок: {}",
            topics_to_subscribe.len()
        );

        // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
        topics_to_subscribe
    }

    async fn run() {
        info!("[WASM] Запуск фонового цикла плагина {}", PLUGIN_NAME);
        // Чтение пользовательского ввода из консоли.
        // Получаем нативный InputStream из подсистемы WASI, которую сгенерировал wit-bindgen.
        // В зависимости от вашей версии wit-bindgen путь может быть:
        // wasi::cli::stdin::get_stdin() ИЛИ вызов std::io::stdin() напрямую,
        // так как стандартная библиотека Rust под target_arch="wasm32-wasip2"
        // автоматически мапит std::io::stdin() на этот интерфейс!
        loop {
            // В контексте WASI Component Model этот вызов блокирует только текущую "микропрограмму" (fiber),
            // оставляя планировщик хоста свободным для вызовов handle_event.
            match ai::host::console::read_line(PROMPT.to_string()).await {
                None => {
                    // EOF - поток ввода закрылся
                    info!("[WASM] Поток stdin завершен.");
                    break;
                }
                Some(buffer) => {
                    info!("[WASM] Новое сообщение из stdin.");
                    let trimmed = buffer.trim();
                    if !trimmed.is_empty() {
                        let host_event = Event {
                            request_id: Uuid::new_v4().to_string(),
                            session_id: Uuid::new_v4().to_string(),
                            source: PLUGIN_NAME.to_string(),
                            target: "*".to_string(),
                            topic: "request".to_string(),
                            payload: trimmed.to_string(),
                        };
                        // Отправка события в хост
                        ai::host::event_bus::publish_event(&host_event);
                    }
                }
            }
        }
    }

    async fn handle_event(ev: Event) {
        // ХОСТ ВЫЗВАЛ ЭТОТ МЕТОД ПАРАЛЛЕЛЬНО
        // Данный метод выполняется асинхронно и независимо от того,
        // ждет ли сейчас функция read_line() ввода в консоли.

        debug!("[WASM] Получен ивент от хоста: {:?}", ev);

        debug!("{}: {}", ev.topic, ev.payload);

        // Если это про печать в консоль - выводим
        if ev.topic == "print" {
            println!("{}", ev.payload);
        }

        // Имитация пинга
        if ev.topic == "request" {
            let host_event = Event {
                request_id: ev.request_id,
                session_id: ev.session_id,
                source: PLUGIN_NAME.to_string(),
                target: "*".to_string(),
                topic: "print".to_string(),
                payload: ev.payload,
            };
            ai::host::event_bus::publish_event(&host_event);
        }
    }
}

export!(FrontConsolePluginImplementation);
