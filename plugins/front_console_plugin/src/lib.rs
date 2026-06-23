wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Deserialize)]
struct FrontConsolePluginConfig {
    // Плагин может принимать из конфига топики, которые ему нужно слушать
    subscriptions: Vec<String>,
    // Кому плагин будет отправлять сообщения
    #[allow(dead_code)]
    target: Vec<String>,
}

static CONFIG: Mutex<Option<FrontConsolePluginConfig>> = Mutex::new(None);
static SESSIONS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

struct FrontConsolePluginImplementation;

impl Guest for FrontConsolePluginImplementation {
    // Изменяем сигнатуру: возвращаем Vec<String> хосту
    fn init(config_json: String) -> Vec<String> {
        // 1. Парсим конфигурацию плагина
        let parsed_config: FrontConsolePluginConfig = serde_json::from_str(&config_json)
            .unwrap_or_else(|_| FrontConsolePluginConfig {
                // Дефолтные подписки, если конфиг пустой
                subscriptions: vec![
                    "front:console".to_string(),
                    "info".to_string(),
                    "error".to_string(),
                ],
                target: vec![
                    "choir".to_string(),
                ],
            });

        // 2. Сохраняем список подписок для хоста, чтобы вернуть его в конце
        let topics_to_subscribe = parsed_config.subscriptions.clone();

        // 3. Инициализируем внутренний стейт плагина
        let mut config_lock = CONFIG.lock().unwrap();
        *config_lock = Some(parsed_config);

        let mut sessions_lock = SESSIONS.lock().unwrap();
        *sessions_lock = Some(HashMap::new());

        println!("[WASM] Плагин инициализирован. Запрошено подписок: {}", topics_to_subscribe.len());

        // Возвращаем вектор хосту. wit-bindgen сам переведет его в list<string> на уровне Wasm
        topics_to_subscribe
    }

    fn handle_event(ev: Event) {
        // Логика обработки ивентов (остается прежней)
        if ev.topic == "agent:start" {
            // ...
        }
    }
}

export!(FrontConsolePluginImplementation);
