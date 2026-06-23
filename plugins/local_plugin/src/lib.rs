wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Deserialize)]
struct LocalPluginConfig {
    // Плагин может принимать из конфига топики, которые ему нужно слушать
    subscriptions: Vec<String>,
    #[allow(dead_code)]
    system_prompt: Option<String>,
}

static CONFIG: Mutex<Option<LocalPluginConfig>> = Mutex::new(None);
static SESSIONS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

struct LocalPluginImplementation;

impl Guest for LocalPluginImplementation {

    // Изменяем сигнатуру: возвращаем Vec<String> хосту
    fn init(config_json: String) -> Vec<String> {
        // 1. Парсим конфигурацию плагина
        let parsed_config: LocalPluginConfig = serde_json::from_str(&config_json)
            .unwrap_or_else(|_| LocalPluginConfig {
                // Дефолтные подписки, если конфиг пустой
                subscriptions: vec!["agent:start".to_string(), "ai:response".to_string()],
                system_prompt: None,
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

export!(LocalPluginImplementation);
