wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::collections::HashMap;
use std::io::BufRead;
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

        println!(
            "[WASM] Плагин инициализирован. Запрошено подписок: {}",
            topics_to_subscribe.len()
        );

        // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
        topics_to_subscribe
    }

    async fn handle_event(ev: Event) {
        // ХОСТ ВЫЗВАЛ ЭТОТ МЕТОД ПАРАЛЛЕЛЬНО
        // Данный метод выполняется асинхронно и независимо от того,
        // ждет ли сейчас функция read_line() ввода в консоли.

        if ev.topic == "agent:start" {
            println!("[WASM] Получен агент старт от хоста: {}", ev.payload);

            // TODO - здесь будет обработка входящих событий
            println!("{:?}", ev);
        }
    }
    async fn run() {
        println!("[WASM] Запуск фонового цикла плагина {}", PLUGIN_NAME);
        // Чтение пользовательского ввода из консоли.
        // Получаем нативный InputStream из подсистемы WASI, которую сгенерировал wit-bindgen.
        // В зависимости от вашей версии wit-bindgen путь может быть:
        // wasi::cli::stdin::get_stdin() ИЛИ вызов std::io::stdin() напрямую,
        // так как стандартная библиотека Rust под target_arch="wasm32-wasip2"
        // автоматически мапит std::io::stdin() на этот интерфейс!
        let stdin = std::io::stdin();
        let mut reader = std::io::BufReader::new(stdin.lock());
        let mut buffer = String::new();

        loop {
            buffer.clear();

            // В контексте WASI Component Model этот вызов блокирует только текущую "микропрограмму" (fiber),
            // оставляя планировщик хоста свободным для вызовов handle_event.
            match reader.read_line(&mut buffer) {
                Ok(0) => {
                    // EOF - поток ввода закрылся
                    println!("[WASM] Поток stdin завершен.");
                    break;
                }
                Ok(_) => {
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
                Err(e) => {
                    eprintln!("[WASM Error] Ошибка чтения: {:?}", e);
                    break;
                }
            }
        }
    }
}

export!(FrontConsolePluginImplementation);
