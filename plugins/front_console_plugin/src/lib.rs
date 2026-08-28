wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize, Clone)]
struct FrontConsolePluginConfig {
    // Плагин может принимать из конфига топики, которые ему нужно слушать
    subscriptions: Vec<String>,
    // Кому плагин будет отправлять сообщения
    #[allow(dead_code)]
    target: Vec<String>,
    // Режим управления промптом/чтением ввода:
    //   "wait"   (default) — после отправки запроса НЕ читаем ввод до ответа;
    //   "queue"  — после ответа отправляем следующее из очереди (ввод копится);
    //   "direct" — ввод всегда доступен, запросы уходят сразу (параллельно).
    #[serde(default = "default_mode")]
    mode: String,
}

fn default_mode() -> String {
    "wait".to_string()
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
                mode: default_mode(),
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
            "[WASM] Плагин {} инициализирован. Запрошено подписок: {}",
            PLUGIN_NAME,
            topics_to_subscribe.len()
        );

        // Сообщаем хосту о готовности (хост агрегирует и публикует host:"ready").
        // Само приглашение (prompt>) показываем ТОЛЬКО после host:"ready"
        // (см. run()), чтобы не спамить промпт до готовности всех плагинов.
        crate::ai::host::event_bus::publish_event(&Event {
            request_id: "-".to_string(),
            session_id: "-".to_string(),
            source: PLUGIN_NAME.to_string(),
            target: "*".to_string(),
            topic: "status".to_string(),
            payload: "ready".to_string(),
        });

        // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
        topics_to_subscribe
    }

    async fn run() {
        info!("[WASM] Запуск фонового цикла плагина {}", PLUGIN_NAME);

        // Ждём, пока хост не сообщит, что ВСЕ плагины готовы (host:"ready").
        // wait-for-ready — это async-вызов к хосту: он отдаёт управление
        // wasm-планировщику (handle_event может выполняться), а хост сигналит
        // Notify, когда все плагины готовы. Без блокировки wasm-нити.
        crate::ai::host::console::print_line("[Система] Загрузка плагинов...");
        crate::ai::host::host_control::wait_for_ready().await;
        crate::ai::host::console::print_line("[Система] Все плагины готовы. Можете вводить запросы.");

        // Чтение пользовательского ввода из консоли с управлением режимами.
        // "wait"/"queue": после отправки запроса НЕ читаем ввод до ответа
        // (нет ни промпта, ни чтения). "direct": ввод всегда доступен.
        let mode = CONFIG
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.mode.clone())
            .unwrap_or_else(default_mode);

        // Очередь сообщений для режима "queue".
        let mut queue: VecDeque<String> = VecDeque::new();

        loop {
            // Флаг: ждём ли мы ответ на отправленный запрос.
            // В wait/queue он true после отправки, в direct — всегда false.
            let mut awaiting = false;
            let mut pending_request_id = String::new();

            // 1) Читаем ввод (промпт показываем, только если готовы принять).
            //    В wait/queue после отправки сюда не вернёмся до ответа.
            let line = ai::host::console::read_line(PROMPT.to_string()).await;
            match line {
                None => {
                    info!("[WASM] Поток stdin завершен.");
                    break;
                }
                Some(buffer) => {
                    let trimmed = buffer.trim().to_string();
                    if trimmed.is_empty() {
                        continue;
                    }
                    // Режим direct: сразу отправляем и продолжаем читать.
                    if mode == "direct" {
                        publish_request(&trimmed);
                        continue;
                    }
                    // wait/queue: если уже ждём ответ — копим в очередь (queue)
                    // либо игнорируем (wait не читает ввод, поэтому сюда
                    // попадём только в queue-итерации — см. ниже).
                    if awaiting {
                        queue.push_back(trimmed);
                        continue;
                    }
                    // Отправляем первый запрос.
                    pending_request_id = publish_request(&trimmed);
                    awaiting = true;
                }
            }

            // 2) В wait/queue ждём ответ на отправленный запрос (async, не
            //    блокирует wasm). Пока ждём — ввод НЕ читается (нет промпта).
            if awaiting && mode != "direct" {
                crate::ai::host::host_control::wait_for_response(pending_request_id).await;
                awaiting = false;
                // queue: если есть накопленное — отправляем следующее.
                if mode == "queue" {
                    while let Some(next) = queue.pop_front() {
                        pending_request_id = publish_request(&next);
                        crate::ai::host::host_control::wait_for_response(pending_request_id)
                            .await;
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

        // Прогресс обработки запроса (от агентов/инструментов) — показываем.
        // События status с payload=="ready" — это сигналы готовности, НЕ прогресс,
        // их не печатаем (готовность хост обрабатывает через wait-for-ready).
        if ev.topic == "status" && ev.payload != "ready" {
            println!("  ⏳ {}", ev.payload);
        }

        // Если это про печать в консоль - выводим
        if ev.topic == "print" || ev.topic == "response" {
            println!("{}", ev.payload);
        }

        // NOTE: эхо введённой строки (topic == "request") намеренно убрано —
        // иначе консоль дублирует пользовательский ввод. Маршрутизацию
        // пользовательского запроса агенту выполняет фронт в run() (target="agent:*").
    }
}

export!(FrontConsolePluginImplementation);

/// Отправить пользовательский запрос агенту (target:"agent:*") и вернуть request_id.
fn publish_request(payload: &str) -> String {
    let request_id = Uuid::new_v4().to_string();
    let host_event = Event {
        request_id: request_id.clone(),
        session_id: Uuid::new_v4().to_string(),
        source: PLUGIN_NAME.to_string(),
        target: "agent:*".to_string(),
        topic: "request".to_string(),
        payload: payload.to_string(),
    };
    ai::host::event_bus::publish_event(&host_event);
    request_id
}
