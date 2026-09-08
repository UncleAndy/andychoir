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
    // Кому плагин будет отправлять сообщения.
    // В конфиге ключ называется "targets" (мн.ч.); алиас + default, чтобы
    // поле не было обязательным и не ломало парсинг остальных полей.
    #[serde(default, alias = "targets")]
    #[allow(dead_code)]
    target: Vec<String>,
    // Режим управления промптом/чтением ввода:
    //   "wait"   (default) — после отправки запроса НЕ читаем ввод до ответа;
    //   "queue"  — после ответа отправляем следующее из очереди (ввод копится);
    //   "direct" — ввод всегда доступен, запросы уходят сразу (параллельно).
    #[serde(default = "default_mode")]
    mode: String,
    // Если true — пришедший ответ выводить с форматированием Markdown.
    #[serde(default = "default_markdown")]
    markdown: bool,
}

fn default_mode() -> String {
    "wait".to_string()
}

fn default_markdown() -> bool {
    false
}

const PLUGIN_NAME: &str = "front:console";

static CONFIG: Mutex<Option<FrontConsolePluginConfig>> = Mutex::new(None);
static SESSIONS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);
/// Текущий session_id сессии (стабилен для всей сессии, меняется только
/// при чистом старте без сохранённой сессии или по команде /new).
static CURRENT_SESSION: Mutex<Option<String>> = Mutex::new(None);

const PROMPT: &str = "prompt> ";

/// Получить флаг `markdown` из конфига плагина, читая его с ХОСТА
/// (get_plugin_config), а не из static CONFIG. Нужно потому, что для
/// allow_background:true плагин живёт в двух былm-инстансах (основной init и
/// фоновый run) с разными static-данными; конфиг же должен быть одинаковым.
/// Хост хранит конфиг и отдаёт его любому инстансу.
async fn markdown_enabled() -> bool {
    let cfg_json = crate::ai::host::host_control::get_plugin_config().await;
    serde_json::from_str::<FrontConsolePluginConfig>(&cfg_json)
        .map(|c| c.markdown)
        .unwrap_or(false)
}

/// Печать двойной разделительной линии `═` для визуального выделения ответа.
const DIVIDER_LEN: usize = 60;
fn print_divider() {
    // ВАЖНО: здесь НЕЛЬЗЯ использовать локальный макрос println! — он объявлен
    // ниже в этом файле (macro_rules! println), а макросы доступны только после
    // объявления. Поэтому println! здесь резолвился бы в std::println! -> stdout
    // былm (не подключён к консоли хоста). Используем прямой WIT-импорт.
    crate::ai::host::console::print_line(&format!("{}", "═".repeat(DIVIDER_LEN)));
}

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
        let parsed_result = serde_json::from_str::<FrontConsolePluginConfig>(&config_json);
        let parsed_config: FrontConsolePluginConfig = match parsed_result {
            Ok(c) => c,
            Err(e) => {
                // НЕ молча падаем в fallback: ошибка конфига должна остановить
                // запуск хоста (былm trap -> хост прерывает запуск с ошибкой).
                panic!("[WASM] front:console: неверный конфиг плагина: {}; raw={}", e, config_json);
            }
        };
        // Диагностика (полезно в логе): инициализация с реальными настройками.
        let topics_to_subscribe = parsed_config.subscriptions.clone();
        let md_enabled = parsed_config.markdown;

        // 2. Инициализируем стейт
        {
            let mut config_lock = CONFIG.lock().unwrap();
            *config_lock = Some(parsed_config);

            let mut sessions_lock = SESSIONS.lock().unwrap();
            *sessions_lock = Some(HashMap::new());
        }

        info!(
            "[WASM] Плагин {} инициализирован. Запрошено подписок: {}, markdown={}",
            PLUGIN_NAME,
            topics_to_subscribe.len(),
            md_enabled
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

        // Восстанавливаем (или создаём) текущую сессию.
        {
            let sid = crate::ai::host::host_control::get_current_session_id().await;
            let mut cur = CURRENT_SESSION.lock().unwrap();
            *cur = Some(sid.clone());
            drop(cur);

            // Показываем предыдущую историю сессии (если есть).
            // response из истории тоже форматируем как Markdown, если включено.
            let md = markdown_enabled().await;
            let history = crate::ai::host::host_control::get_session_history(sid.clone()).await;
            info!(
                "[WASM] front:console: восстановление сессии {}: событий={}, markdown={}",
                sid,
                history.len(),
                md
            );
            for ev in &history {
                if ev.topic == "request" {
                    // Запрос пользователя — жёлтым (ANSII).
                    crate::ai::host::console::print_line(
                        &format!("\x1b[33m> {}\x1b[0m", ev.payload),
                    );
                } else if ev.topic == "response" {
                    // Выделяем восстановленный ответ двойной линией.
                    print_divider();
                    if md {
                        crate::ai::host::console::print_markdown(&ev.payload);
                    } else {
                        println!("{}", ev.payload);
                    }
                    print_divider();
                }
            }
        }

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
            // 1) Читаем ввод (промпт показываем всегда; в wait/queue после
            //    отправки мы не возвращаемся сюда до ответа).
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
                    // Команды: /new — новая сессия; /help — справка.
                    if trimmed.starts_with('/') {
                        handle_command(&trimmed).await;
                        continue;
                    }
                    // Режим direct: сразу отправляем и продолжаем читать.
                    if mode == "direct" {
                        publish_request(&trimmed).await;
                        continue;
                    }
                    // wait/queue: отправляем запрос и ждём ответ (async, не
                    // блокирует wasm). Пока ждём — ввод НЕ читается (нет промпта).
                    let (pending, pending_session) = publish_request(&trimmed).await;
                    crate::ai::host::host_control::wait_for_response(pending, pending_session).await;
                    // queue: если есть накопленное — отправляем следующее.
                    if mode == "queue" {
                        while let Some(next) = queue.pop_front() {
                            let (qprid, qsession) = publish_request(&next).await;
                            crate::ai::host::host_control::wait_for_response(qprid, qsession).await;
                        }
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
            // Форматирование Markdown: если включено в конфиге, просим ХОСТ
            // отрендерить Markdown через termimad (ANSI) вместо сырого текста.
            let md = markdown_enabled().await;
            info!(
                "[WASM] front:console вывод response: markdown={} len={}",
                md,
                ev.payload.len()
            );
            // Выделяем ответ двойной линией сверху и снизу.
            print_divider();
            if md {
                crate::ai::host::console::print_markdown(&ev.payload);
            } else {
                println!("{}", ev.payload);
            }
            print_divider();
        }

        // NOTE: эхо введённой строки (topic == "request") намеренно убрано —
        // иначе консоль дублирует пользовательский ввод. Маршрутизацию
        // пользовательского запроса агенту выполняет фронт в run() (target="agent:*").
    }
}

export!(FrontConsolePluginImplementation);

/// Отправить пользовательский запрос агенту (target:"agent:*") и вернуть
/// (request_id, session_id). Пара нужна для wait_for_response (A2: ключ
/// (request_id, session_id) — только своя сессия).
async fn publish_request(payload: &str) -> (String, String) {
    let request_id = Uuid::new_v4().to_string();
    // Используем стабильный session_id текущей сессии.
    let session_id = CURRENT_SESSION
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let host_event = Event {
        request_id: request_id.clone(),
        session_id: session_id.clone(),
        source: PLUGIN_NAME.to_string(),
        target: "agent:*".to_string(),
        topic: "request".to_string(),
        payload: payload.to_string(),
    };
    ai::host::event_bus::publish_event(&host_event);
    (request_id, session_id)
}

/// Обработать команду пользователя (начинается с '/').
async fn handle_command(cmd: &str) {
    match cmd {
        "/new" => {
            // Новая сессия: очищаем старую историю, генерим новый id.
            let old = CURRENT_SESSION.lock().unwrap().clone();
            let new_id = crate::ai::host::host_control::new_session_id().await;
            if let Some(old_id) = old {
                crate::ai::host::host_control::clear_session(old_id).await;
            }
            let mut cur = CURRENT_SESSION.lock().unwrap();
            *cur = Some(new_id);
            println!("[Система] Начата новая сессия.");
        }
        "/help" => {
            println!(
                "[Система] Команды: /new — начать новую сессию, /help — эта справка."
            );
        }
        _ => {
            println!("[Система] Неизвестная команда: {} (введите /help)", cmd);
        }
    }
}

