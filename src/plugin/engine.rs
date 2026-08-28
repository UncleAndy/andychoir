use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock};

use tokio::sync::{Mutex, RwLock, Notify, mpsc};
use tokio::task::JoinHandle;
use wasmtime::component::{Component, HasData, Linker, ResourceTable};
use wasmtime::{Config as WasmtimeConfig, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::{debug, error, info, warn, HostPlugin};
use crate::ai::host::types::Event;
use crate::config::Config;
use crate::exports::ai::host::plugin_lifecycle::Guest;
use crate::messages::bus::PluginRegistry;
use crate::plugin::config::{PluginAccess, PluginConfig};

pub struct PluginInstance {
    pub config: PluginConfig,
    pub topics: Vec<String>,
    pub lifecycle: Guest,
    pub store: Arc<Mutex<Store<ChoirHostState>>>,
}

pub struct BackgroundPluginHandle {
    plugin_name: String,
    handle: JoinHandle<()>,
}

impl BackgroundPluginHandle {
    pub async fn shutdown(self) {
        self.handle.abort();

        match self.handle.await {
            Ok(()) => info!(
                "[Хост] Фоновый процесс плагина {} завершён.",
                self.plugin_name
            ),
            Err(err) if err.is_cancelled() => info!(
                "[Хост] Фоновый процесс плагина {} остановлен.",
                self.plugin_name
            ),
            Err(err) => error!(
                "[Хост] Фоновый процесс плагина {} завершился с ошибкой: {:?}",
                self.plugin_name, err
            ),
        }
    }
}

pub struct ChoirHostState {
    wasi: WasiCtx,
    table: ResourceTable,
    event_sender: mpsc::Sender<Event>,
    pub current_plugin_permissions: Option<Vec<PluginAccess>>,
}

/// Глобальный сигнал готовности: хост сигналит, когда все плагины готовы
/// (host:"ready"). Консоль (и др.) await-ит его через host-control.wait-for-ready.
static READINESS_NOTIFY: OnceLock<Arc<Notify>> = OnceLock::new();

fn readiness_notify() -> &'static Arc<Notify> {
    READINESS_NOTIFY.get_or_init(|| Arc::new(Notify::new()))
}

/// Сигналить ожидающим плагинам, что все плагины готовы. Идемпотентно.
pub fn signal_host_ready() {
    readiness_notify().notify_waiters();
}

/// Карта ожидающих ответов: request_id -> Notify. Хост сигналит Notify,
/// когда приходит событие topic:"response" с этим request_id.
static PENDING_RESPONSES: OnceLock<tokio::sync::Mutex<HashMap<String, Arc<Notify>>>> =
    OnceLock::new();

fn pending_responses() -> &'static tokio::sync::Mutex<HashMap<String, Arc<Notify>>> {
    PENDING_RESPONSES.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

/// Зарегистрировать ожидание ответа на запрос request_id. Возвращает Notify,
/// который сигналит хост, когда придёт response с этим id.
pub async fn register_wait_response(request_id: String) -> Arc<Notify> {
    let mut map = pending_responses().lock().await;
    if let Some(existing) = map.get(&request_id) {
        return existing.clone();
    }
    let notify = Arc::new(Notify::new());
    map.insert(request_id, notify.clone());
    notify
}

/// Сигналить ожидающим ответ на request_id (вызывается при response).
pub async fn signal_response(request_id: &str) {
    let notify = {
        let mut map = pending_responses().lock().await;
        map.remove(request_id)
    };
    if let Some(n) = notify {
        n.notify_waiters();
    }
}

/// Глобальное хранилище историй сессий (вариант A).
/// session_id -> упорядоченные диалоговые события (request/response).
/// RwLock: много читателей (агент history_get, saver-снимок), редкие короткие
/// записи (append). Читатели идут параллельно, блокируются только на запись.
static SESSION_HISTORIES: OnceLock<RwLock<HashMap<String, Vec<Event>>>> = OnceLock::new();

/// Время последнего изменения каждой сессии (для TTL и решения о сохранении).
/// std::sync::Mutex: почти всегда пишется append-ом, RwLock выигрыша не даёт.
static SESSION_MTIME: OnceLock<StdMutex<HashMap<String, std::time::Instant>>> = OnceLock::new();

/// Набор session_id, изменившихся с последнего автосохранения (команда saver-у).
/// DashSet: конкурентная структура, добавление/чтение без долгих блокировок.
static DIRTY_SESSIONS: OnceLock<dashmap::DashSet<String>> = OnceLock::new();

fn session_histories() -> &'static RwLock<HashMap<String, Vec<Event>>> {
    SESSION_HISTORIES.get_or_init(|| RwLock::new(HashMap::new()))
}

fn session_mtimes() -> &'static StdMutex<HashMap<String, std::time::Instant>> {
    SESSION_MTIME.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn dirty_sessions() -> &'static dashmap::DashSet<String> {
    DIRTY_SESSIONS.get_or_init(dashmap::DashSet::new)
}

/// Полное описание инструмента (name, description, параметры JSON).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub parameters_json: String,
}

/// Реестр ЛОКАЛЬНЫХ инструментов хоста: name -> ToolDef.
/// Заполняется при загрузке tool-плагинов (их определения из конфига).
static LOCAL_TOOLS: OnceLock<RwLock<HashMap<String, ToolDef>>> = OnceLock::new();

/// Инструменты СЕССИИ (добавленные в ходе работы, из подключённых хостов):
/// session_id -> name -> ToolDef. Приоритет над локальными при конфликте.
static SESSION_TOOLS: OnceLock<RwLock<HashMap<String, HashMap<String, ToolDef>>>> = OnceLock::new();

pub fn local_tools() -> &'static RwLock<HashMap<String, ToolDef>> {
    LOCAL_TOOLS.get_or_init(|| RwLock::new(HashMap::new()))
}

fn session_tools() -> &'static RwLock<HashMap<String, HashMap<String, ToolDef>>> {
    SESSION_TOOLS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Зарегистрировать локальный инструмент хоста (вызывается при загрузке
/// tool-плагина).
pub async fn register_local_tool(def: ToolDef) {
    local_tools().write().await.insert(def.name.clone(), def);
}

/// Добавить инструменты в сессию (например, при подключении удалённого хоста,
/// инструменты которого привязаны к этому коннекту/сессии).
pub async fn add_session_tools(session_id: &str, defs: Vec<ToolDef>) {
    let mut map = session_tools().write().await;
    let entry = map.entry(session_id.to_string()).or_default();
    for d in defs {
        entry.insert(d.name.clone(), d);
    }
}

/// Инструменты, доступные сессии: <инструменты сессии> + <локальные>,
/// с приоритетом инструмента сессии при конфликте.
pub async fn get_session_tools(session_id: &str) -> Vec<ToolDef> {
    let mut result: HashMap<String, ToolDef> = HashMap::new();
    // Сначала локальные (будут перекрыты сессионными при конфликте).
    for (k, v) in local_tools().read().await.iter() {
        result.insert(k.clone(), v.clone());
    }
    // Затем инструменты сессии (перекрывают локальные).
    if let Some(map) = session_tools().read().await.get(session_id) {
        for (k, v) in map.iter() {
            result.insert(k.clone(), v.clone());
        }
    }
    result.into_values().collect()
}

/// Добавить диалоговое событие в историю сессии (с FIFO-лимитом),
/// обновить время последнего изменения и пометить сессию как «грязную»
/// (saver-цикл перепишет её файл).
pub async fn history_append(session_id: &str, ev: &Event, max_events: usize) {
    {
        let mut map = session_histories().write().await;
        let entries = map.entry(session_id.to_string()).or_default();
        if entries.len() >= max_events {
            let overflow = entries.len() - (max_events.saturating_sub(1));
            entries.drain(..overflow);
        }
        entries.push(ev.clone());
    }
    // Время последнего изменения (микросекунды, отдельный Mutex).
    if let Ok(mut m) = session_mtimes().lock() {
        m.insert(session_id.to_string(), std::time::Instant::now());
    }
    // Команда saver-у: эта сессия изменилась, её файл надо переписать.
    dirty_sessions().insert(session_id.to_string());
}

/// Получить копию истории сессии (в порядке: старые -> новые).
pub async fn history_get(session_id: &str) -> Vec<Event> {
    let map = session_histories().read().await;
    map.get(session_id).cloned().unwrap_or_default()
}

/// Очистить историю сессии (и убрать из набора изменённых).
pub async fn history_clear(session_id: &str) {
    {
        let mut map = session_histories().write().await;
        map.remove(session_id);
    }
    if let Ok(mut m) = session_mtimes().lock() {
        m.remove(session_id);
    }
    dirty_sessions().remove(session_id);
}

/// Вернуть все истории (для сохранения на диск при shutdown).
pub async fn history_all() -> HashMap<String, Vec<Event>> {
    session_histories().read().await.clone()
}

/// Загрузить истории в хранилище (при старте из файла).
pub async fn history_load(data: HashMap<String, Vec<Event>>) {
    let mut map = session_histories().write().await;
    map.extend(data);
}

/// Сохранить ОДНУ сессию в файл <dir>/<sid>.json (если она существует).
/// Используется saver-циклом. Запись на диск ВНЕ lock (только снимок под lock).
pub async fn save_session_to_disk(dir: &str, sid: &str) {
    let events: Vec<Event> = {
        let map = session_histories().read().await;
        map.get(sid).cloned().unwrap_or_default()
    };
    if events.is_empty() {
        return;
    }
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    let path = format!("{}/{}.json", dir, sid);
    let evs: Vec<serde_json::Value> = events.iter().map(event_to_value).collect();
    let data = serde_json::json!({ "session_id": sid, "events": evs });
    let _ = tokio::fs::write(&path, data.to_string()).await;
}

/// Забрать (drain) набор «грязных» сессий для автосохранения.
/// Возвращает список session_id, файлы которых нужно переписать.
pub fn take_dirty_sessions() -> Vec<String> {
    let ids: Vec<String> = dirty_sessions()
        .iter()
        .map(|r| r.clone())
        .collect();
    for id in &ids {
        dirty_sessions().remove(id);
    }
    ids
}

/// Сгенерировать новый session_id (хост централизованно).
pub fn new_session_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Один HTTP-слушатель, зарегистрированный http-фронтом.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HttpListener {
    pub port: u16,
    pub path: String,
    pub target: String,
}

/// Один WebSocket-слушатель, зарегистрированный ws-фронтом.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WsListener {
    pub port: u16,
    pub path: String,
    pub target: String,
}

/// Глобальное хранилище HTTP-слушателей: (port, path) -> listener.
/// Используем std::sync::Mutex (не tokio), чтобы обращение было синхронным
/// и не зависало внутри axum-обработчика.
static HTTP_LISTENERS: OnceLock<StdMutex<HashMap<(u16, String), HttpListener>>> = OnceLock::new();

fn http_listeners() -> &'static StdMutex<HashMap<(u16, String), HttpListener>> {
    HTTP_LISTENERS.get_or_init(|| StdMutex::new(HashMap::new()))
}

/// Зарегистрировать HTTP-слушатель. Возвращает false, если (порт,путь) занят.
pub fn http_listen(l: HttpListener) -> bool {
    let key = (l.port, l.path.clone());
    let mut map = http_listeners().lock().unwrap();
    if map.contains_key(&key) {
        return false;
    }
    map.insert(key, l);
    true
}

/// Снять HTTP-слушатель.
pub fn http_remove(port: u16, path: &str) -> bool {
    http_listeners()
        .lock()
        .unwrap()
        .remove(&(port, path.to_string()))
        .is_some()
}

/// Список активных слушателей.
pub fn http_listeners_snapshot() -> Vec<HttpListener> {
    http_listeners().lock().unwrap().values().cloned().collect()
}

/// Глобальное хранилище WebSocket-слушателей: (port, path) -> listener.
/// std::sync::Mutex (как у http), чтобы обращение было синхронным.
static WS_LISTENERS: OnceLock<StdMutex<HashMap<(u16, String), WsListener>>> = OnceLock::new();

fn ws_listeners() -> &'static StdMutex<HashMap<(u16, String), WsListener>> {
    WS_LISTENERS.get_or_init(|| StdMutex::new(HashMap::new()))
}

/// Зарегистрировать WS-слушатель. Возвращает false, если (порт,путь) занят.
pub fn ws_listen(l: WsListener) -> bool {
    let key = (l.port, l.path.clone());
    let mut map = ws_listeners().lock().unwrap();
    if map.contains_key(&key) {
        return false;
    }
    map.insert(key, l);
    true
}

/// Снять WS-слушатель.
pub fn ws_remove(port: u16, path: &str) -> bool {
    ws_listeners()
        .lock()
        .unwrap()
        .remove(&(port, path.to_string()))
        .is_some()
}

/// Список активных WS-слушателей.
pub fn ws_listeners_snapshot() -> Vec<WsListener> {
    ws_listeners().lock().unwrap().values().cloned().collect()
}

/// Получить (или создать) текущий session_id для проекта.
/// Читает ~/.andychour/current_session.json; если там нет id — генерит новый
/// и сохраняет. Путь файла настраивается в конфиге (history.current_session_file).
pub async fn get_or_create_current_session(current_session_file: &str) -> String {
    let path = expand_tilde(current_session_file);
    if let Ok(content) = tokio::fs::read_to_string(&path).await {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(id) = v.get("session_id").and_then(|x| x.as_str()) {
                if !id.is_empty() {
                    return id.to_string();
                }
            }
        }
    }
    let id = new_session_id();
    save_current_session(current_session_file, &id).await;
    id
}

/// Сохранить текущий session_id в ~/.andychour/current_session.json.
pub async fn save_current_session(current_session_file: &str, id: &str) {
    let path = expand_tilde(current_session_file);
    if let Some(parent) = std::path::Path::new(&path).parent() {
        if std::fs::create_dir_all(parent).is_err() {
            // не критично — просто не сможем сохранить
        }
    }
    let data = serde_json::json!({ "session_id": id });
    let _ = tokio::fs::write(&path, data.to_string()).await;
}

/// Развернуть `~` в домашний каталог.
fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home, rest);
        }
    }
    path.to_string()
}

/// Путь к файлу текущей сессии (из конфига history.current_session_file),
/// устанавливается при старте хоста.
static CURRENT_SESSION_FILE: OnceLock<String> = OnceLock::new();

pub fn set_current_session_file(path: String) {
    let _ = CURRENT_SESSION_FILE.set(path);
}

fn current_session_file() -> &'static str {
    CURRENT_SESSION_FILE.get().map(String::as_str).unwrap_or("~/.andychour/current_session.json")
}

/// Сохранить все истории сессий в каталог dir как <session_id>.json.
pub async fn save_histories_to_disk(dir: &str) {
    let all = history_all().await;
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    for (sid, events) in all {
        let path = format!("{}/{}.json", dir, sid);
        let evs: Vec<serde_json::Value> = events.iter().map(event_to_value).collect();
        let data = serde_json::json!({ "session_id": sid, "events": evs });
        let _ = tokio::fs::write(&path, data.to_string()).await;
    }
}

/// Загрузить все истории из каталога dir (файлы <session_id>.json).
/// Файлы, чей возраст (mtime) больше ttl_secs, удаляются (0 = не чистить).
pub async fn load_histories_from_disk(dir: &str, ttl_secs: u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut data: HashMap<String, Vec<Event>> = HashMap::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        // TTL-чистка: файл старше лимита удаляем и не грузим.
        if ttl_secs > 0 {
            let stale = std::fs::metadata(&path)
                .and_then(|m| m.modified())
                .map(|modified| {
                    modified.elapsed().map(|e| e.as_secs() > ttl_secs).unwrap_or(false)
                })
                .unwrap_or(false);
            if stale {
                let sid_hint = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("?")
                    .to_string();
                info!(
                    "[Хост] Удалена устаревшая сессия {} (возраст > TTL {}с)",
                    sid_hint, ttl_secs
                );
                let _ = std::fs::remove_file(&path);
                continue;
            }
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) {
            if let (Some(sid), Some(evs)) = (
                v.get("session_id").and_then(|x| x.as_str()),
                v.get("events").and_then(|x| x.as_array()),
            ) {
                let events: Vec<Event> = evs
                    .iter()
                    .filter_map(|e| value_to_event(e).ok())
                    .collect();
                data.insert(sid.to_string(), events);
            }
        }
    }
    history_load(data).await;
}

/// Event -> JSON (WIT record Event не имеет serde derive, конвертируем вручную).
fn event_to_value(ev: &Event) -> serde_json::Value {
    serde_json::json!({
        "request_id": ev.request_id,
        "session_id": ev.session_id,
        "source": ev.source,
        "target": ev.target,
        "topic": ev.topic,
        "payload": ev.payload,
    })
}

/// JSON -> Event.
fn value_to_event(v: &serde_json::Value) -> anyhow::Result<Event> {
    Ok(Event {
        request_id: v.get("request_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        session_id: v.get("session_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        source: v.get("source").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        target: v.get("target").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        topic: v.get("topic").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        payload: v.get("payload").and_then(|x| x.as_str()).unwrap_or("").to_string(),
    })
}

/// Проверка права плагина на вывод текста в консоль (console_print).
/// Возвращает true, если среди прав есть `ConsolePrint(max_size)` и длина <= max_size.
/// Выделено для юнит-тестирования (без состояния хоста).
pub(crate) fn can_plugin_print(perms: &[PluginAccess], text: &str) -> bool {
    perms.iter().any(|p| {
        if let PluginAccess::ConsolePrint(max_size) = p {
            text.len() <= *max_size as usize
        } else {
            false
        }
    })
}

/// Проверка права плагина на чтение из консоли (console_input).
/// Возвращает true, если среди прав есть `ConsoleInput(prompt)` и prompt совпадает с запрошенным.
/// Выделено для юнит-тестирования (без состояния хоста).
pub(crate) fn can_plugin_read_console(perms: &[PluginAccess], prompt: &str) -> bool {
    perms.iter().any(|p| {
        if let PluginAccess::ConsoleInput(allowed_prompt) = p {
            allowed_prompt == prompt
        } else {
            false
        }
    })
}

impl crate::ai::host::event_bus::Host for ChoirHostState {
    fn publish_event(&mut self, event: Event) -> () {
        info!("[Хост] Новое входящее событие: {:?}.", event);
        if let Err(err) = self.event_sender.try_send(event) {
            error!(
                "[Хост] Очередь входящих событий переполнена или закрыта: {}",
                err
            );
        }
    }
}

impl crate::ai::host::console::Host for ChoirHostState {
    fn print_line(&mut self, line: String) -> () {
        // Проверяем права плагина на работу с консолью.
        let has_access = self
            .current_plugin_permissions
            .as_ref()
            .map(|perms| can_plugin_print(perms, &line))
            .unwrap_or(false);
        if !has_access {
            error!("[Хост] Плагин не имеет доступа к выводу консоли с таким размером текста.");
            return ();
        }

        crate::host::console::print_line(format_args!("{}", line));
    }
}

impl crate::ai::host::log::Host for ChoirHostState {
    fn debug(&mut self, line: String) -> () {
        debug!("{}", line);
    }

    fn info(&mut self, line: String) -> () {
        info!("{}", line);
    }

    fn warn(&mut self, line: String) -> () {
        warn!("{}", line);
    }

    fn error(&mut self, line: String) -> () {
        error!("{}", line);
    }
}

// HTTP-прокси: плагин не имеет прямого сетевого доступа (песочница,
// fail-closed для сети). Хост выполняет реальный POST-запрос и возвращает
// (HTTP-статус, тело ответа). Узкий контракт: JSON in / JSON out.
//
// wit_bindgen генерирует и `Host` (для синхронных функций интерфейса), и
// `HostWithStore` (для async). Для async-интерфейса требуются оба трейта,
// поэтому здесь пустой `Host`, а вся логика — в `HostWithStore` ниже.
impl crate::ai::host::http::Host for ChoirHostState {}

impl crate::ai::host::http::HostWithStore<ChoirHostState> for ChoirHostState {
    async fn post_json(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        url: String,
        json_body: String,
    ) -> (u16, String) {
        // TODO(P1): здесь можно добавить проверку прав плагина на сетевой
        // доступ (PluginAccess::Network) — fail-closed, как для консоли.
        match reqwest::Client::new()
            .post(&url)
            .header("Content-Type", "application/json")
            .body(json_body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                (status, body)
            }
            Err(e) => {
                error!("[Хост] HTTP-прокси ошибка запроса к {}: {}", url, e);
                (0, format!("{{\"error\": \"{}\"}}", e))
            }
        }
    }
}

impl crate::ai::host::console::HostWithStore<ChoirHostState> for ChoirHostState {
    async fn read_line(
        accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        prompt: String,
    ) -> Option<String> {
        // Проверяем права плагина на работу с консолью.
        let has_access = accessor.with(|mut access| {
            // Внутри этого замыкания у нас есть эксклюзивный, временный доступ к состоянию
            let host_state = access.get();

            // Проверяем наличие PluginAccess::ConsoleInput в его конфиге
            host_state
                .current_plugin_permissions
                .as_ref()
                .map(|perms| can_plugin_read_console(perms, &prompt))
                .unwrap_or(false)
        });
        if !has_access {
            error!("[Хост] Плагин не имеет доступа к чтению консоли с таким промптом.");
            return None;
        }

        crate::host::console::read_prompted_line(prompt).await
    }
}

// Реализация host-control.wait-for-ready: блокирует до сигнала готовности.
impl crate::ai::host::host_control::Host for ChoirHostState {}

impl crate::ai::host::host_control::HostWithStore<ChoirHostState> for ChoirHostState {
    async fn wait_for_ready(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
    ) {
        // Отдаём управление планировщику и ждём сигнала от хоста.
        // notify_waiters() будит ВСЕх ожидающих, поэтому loop безопасен.
        readiness_notify().notified().await;
    }

    async fn wait_for_response(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        request_id: String,
    ) {
        // Регистрируем ожидание ответа и ждём Notify (async, не блокирует wasm).
        // Хост сигналит его в signal_response() при приходе topic:"response".
        let notify = register_wait_response(request_id).await;
        notify.notified().await;
    }

    async fn wait_for_response_timeout(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        request_id: String,
        timeout_ms: u64,
    ) -> bool {
        // Ждём ответ с таймаутом. Возвращает true, если ответ пришёл.
        let notify = register_wait_response(request_id).await;
        let duration = std::time::Duration::from_millis(timeout_ms);
        tokio::time::timeout(duration, notify.notified()).await.is_ok()
    }

    async fn get_session_history(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        session_id: String,
    ) -> Vec<crate::ai::host::types::Event> {
        history_get(&session_id).await
    }

    async fn clear_session(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        session_id: String,
    ) {
        history_clear(&session_id).await;
    }

    async fn new_session_id(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
    ) -> String {
        new_session_id()
    }

    async fn get_current_session_id(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
    ) -> String {
        get_or_create_current_session(current_session_file()).await
    }

    async fn get_session_tools(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        session_id: String,
    ) -> Vec<crate::ai::host::types::ToolDefinition> {
        let defs = get_session_tools(&session_id).await;
        defs.into_iter()
            .map(|d| crate::ai::host::types::ToolDefinition {
                name: d.name,
                description: d.description,
                parameters_json: d.parameters_json,
            })
            .collect()
    }
}

// Реализация http-server: регистрация/снятие HTTP-слушателей, которые
// затем обслуживает хостовый axum-сервер (transparent transport).
impl crate::ai::host::http_server::Host for ChoirHostState {}

impl crate::ai::host::http_server::HostWithStore<ChoirHostState> for ChoirHostState {
    async fn listen_http(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        l: crate::ai::host::http_server::Listener,
    ) -> bool {
        let port = l.port;
        let path = l.path.clone();
        let target = l.target.clone();
        let listener = HttpListener {
            port: l.port,
            path: l.path,
            target: l.target,
        };
        let ok = http_listen(listener);
        info!(
            "[Хост] HTTP-слушатель {}:{}/{} (target={}) -> {}",
            "0.0.0.0",
            port,
            path,
            target,
            if ok { "зарегистрирован" } else { "УЖЕ ЗАНЯТ" }
        );
        ok
    }

    async fn remove_listener(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        port: u16,
        path: String,
    ) -> bool {
        let ok = http_remove(port, &path);
        info!("[Хост] HTTP-слушатель {}:{} удалён: {}", port, path, ok);
        ok
    }

    async fn get_listeners(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
    ) -> Vec<crate::ai::host::http_server::Listener> {
        http_listeners_snapshot()
            .into_iter()
            .map(|l| crate::ai::host::http_server::Listener {
                port: l.port,
                path: l.path,
                target: l.target,
            })
            .collect()
    }
}

// Реализация ws-server: регистрация/снятие WebSocket-слушателей, которые
// затем обслуживает хостовый axum WS-сервер (transparent transport).
impl crate::ai::host::ws_server::Host for ChoirHostState {}

impl crate::ai::host::ws_server::HostWithStore<ChoirHostState> for ChoirHostState {
    async fn listen_ws(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        l: crate::ai::host::ws_server::WsListener,
    ) -> bool {
        let port = l.port;
        let path = l.path.clone();
        let target = l.target.clone();
        let listener = WsListener {
            port: l.port,
            path: l.path,
            target: l.target,
        };
        let ok = ws_listen(listener);
        info!(
            "[Хост] WS-слушатель {}:{}/{} (target={}) -> {}",
            "0.0.0.0",
            port,
            path,
            target,
            if ok { "зарегистрирован" } else { "УЖЕ ЗАНЯТ" }
        );
        ok
    }

    async fn remove_listener(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
        port: u16,
        path: String,
    ) -> bool {
        let ok = ws_remove(port, &path);
        info!("[Хост] WS-слушатель {}:{} удалён: {}", port, path, ok);
        ok
    }

    async fn get_listeners(
        _accessor: &wasmtime::component::Accessor<ChoirHostState, Self>,
    ) -> Vec<crate::ai::host::ws_server::WsListener> {
        ws_listeners_snapshot()
            .into_iter()
            .map(|l| crate::ai::host::ws_server::WsListener {
                port: l.port,
                path: l.path,
                target: l.target,
            })
            .collect()
    }
}

impl WasiView for ChoirHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl HasData for ChoirHostState {
    type Data<'a> = &'a mut ChoirHostState;
}

pub fn create_engine(config: &Config) -> anyhow::Result<Engine> {
    let mut engine_config = WasmtimeConfig::new();

    // Устанавливаем максимальный размер памяти для любого инстанса.
    // Значение задается в байтах.
    // Например, 256 МБ = 256 * 1024 * 1024
    engine_config.memory_reservation_for_growth(config.max_plugin_memory);
    engine_config.concurrency_support(true);

    Ok(Engine::new(&engine_config)?)
}

pub fn create_linker(engine: &Engine) -> anyhow::Result<Linker<ChoirHostState>> {
    let mut linker = Linker::<ChoirHostState>::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    crate::ai::host::event_bus::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;
    crate::ai::host::console::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;
    crate::ai::host::log::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;
    crate::ai::host::http::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;
    crate::ai::host::host_control::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;
    crate::ai::host::http_server::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;
    crate::ai::host::ws_server::add_to_linker::<ChoirHostState, ChoirHostState>(
        &mut linker,
        |state| state,
    )?;

    Ok(linker)
}

pub async fn load_plugins(
    config: &Config,
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    tx: mpsc::Sender<Event>,
) -> anyhow::Result<(PluginRegistry, Vec<BackgroundPluginHandle>)> {
    let plugins = Arc::new(RwLock::new(HashMap::<String, PluginInstance>::new()));
    let mut background_handles = Vec::new();

    for plugin in config.plugins.iter() {
        let (subscriptions, lifecycle, store) =
            load_and_init_plugin(engine, linker, plugin, tx.clone()).await?;
        let store = Arc::new(Mutex::new(store));

        let background_handle = if plugin.allow_background {
            let (_, background_lifecycle, background_store) =
                load_and_init_plugin(engine, linker, plugin, tx.clone()).await?;
            Some(run_plugin_in_background(
                plugin.name.clone(),
                background_lifecycle,
                Arc::new(Mutex::new(background_store)),
            ))
        } else {
            None
        };

        let plugin_instance = PluginInstance {
            config: plugin.clone(),
            topics: subscriptions,
            lifecycle: lifecycle.clone(),
            store: store.clone(),
        };

        if let Some(background_handle) = background_handle {
            background_handles.push(background_handle);
        }

        let mut lock = plugins.write().await;
        lock.insert(plugin.name.clone(), plugin_instance);
    }

    Ok((plugins, background_handles))
}

fn run_plugin_in_background(
    plugin_name: String,
    lifecycle: Guest,
    store: Arc<Mutex<Store<ChoirHostState>>>,
) -> BackgroundPluginHandle {
    let name_for_spawn = plugin_name.clone();
    let name_for_return = plugin_name.clone();
    let handle = tokio::spawn(async move {
        info!("[Хост] Запуск фонового процесса плагина {}...", name_for_spawn);

        let mut store_guard = store.lock().await;
        debug!("[Хост] Вызов run() для плагина {}", name_for_spawn);
        let run_res = store_guard
            .run_concurrent(async |accessor| lifecycle.call_run(accessor).await)
            .await;

        if let Err(err) = run_res {
            error!(
                "[Хост] Фоновый процесс плагина {} запустился с ошибкой: {:?}",
                name_for_spawn, err
            );
        } else {
            info!(
                "[Хост] Фоновый процесс плагина {} запустился успешно",
                name_for_spawn
            );
        }
    });

    BackgroundPluginHandle {
        plugin_name: name_for_return,
        handle,
    }
}

pub async fn load_and_init_plugin(
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    plugin_config: &PluginConfig,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<(Vec<String>, Guest, Store<ChoirHostState>)> {
    let mut store = new_plugin_store(engine, plugin_config, event_sender).await?;

    info!("[Хост] Загрузка файла: {:?}", plugin_config.file.clone());
    let component = Component::from_file(engine, plugin_config.file.clone())?;

    let plugin = HostPlugin::instantiate_async(&mut store, &component, linker).await?;
    let lifecycle = plugin.ai_host_plugin_lifecycle().clone();

    info!("[Хост] Вызов метода init...");

    let config_str = plugin_config.config.to_string();
    info!("[Хост] Конфигурация плагина: {:?}", config_str);

    let subscriptions_res = store
        .run_concurrent(async |accessor| lifecycle.call_init(accessor, config_str).await)
        .await;
    let subscriptions = subscriptions_res.and_then(|res| res).unwrap_or_else(|err| {
        error!("[Хост] Ошибка при вызове метода init: {:?}", err);
        Vec::<String>::new()
    });

    info!(
        "[Хост] Плагин успешно загружен. Его подписки: {:?}",
        subscriptions
    );

    Ok((subscriptions, lifecycle, store))
}

pub async fn new_plugin_store(
    engine: &Engine,
    config: &PluginConfig,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<Store<ChoirHostState>> {
    let mut wasi_builder = WasiCtxBuilder::new();

    let host_state = ChoirHostState {
        wasi: wasi_builder.build(),
        table: Default::default(),
        event_sender,
        current_plugin_permissions: Some(config.access.clone()),
    };
    let store = Store::new(engine, host_state);

    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- can_plugin_print ----------------------------------------------------
    #[test]
    fn print_allowed_when_within_limit() {
        let perms = vec![PluginAccess::ConsolePrint(10)];
        assert!(can_plugin_print(&perms, "short"));
        assert!(can_plugin_print(&perms, "0123456789")); // ровно лимит
    }

    #[test]
    fn print_denied_when_over_limit() {
        let perms = vec![PluginAccess::ConsolePrint(5)];
        assert!(!can_plugin_print(&perms, "too long"));
    }

    #[test]
    fn print_denied_when_no_console_print_perm() {
        // Есть другие права, но не console_print
        let perms = vec![PluginAccess::ConsoleInput("prompt>".to_string())];
        assert!(!can_plugin_print(&perms, "any"));
    }

    #[test]
    fn print_denied_when_no_perms() {
        assert!(!can_plugin_print(&[], "any"));
    }

    // --- can_plugin_read_console -------------------------------------------
    #[test]
    fn read_allowed_when_prompt_matches() {
        let perms = vec![PluginAccess::ConsoleInput("prompt>".to_string())];
        assert!(can_plugin_read_console(&perms, "prompt>"));
    }

    #[test]
    fn read_denied_when_prompt_differs() {
        let perms = vec![PluginAccess::ConsoleInput("prompt>".to_string())];
        assert!(!can_plugin_read_console(&perms, "other>"));
    }

    #[test]
    fn read_denied_when_no_console_input_perm() {
        let perms = vec![PluginAccess::ConsolePrint(100)];
        assert!(!can_plugin_read_console(&perms, "prompt>"));
    }

    #[test]
    fn read_denied_when_no_perms() {
        assert!(!can_plugin_read_console(&[], "prompt>"));
    }

    // --- history / автосохранение / TTL -------------------------------------

    #[tokio::test]
    async fn history_append_marks_dirty_and_mtime() {
        let sid = "sess-ttl-1";
        let ev = Event {
            request_id: "r".into(),
            session_id: sid.into(),
            source: "front:console".into(),
            target: "agent:*".into(),
            topic: "request".into(),
            payload: "hi".into(),
        };
        history_append(sid, &ev, 100).await;

        // Сессия попала в «грязные».
        let dirty = take_dirty_sessions();
        assert!(dirty.contains(&sid.to_string()), "сессия должна быть грязной");

        // mtime записан.
        let mtime_set = session_mtimes().lock().unwrap().contains_key(sid);
        assert!(mtime_set);

        // dirty очистился после take.
        assert!(!take_dirty_sessions().contains(&sid.to_string()));
    }

    #[tokio::test]
    async fn save_session_to_disk_writes_file() {
        let sid = "sess-save-test";
        let ev = Event {
            request_id: "r".into(),
            session_id: sid.into(),
            source: "front:console".into(),
            target: "agent:*".into(),
            topic: "request".into(),
            payload: "hello".into(),
        };
        history_append(sid, &ev, 100).await;

        let dir = "./.test_andychour_sessions";
        let _ = std::fs::remove_dir_all(dir);
        save_session_to_disk(dir, sid).await;

        let path = format!("{}/{}.json", dir, sid);
        assert!(std::path::Path::new(&path).exists(), "файл сессии должен создаться");
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["session_id"], sid);

        let _ = std::fs::remove_dir_all(dir);
        history_clear(sid).await;
    }

    #[tokio::test]
    async fn load_cleans_stale_file_over_ttl() {
        // Создаём старый файл (mtime в прошлом) и свежий.
        let dir = "./.test_andychour_ttl";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();

        let stale = format!("{}/stale.json", dir);
        std::fs::write(&stale, r#"{"session_id":"stale","events":[]}"#).unwrap();
        // Откатываем mtime в прошлое (> 1с).
        let past = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        let filetime = filetime::FileTime::from_system_time(past);
        let _ = filetime::set_file_mtime(&stale, filetime);

        let fresh = format!("{}/fresh.json", dir);
        std::fs::write(&fresh, r#"{"session_id":"fresh","events":[]}"#).unwrap();

        // TTL = 60с: stale (1ч) удалится, fresh останется.
        load_histories_from_disk(dir, 60).await;

        assert!(!std::path::Path::new(&stale).exists(), "старый файл должен удалиться");
        assert!(std::path::Path::new(&fresh).exists(), "свежий файл должен остаться");

        let _ = std::fs::remove_dir_all(dir);
        history_clear("stale").await;
        history_clear("fresh").await;
    }
}
