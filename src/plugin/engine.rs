use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock, mpsc};
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
}
