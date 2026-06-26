wasmtime::component::bindgen!("host-plugin");

// TODO: Сделать инструменты: работа с консолью, работа с файлами, работа с сетью.

pub mod config;
pub mod host;
pub mod messages;
pub mod plugin;

use std::collections::HashMap;
use std::error::Error;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use wasmtime::component::{Component, HasData, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::config::Config as AppConfig;
use crate::plugin::config::{PluginAccess, PluginConfig};
use clap::Parser;
use wasmtime_wasi::sockets::SocketAddrUse;

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc, watch};

pub struct PluginInstance {
    pub config: PluginConfig,
    pub topics: Vec<String>,
    pub lifecycle: Guest,
    pub store: Arc<Mutex<Store<ChoirHostState>>>,
}

pub struct ChoirHostState {
    wasi: WasiCtx,
    table: ResourceTable,
    event_sender: mpsc::Sender<Event>,
}

impl ai::host::event_bus::Host for ChoirHostState {
    fn publish_event(&mut self, event: Event) -> () {
        if let Err(err) = self.event_sender.try_send(event) {
            println!(
                "[Хост] Очередь входящих событий переполнена или закрыта: {}",
                err
            );
        }
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

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct AppArgs {
    #[arg(short, long, help = "Config file path.")]
    pub config: PathBuf,
}

struct SessionSlot {
    store: Arc<Mutex<Store<ChoirHostState>>>,
    last_used: std::time::Instant,
}

struct EventJob {
    plugin_name: String,
    store_key: (String, String),
    lifecycle: Guest,
    event: Event,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = AppArgs::parse();

    let config = AppConfig::new_from_file(args.config).await?;

    let mut engine_config = Config::new();

    // Устанавливаем максимальный размер памяти для любого инстанса.
    // Значение задается в байтах.
    // Например, 256 МБ = 256 * 1024 * 1024
    engine_config.memory_reservation_for_growth(config.max_plugin_memory);

    // Дополнительно: чтобы предотвратить бесконечные циклы (CPU DoS),
    // можно включить "топливо" (fuel), но это потребует вызова
    // store.set_fuel() при каждом запуске.
    engine_config.consume_fuel(true);
    engine_config.concurrency_support(true);

    let engine = Engine::new(&engine_config)?;

    let mut linker = Linker::<ChoirHostState>::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    ai::host::event_bus::add_to_linker::<ChoirHostState, ChoirHostState>(&mut linker, |state| {
        state
    })?;

    println!("Модули рантайма Wasmtime успешно инициализированы.");

    let plugins = Arc::new(RwLock::new(HashMap::<String, PluginInstance>::new()));
    let (tx, mut rx) = mpsc::channel::<Event>(config.event_queue_size);

    for plugin in config.plugins.iter() {
        let (subscriptions, lifecycle, store) =
            load_and_init_plugin(&engine, &linker, plugin, tx.clone()).await?;

        let store = Arc::new(Mutex::new(store));
        let plugin_instance = PluginInstance {
            config: plugin.clone(),
            topics: subscriptions,
            lifecycle,
            store: store.clone(),
        };

        let mut lock = plugins.write().await;
        lock.insert(plugin.name.clone(), plugin_instance);
    }

    let dispatcher_plugins = Arc::new(plugins);

    // Хранилище пула Store. Ключ: (<плагин>, <сессия>)
    let stores = Arc::new(dashmap::DashMap::<(String, String), SessionSlot>::new());

    let worker_count = config.thread_pool_size.max(1);
    let (worker_tx, worker_rx) = mpsc::channel::<EventJob>(config.event_queue_size);
    let worker_rx = Arc::new(Mutex::new(worker_rx));
    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let worker_rx = worker_rx.clone();
        let stores_worker = stores.clone();
        let max_fuel_for_call = config.max_fuel_for_call;
        worker_handles.push(tokio::spawn(async move {
            loop {
                let job = {
                    let mut rx = worker_rx.lock().await;
                    rx.recv().await
                };

                let Some(job) = job else {
                    break;
                };

                if let Some(mut slot) = stores_worker.get_mut(&job.store_key) {
                    slot.last_used = std::time::Instant::now();
                    let store_arc = slot.store.clone();
                    drop(slot);

                    let mut store_guard = store_arc.lock().await;
                    let _ = store_guard.set_fuel(max_fuel_for_call);

                    let handle_event_res = store_guard
                        .run_concurrent(async |accessor| {
                            job.lifecycle.call_handle_event(accessor, job.event).await
                        })
                        .await;

                    if let Err(e) = handle_event_res {
                        println!(
                            "[Хост] Ошибка при выполнении плагина {}: {:?}",
                            job.plugin_name, e
                        );
                    }
                } else {
                    println!("[Хост] Сессия не найдена для ключа {:?}", job.store_key);
                }
            }

            println!("[Хост] Исполнитель событий {} завершён.", worker_id);
        }));
    }

    // Внутренний процесс очистки неактивных сессий
    let stores_cleanup = stores.clone();
    tokio::spawn(async move {
        let timeout = std::time::Duration::from_secs(config.session_timeout);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(config.session_check_period)).await;

            let now = std::time::Instant::now();
            // retain удаляет элементы, для которых предикат вернул false
            stores_cleanup.retain(|_, slot| now.duration_since(slot.last_used) < timeout);
        }
    });

    let dispatcher_plugins_loop = dispatcher_plugins.clone();
    let stores_loop = stores.clone();
    let engine_loop = engine.clone();
    let worker_tx_loop = worker_tx.clone();
    let tx_loop = tx.clone();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let dispatcher_handle = tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                event = rx.recv() => event,
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
            };

            let Some(event) = event else {
                break;
            };

            let targets = event.target.split_whitespace().collect::<Vec<_>>();
            for target_pattern in targets {
                let matched_plugins: Vec<String> = if target_pattern.contains('*') {
                    let pattern = target_pattern.replace("*", "");
                    let lock = dispatcher_plugins_loop.read().await;
                    lock.keys()
                        .filter(|name| name.starts_with(&pattern))
                        .cloned()
                        .collect()
                } else {
                    let lock = dispatcher_plugins_loop.read().await;
                    if lock.contains_key(target_pattern) {
                        vec![target_pattern.to_string()]
                    } else {
                        vec![]
                    }
                };

                for plugin_name in matched_plugins {
                    // Добавляем в plugin_name идентификатор сессии из сообщения
                    // Проверяем существование плагина для данной сессии
                    let current_store_key = {
                        let store_option =
                            stores_loop.get(&(plugin_name.clone(), event.session_id.clone()));

                        match store_option {
                            None => {
                                // Ищем плагин для создания нового store
                                let plugin_config_opt = {
                                    let lock = dispatcher_plugins_loop.read().await;
                                    lock.get(&plugin_name).map(|p| p.config.clone())
                                };

                                if let Some(config) = plugin_config_opt {
                                    let tx_clone = tx_loop.clone();
                                    let engine_ref = engine_loop.clone(); // Engine в Wasmtime реализует Arc внутри

                                    match new_plugin_store(&engine_ref, &config, tx_clone).await {
                                        Ok(store) => {
                                            let key =
                                                (plugin_name.clone(), event.session_id.clone());
                                            stores_loop.insert(
                                                key.clone(),
                                                SessionSlot {
                                                    store: Arc::new(Mutex::new(store)),
                                                    last_used: std::time::Instant::now(),
                                                },
                                            );
                                            key
                                        }
                                        Err(err) => {
                                            println!(
                                                "Error creating plugin store: {} (plugin: {}).",
                                                err, plugin_name
                                            );
                                            continue;
                                        }
                                    }
                                } else {
                                    println!("Cannot find plugin: {}", plugin_name);
                                    continue;
                                }
                            }
                            Some(_) => (plugin_name.clone(), event.session_id.clone()),
                        }
                    };

                    let plugin_res = {
                        let lock = dispatcher_plugins_loop.read().await;
                        lock.get(&plugin_name).map(|p| p.lifecycle.clone())
                    };

                    if let Some(lifecycle) = plugin_res {
                        let job = EventJob {
                            plugin_name: plugin_name.clone(),
                            store_key: current_store_key.clone(),
                            lifecycle,
                            event: event.clone(),
                        };

                        if let Err(err) = worker_tx_loop.try_send(job) {
                            println!(
                                "[Хост] Очередь пула исполнителей переполнена или закрыта, событие для плагина {} отклонено: {}",
                                plugin_name, err
                            );
                        }
                    } else {
                        println!("[Хост] Не найден lifecycle плагина {}", plugin_name);
                    }
                }
            }
        }

        println!("[Хост] Диспетчер событий завершён.");
    });

    println!("Хост запущен. Нажмите Ctrl+C для выхода.");
    tokio::signal::ctrl_c().await?;
    println!("Завершение работы...");

    let _ = shutdown_tx.send(true);
    drop(tx);
    drop(worker_tx);

    if let Err(err) = dispatcher_handle.await {
        println!("[Хост] Диспетчер событий завершился с ошибкой: {:?}", err);
    }

    for worker_handle in worker_handles {
        if let Err(err) = worker_handle.await {
            println!("[Хост] Исполнитель событий завершился с ошибкой: {:?}", err);
        }
    }

    Ok(())
}

async fn load_and_init_plugin(
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    plugin_config: &PluginConfig,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<(Vec<String>, Guest, Store<ChoirHostState>)> {
    let mut wasi_builder = WasiCtxBuilder::new();

    for access in plugin_config.access.iter() {
        match access {
            PluginAccess::Console(_) => {
                wasi_builder.inherit_stdin();
                wasi_builder.inherit_stdout();
                wasi_builder.inherit_stderr();
            }
            PluginAccess::Filesystem(path, dir_perms, files_perms) => {
                let dir_perms = match dir_perms.to_lowercase().as_str() {
                    "ro" => DirPerms::READ,
                    "rw" => DirPerms::MUTATE | DirPerms::READ,
                    _ => DirPerms::READ,
                };

                let files_perms = match files_perms.to_lowercase().as_str() {
                    "ro" => FilePerms::READ,
                    "rw" => FilePerms::WRITE | FilePerms::READ,
                    _ => FilePerms::READ,
                };

                wasi_builder.preopened_dir(
                    Path::new(path.as_str()),
                    "/mnt",
                    dir_perms,
                    files_perms,
                )?;
            }
            PluginAccess::Network(listens) => {
                wasi_builder
                    .allow_udp(true)
                    .allow_tcp(true)
                    .allow_ip_name_lookup(true);

                let allowed_list = listens.clone();

                wasi_builder.socket_addr_check(move |socket_addr, socket_ctx| {
                    let allowed_list = allowed_list.clone();
                    Box::pin(async move {
                        let good_proto = match socket_ctx {
                            SocketAddrUse::TcpBind => false,
                            SocketAddrUse::UdpBind => false,
                            _ => true,
                        };
                        if good_proto {
                            return true;
                        }

                        let port = socket_addr.port();
                        let ip = socket_addr.ip();

                        for (good_host, good_port) in allowed_list {
                            let good_ip = IpAddr::from_str(good_host.as_str()).ok();

                            if port == good_port
                                && (good_ip.map_or(false, |gi| ip == gi) || good_host == "0.0.0.0")
                            {
                                return true;
                            }
                        }
                        false
                    })
                });
            }
        }
    }

    let host_state = ChoirHostState {
        wasi: wasi_builder.build(),
        table: Default::default(),
        event_sender,
    };
    let mut store = Store::new(engine, host_state);
    let _ = store.set_fuel(1_000_000_000);

    println!("[Хост] Загрузка файла: {:?}", plugin_config.file.clone());
    let component = Component::from_file(engine, plugin_config.file.clone())?;

    let plugin = HostPlugin::instantiate_async(&mut store, &component, linker).await?;
    let lifecycle = plugin.ai_host_plugin_lifecycle().clone();

    println!("[Хост] Вызов метода init...");

    let config_str = plugin_config.config.to_string();
    println!("[Хост] Конфигурация плагина: {:?}", config_str);

    let subscriptions_res = store
        .run_concurrent(async |accessor| lifecycle.call_init(accessor, config_str).await)
        .await;
    let subscriptions = subscriptions_res.and_then(|res| res).unwrap_or_else(|err| {
        eprintln!("[Хост] Ошибка при вызове метода init: {:?}", err);
        Vec::<String>::new()
    });

    println!(
        "[Хост] Плагин успешно загружен. Его подписки: {:?}",
        subscriptions
    );

    Ok((subscriptions, lifecycle, store))
}

async fn new_plugin_store(
    engine: &Engine,
    plugin_config: &PluginConfig,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<Store<ChoirHostState>> {
    let mut wasi_builder = WasiCtxBuilder::new();

    for access in plugin_config.access.iter() {
        match access {
            PluginAccess::Console(_) => {
                wasi_builder.inherit_stdin();
                wasi_builder.inherit_stdout();
                wasi_builder.inherit_stderr();
            }
            PluginAccess::Filesystem(path, dir_perms, files_perms) => {
                let dir_perms = match dir_perms.to_lowercase().as_str() {
                    "ro" => DirPerms::READ,
                    "rw" => DirPerms::MUTATE | DirPerms::READ,
                    _ => DirPerms::READ,
                };

                let files_perms = match files_perms.to_lowercase().as_str() {
                    "ro" => FilePerms::READ,
                    "rw" => FilePerms::WRITE | FilePerms::READ,
                    _ => FilePerms::READ,
                };

                wasi_builder.preopened_dir(
                    Path::new(path.as_str()),
                    "/mnt",
                    dir_perms,
                    files_perms,
                )?;
            }
            PluginAccess::Network(listens) => {
                wasi_builder
                    .allow_udp(true)
                    .allow_tcp(true)
                    .allow_ip_name_lookup(true);

                let allowed_list = listens.clone();

                wasi_builder.socket_addr_check(move |socket_addr, socket_ctx| {
                    let allowed_list = allowed_list.clone();
                    Box::pin(async move {
                        let good_proto = match socket_ctx {
                            SocketAddrUse::TcpBind => false,
                            SocketAddrUse::UdpBind => false,
                            _ => true,
                        };
                        if good_proto {
                            return true;
                        }

                        let port = socket_addr.port();
                        let ip = socket_addr.ip();

                        for (good_host, good_port) in allowed_list {
                            let good_ip = IpAddr::from_str(good_host.as_str()).ok();

                            if port == good_port
                                && (good_ip.map_or(false, |gi| ip == gi) || good_host == "0.0.0.0")
                            {
                                return true;
                            }
                        }
                        false
                    })
                });
            }
        }
    }

    let host_state = ChoirHostState {
        wasi: wasi_builder.build(),
        table: Default::default(),
        event_sender,
    };
    let mut store = Store::new(engine, host_state);
    let _ = store.set_fuel(1_000_000_000);

    Ok(store)
}
