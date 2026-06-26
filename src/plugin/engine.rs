use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock, mpsc};
use wasmtime::component::{Component, HasData, Linker, ResourceTable};
use wasmtime::{Config as WasmtimeConfig, Engine, Store};
use wasmtime_wasi::sockets::SocketAddrUse;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::HostPlugin;
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

pub struct ChoirHostState {
    wasi: WasiCtx,
    table: ResourceTable,
    event_sender: mpsc::Sender<Event>,
}

impl crate::ai::host::event_bus::Host for ChoirHostState {
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

pub fn create_engine(config: &Config) -> anyhow::Result<Engine> {
    let mut engine_config = WasmtimeConfig::new();

    // Устанавливаем максимальный размер памяти для любого инстанса.
    // Значение задается в байтах.
    // Например, 256 МБ = 256 * 1024 * 1024
    engine_config.memory_reservation_for_growth(config.max_plugin_memory);

    // Дополнительно: чтобы предотвратить бесконечные циклы (CPU DoS),
    // можно включить "топливо" (fuel), но это потребует вызова
    // store.set_fuel() при каждом запуске.
    engine_config.consume_fuel(true);
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

    Ok(linker)
}

pub async fn load_plugins(
    config: &Config,
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    tx: mpsc::Sender<Event>,
) -> anyhow::Result<PluginRegistry> {
    let plugins = Arc::new(RwLock::new(HashMap::<String, PluginInstance>::new()));

    for plugin in config.plugins.iter() {
        let (subscriptions, lifecycle, store) =
            load_and_init_plugin(engine, linker, plugin, tx.clone()).await?;

        let plugin_instance = PluginInstance {
            config: plugin.clone(),
            topics: subscriptions,
            lifecycle,
            store: Arc::new(Mutex::new(store)),
        };

        let mut lock = plugins.write().await;
        lock.insert(plugin.name.clone(), plugin_instance);
    }

    Ok(plugins)
}

pub async fn load_and_init_plugin(
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    plugin_config: &PluginConfig,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<(Vec<String>, Guest, Store<ChoirHostState>)> {
    let mut store = new_plugin_store(engine, plugin_config, event_sender).await?;

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

pub async fn new_plugin_store(
    engine: &Engine,
    plugin_config: &PluginConfig,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<Store<ChoirHostState>> {
    let mut wasi_builder = WasiCtxBuilder::new();

    configure_plugin_access(&mut wasi_builder, plugin_config)?;

    let host_state = ChoirHostState {
        wasi: wasi_builder.build(),
        table: Default::default(),
        event_sender,
    };
    let mut store = Store::new(engine, host_state);
    let _ = store.set_fuel(1_000_000_000);

    Ok(store)
}

fn configure_plugin_access(
    wasi_builder: &mut WasiCtxBuilder,
    plugin_config: &PluginConfig,
) -> anyhow::Result<()> {
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

    Ok(())
}
