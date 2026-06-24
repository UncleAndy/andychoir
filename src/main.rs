wasmtime::component::bindgen!({
    world: "host-plugin",
    path: "./wit",
});

pub mod config;
pub mod host;
pub mod messages;
pub mod plugin;

use std::collections::HashMap;
use std::error::Error;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use wasmtime::{Config, Engine, Store};
use wasmtime::component::{Component, HasData, Linker, ResourceTable};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use clap::Parser;
use wasmtime_wasi::sockets::SocketAddrUse;
use crate::config::Config as AppConfig;
use crate::plugin::config::{PluginAccess, PluginConfig};

use ai::host::types::Event;
use exports::ai::host::plugin_lifecycle::Guest;

use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ChoirHostState {
    wasi: WasiCtx,
    table: ResourceTable,
    event_sender: mpsc::Sender<Event>,
}

impl ai::host::event_bus::Host for ChoirHostState {
    fn publish_event(&mut self, event: Event) -> () {
        let sender = self.event_sender.clone();
        tokio::spawn(async move {
            let _ = sender.send(event).await;
        });
    }
}

impl WasiView for ChoirHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table }
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

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = AppArgs::parse();

    let config = Config::new();
    let engine = Engine::new(&config)?;

    let mut linker = Linker::<ChoirHostState>::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    ai::host::event_bus::add_to_linker::<ChoirHostState, ChoirHostState>(&mut linker, |state| state)?;

    println!("Модули рантайма Wasmtime успешно инициализированы.");

    let config = AppConfig::new_from_file(args.config).await?;

    let mut plugins = HashMap::<String, PluginInstance>::new();
    let (tx, mut rx) = mpsc::channel::<Event>(100);

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

        plugins.insert(plugin.name.clone(), plugin_instance);
    }

    let dispatcher_plugins = Arc::new(plugins);
    
    let dispatcher_plugins_loop = dispatcher_plugins.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let targets = event.target.split_whitespace().collect::<Vec<_>>();
            for target_pattern in targets {
                let matched_plugins: Vec<String> = if target_pattern.contains('*') {
                    let pattern = target_pattern.replace("*", "");
                    dispatcher_plugins_loop.keys()
                        .filter(|name| name.starts_with(&pattern))
                        .cloned()
                        .collect()
                } else {
                    if dispatcher_plugins_loop.contains_key(target_pattern) {
                        vec![target_pattern.to_string()]
                    } else {
                        vec![]
                    }
                };

                for plugin_name in matched_plugins {
                    if let Some(plugin) = dispatcher_plugins_loop.get(&plugin_name) {
                        let lifecycle = plugin.lifecycle.clone();
                        let store = plugin.store.clone();
                        let ev = event.clone();
                        tokio::spawn(async move {
                            let mut store = store.lock().await;
                            let _ = lifecycle.call_handle_event(&mut *store, &ev);
                        });
                    }
                }
            }
        }
    });

    println!("Хост запущен. Нажмите Ctrl+C для выхода.");
    tokio::signal::ctrl_c().await?;
    println!("Завершение работы...");

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
                            return true
                        }

                        let port = socket_addr.port();
                        let ip = socket_addr.ip();

                        for (good_host, good_port) in allowed_list {
                            let good_ip = IpAddr::from_str(good_host.as_str()).ok();

                            if port == good_port && (good_ip.map_or(false, |gi| ip == gi) || good_host == "0.0.0.0") {
                                return true
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

    println!("[Хост] Загрузка файла: {:?}", plugin_config.file.clone());
    let component = Component::from_file(engine, plugin_config.file.clone())?;

    let plugin = HostPlugin::instantiate_async(&mut store, &component, linker).await?;
    let lifecycle = plugin.ai_host_plugin_lifecycle().clone();

    println!("[Хост] Вызов метода init...");
    let subscriptions = lifecycle.call_init(&mut store, plugin_config.config.as_str().unwrap())?;

    println!("[Хост] Плагин успешно загружен. Его подписки: {:?}", subscriptions);

    Ok((subscriptions, lifecycle, store))
}

pub struct PluginInstance {
    pub config: PluginConfig,
    pub topics: Vec<String>,
    pub lifecycle: Guest,
    pub store: Arc<Mutex<Store<ChoirHostState>>>,
}
