wasmtime::component::bindgen!({world: "host-plugin", path: "./wit",});

pub mod config; // <--- Добавьте эту строку
pub mod host;   // Скорее всего, вам понадобятся и остальные модули
pub mod messages;
pub mod plugin;

use std::error::Error;
use std::future::ready;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use wasmtime::{Config, Engine, Store};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use clap::Parser;
use wasmtime_wasi::sockets::SocketAddrUse;
use crate::config::Config as AppConfig;
use crate::exports::ai::host::plugin_lifecycle::Guest;
use crate::plugin::config::{PluginAccess, PluginConfig};

// Стейт хоста, который привязывается к каждому плагину
struct ChoirHostState {
    wasi: WasiCtx,
    table: ResourceTable,
}

// Реализация обязательного трейта для работы WASI Preview 2
impl WasiView for ChoirHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct AppArgs {
    #[arg(short, long, help = "Config file path.")]
    pub config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    // Читаем аргументы командной строки
    let args = AppArgs::parse();

    // 1. Конфигурируем движок
    let config = Config::new();
    let engine = Engine::new(&config)?;

    // 2. Создаем линкер для компонентной модели
    let mut linker = Linker::<ChoirHostState>::new(&engine);

    // Подключаем стандартные системные функции WASI 0.2 к линкеру
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    // Теперь хост полностью готов загружать ваши .wasm файлы, собранные через Makefile!
    println!("Модули рантайма Wasmtime успешно инициализированы.");

    // Читаем главный конфиг
    let config = AppConfig::new_from_file(args.config).await?;

    // Инициализируем плагины
    for plugin in config.plugins.iter() {
        #[allow(unused)]
        let (subscriptions, lifecycle, plugin, store) =
            load_and_init_plugin(&engine, &linker, plugin).await?;
    }

    Ok(())
}

async fn load_and_init_plugin(
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    plugin_config: &PluginConfig,
) -> anyhow::Result<(Vec<String>, Guest, HostPlugin, Store<ChoirHostState>)> {

    // Шаг 1: Настраиваем индивидуальные права WASI для этого инстанса
    let mut wasi_builder = WasiCtxBuilder::new();

    // Цикл по всем имеющимся доступам плагина
    for access in plugin_config.access.iter() {
        match access {
            PluginAccess::Console(_) => {
                // Пробрасываем консоль, если это фронтенд-плагин
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
                    Box::pin(ready({
                        // socket_addr — это std::net::SocketAddr, который плагин пытается открыть.
                        // socket_ctx — контекст (например, SocketContextKind::TcpListen)

                        // Если это не TcpBind или UdpBind - разрешаем сразу (коннекты наружу разрешены)
                        let good_proto = match socket_ctx {
                            SocketAddrUse::TcpBind => false,
                            SocketAddrUse::UdpBind => false,
                            _ => true,
                        };
                        if good_proto {
                            return Box::pin(ready(true))
                        }

                        let port = socket_addr.port();
                        let ip = socket_addr.ip();

                        let mut result = false;
                        for (good_host, good_port) in allowed_list.clone() {
                            let good_ip = IpAddr::from_str(good_host.as_str()).ok();

                            if port == good_port && (ip.eq(good_ip.as_ref().unwrap()) || good_host.eq("0.0.0.0")) {
                                result = true;
                                break
                            }
                        }
                        result
                    }))
                });
            }
        }
    }

    // Создаем изолированное хранилище (Store) памяти для этого плагина
    let host_state = ChoirHostState {
        wasi: wasi_builder.build(),
        table: Default::default(),
    };
    let mut store = Store::new(engine, host_state);

    // Шаг 2: Считываем .wasm файл с диска и парсим его в компонент
    println!("[Хост] Загрузка файла: {:?}", plugin_config.file.clone());
    let component = Component::from_file(engine, plugin_config.file.clone())?;

    // Шаг 3: Линкуем (инстанцируем) компонент в нашей песочнице
    // Макрос bindgen сгенерировал структуру `HostPlugin`, соответствующую нашему миру
    let plugin = HostPlugin::instantiate_async(&mut store, &component, linker).await?;

    // Шаг 4: Получаем доступ к нашему стандартизированному интерфейсу методов
    // Имя метода в структуре полностью повторяет название интерфейса из WIT в camel_case
    let lifecycle = plugin.ai_host_plugin_lifecycle().clone();

    // Шаг 5: Вызываем метод `init` внутри WASM и забираем список подписок!
    println!("[Хост] Вызов метода init...");
    let subscriptions = lifecycle.call_init(&mut store, plugin_config.config.as_str().unwrap())?;

    println!("[Хост] Плагин успешно загружен. Его подписки: {:?}", subscriptions);

    // Возвращаем список топиков. (В реальном оркестраторе вы также сохраните
    // объект `plugin` и `store` в структуру супервизора плагина)
    Ok((subscriptions, lifecycle, plugin, store))
}
