/*
    TODO: Отказываемся от rig и пытаемся использовать модифицированный openai-api-rs для работы
     агента из wasm-плагинов
*/

use std::error::Error;
use std::path::PathBuf;

use clap::Parser;
use tokio::sync::mpsc;
use andychoir::host;

use andychoir::{error, info};
use andychoir::ai::host::types::Event;
use andychoir::config::Config as AppConfig;
use andychoir::messages::bus::{EventBusConfig, start_event_bus};
use andychoir::metrics::{Metrics, MetricsConfig, start_metrics_exporter};
use andychoir::plugin::engine::{create_engine, create_linker, load_plugins};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct AppArgs {
    #[arg(short, long, help = "Config file path.")]
    pub config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = AppArgs::parse();

    let config = AppConfig::new_from_file(args.config).await?;

    // Инициализация логов
    let log_res = host::log::init_log(
        &config.logger
    );
    if log_res.is_err() {
        return Err(Box::from(log_res.err().unwrap()));
    }

    let engine = create_engine(&config)?;
    let linker = create_linker(&engine)?;
    let metrics = Metrics::new();
    let metrics_console = metrics.clone();

    let _metrics_console_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            info!("{}", metrics_console.render_console());
        }
    });

    let _metrics_exporter_handle = start_metrics_exporter(
        metrics.clone(),
        MetricsConfig {
            enabled: config.metrics.enabled,
            host: config.metrics.host.clone(),
            port: config.metrics.port,
            update_interval_secs: config.metrics.update_interval_secs,
            location: config.metrics.location.clone(),
        },
    );

    info!("Модули рантайма Wasmtime успешно инициализированы.");

    let (tx, rx) = mpsc::channel::<Event>(config.event_queue_size);
    let (plugins, background_plugins) = load_plugins(&config, &engine, &linker, tx.clone()).await?;

    let expected_plugins = plugins.read().await.len();
    let all_plugin_names: Vec<String> = plugins.read().await.keys().cloned().collect();
    info!("[Хост] Загружено плагинов: {} (ожидаем готовность всех)", expected_plugins);

    let event_bus = start_event_bus(
        plugins,
        rx,
        tx.clone(),
        engine,
        linker,
        EventBusConfig {
            event_queue_size: config.event_queue_size,
            thread_pool_size: config.thread_pool_size,
            max_fuel_for_call: config.max_fuel_for_call,
            session_timeout: config.session_timeout,
            session_check_period: config.session_check_period,
            metrics,
            expected_plugins,
        },
    );

    // ============ Таймер готовности: ждём status:"ready" от ВСЕХ плагинов ============
    // Если за startup.timeout_secs не все плагины отчитались о готовности —
    // выходим из приложения с ошибкой и списком неготовых (fail-fast).
    {
        let readiness = event_bus.readiness.clone();
        let all_names = all_plugin_names.clone();
        let timeout = std::time::Duration::from_secs(config.startup.timeout_secs);
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                if readiness.ready_sent() {
                    info!("[Хост] Готовность всех плагинов подтверждена.");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            // Таймаут истёк, а не все готовы.
            let ready = readiness.ready_sources().await;
            let not_ready: Vec<&String> = all_names
                .iter()
                .filter(|n| !ready.contains(n))
                .collect();
            error!(
                "[Хост] СТАРТОВЫЙ ТАЙМАУТ ({}с) истёк, не все плагины инициализировались. \
                 Неготовые плагины: {:?}. Выход.",
                timeout.as_secs(),
                not_ready
            );
            // Даём время профлашить stderr/лог перед выходом.
            std::thread::sleep(std::time::Duration::from_millis(200));
            std::process::exit(1);
        });
    }

    println!("Хост запущен. Нажмите Ctrl+C или Ctrl-D для выхода.");
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        () = host::console::wait_for_interrupt() => (),
    }
    info!("[Хост] Завершение работы...");

    drop(tx);
    for background_plugin in background_plugins {
        background_plugin.shutdown().await;
    }
    event_bus.shutdown().await;

    info!("[Хост] Выход из процесса.");
    std::process::exit(0);
}
