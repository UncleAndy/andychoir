// TODO: Сделать инструменты: работа с консолью, работа с файлами, работа с сетью.

use std::error::Error;
use std::path::PathBuf;

use andychoir::ai::host::types::Event;
use andychoir::config::Config as AppConfig;
use andychoir::messages::bus::{EventBusConfig, start_event_bus};
use andychoir::metrics::{Metrics, MetricsConfig, start_metrics_exporter};
use andychoir::plugin::engine::{create_engine, create_linker, load_plugins};
use clap::Parser;
use tokio::sync::mpsc;

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
    let engine = create_engine(&config)?;
    let linker = create_linker(&engine)?;
    let metrics = Metrics::new();
    let _metrics_console = metrics.clone();
    /*
    let _metrics_console_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            andychoir::host_println!("{}", metrics_console.render_console());
        }
    });
     */
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

    andychoir::println!("Модули рантайма Wasmtime успешно инициализированы.");

    let (tx, rx) = mpsc::channel::<Event>(config.event_queue_size);
    let (plugins, background_plugins) = load_plugins(&config, &engine, &linker, tx.clone()).await?;

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
        },
    );

    andychoir::println!("Хост запущен. Нажмите Ctrl+C для выхода.");
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        () = andychoir::host::console::wait_for_interrupt() => (),
    }
    andychoir::println!("Завершение работы...");

    drop(tx);
    for background_plugin in background_plugins {
        background_plugin.shutdown().await;
    }
    event_bus.shutdown().await;

    andychoir::println!("[Хост] Выход из процесса.");
    std::process::exit(0);
}
