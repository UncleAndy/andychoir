use crate::plugin::config::PluginConfig;
use serde::Deserialize;
use std::error::Error;
use std::path::PathBuf;

/// Main config for host
/// Include parameters for frontends, orchestrator, agents, tools

#[derive(Deserialize)]
pub struct Config {
    #[allow(dead_code)]
    pub plugins: Vec<PluginConfig>,

    #[allow(dead_code)]
    #[serde(default = "default_session_timeout")]
    pub session_timeout: u64,
    #[allow(dead_code)]
    #[serde(default = "default_session_check_period")]
    pub session_check_period: u64,
    #[allow(dead_code)]
    #[serde(default = "default_max_fuel_for_call")]
    pub max_fuel_for_call: u64,
    #[allow(dead_code)]
    #[serde(default = "default_max_plugin_memory")]
    pub max_plugin_memory: u64,
    #[allow(dead_code)]
    #[serde(default = "default_thread_pool_size")]
    pub thread_pool_size: usize,
    #[allow(dead_code)]
    #[serde(default = "default_event_queue_size")]
    pub event_queue_size: usize,
}

fn default_max_fuel_for_call() -> u64 {
    1_000_000
}

fn default_session_check_period() -> u64 {
    600
}

fn default_session_timeout() -> u64 {
    24 * 3600
}

fn default_max_plugin_memory() -> u64 {
    512 * 1024 * 1024
}

fn default_thread_pool_size() -> usize {
    8
}

fn default_event_queue_size() -> usize {
    1000
}

impl Config {
    pub async fn new_from_file(path: PathBuf) -> Result<Config, Box<dyn Error>> {
        // 1. Асинхронно читаем весь файл в буфер байт (Vec<u8>)
        let content = tokio::fs::read(path).await?;

        // 2. Десериализуем из среза байт (это быстрая операция в памяти)
        let config: Config = serde_json::from_slice(&content)?;

        Ok(config)
    }
}
