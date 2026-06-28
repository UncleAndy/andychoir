use crate::plugin::config::PluginConfig;
use serde::Deserialize;
use std::error::Error;
use std::path::PathBuf;

/// Main config for host
/// Include parameters for frontends, orchestrator, agents, tools

#[derive(Deserialize)]
pub struct Config {
    pub plugins: Vec<PluginConfig>,

    pub logger: LoggerConfig,

    #[serde(default = "default_session_timeout")]
    pub session_timeout: u64,
    #[serde(default = "default_session_check_period")]
    pub session_check_period: u64,
    #[serde(default = "default_max_fuel_for_call")]
    pub max_fuel_for_call: u64,
    #[serde(default = "default_max_plugin_memory")]
    pub max_plugin_memory: u64,
    #[serde(default = "default_thread_pool_size")]
    pub thread_pool_size: usize,
    #[serde(default = "default_event_queue_size")]
    pub event_queue_size: usize,
    #[serde(default)]
    pub metrics: MetricsExportConfig,
}

#[derive(Deserialize)]
pub struct LoggerConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,         // Например, "info", "debug", "error"
    #[serde(default = "default_log_directory")]
    pub logs_directory: String,    // Путь к папке, например, "logs" или "var/log"
    #[serde(default = "default_log_file_base_name")]
    pub file_base_name: String,    // Имя файла, например, "app_server"
    #[serde(default = "default_log_max_file_size_bytes")]
    pub max_file_size_bytes: u64, // Размер файла для ротации, например, 10_000_000 (10MB)
    #[serde(default = "default_log_days_to_keep")]
    pub days_to_keep: usize,     // Сколько дней хранить старые логи, например, 7
}

fn default_log_level() -> String {
    "debug".to_string()
}
fn default_log_directory() -> String {
    "./logs".to_string()
}
fn default_log_file_base_name() -> String {
    "andychoir".to_string()
}
fn default_log_max_file_size_bytes() -> u64 {
    10_000_000
}
fn default_log_days_to_keep() -> usize {
    7
}


#[derive(Clone, Deserialize)]
pub struct MetricsExportConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_host")]
    pub host: String,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    #[serde(default = "default_metrics_update_interval_secs")]
    pub update_interval_secs: u64,
    #[serde(default = "default_metrics_location")]
    pub location: String,
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

impl Default for MetricsExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: default_metrics_host(),
            port: default_metrics_port(),
            update_interval_secs: default_metrics_update_interval_secs(),
            location: default_metrics_location(),
        }
    }
}

fn default_metrics_host() -> String {
    "127.0.0.1".to_string()
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_metrics_update_interval_secs() -> u64 {
    15
}

fn default_metrics_location() -> String {
    "/metrics".to_string()
}

impl Config {
    pub async fn new_from_file(path: PathBuf) -> Result<Config, Box<dyn Error>> {
        // Асинхронно читаем весь файл в буфер байт (Vec<u8>)
        let content = tokio::fs::read(path.clone()).await?;

        // Определяем расширение файла ("json", "toml", "yaml"/"yml")
        let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("json");

        // 2. Десериализуем из среза байт (это быстрая операция в памяти)
        let config: Config = match extension {
            "json" => { serde_json::from_slice(&content)? }
            "toml" => { toml::from_slice(&content)? }
            "yaml" | "yml" => { serde_yaml::from_slice(&content)? }
            _ => return Err("Unsupported config file format".into())
        };

        Ok(config)
    }
}
