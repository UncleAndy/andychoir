use std::error::Error;
use std::path::PathBuf;
use serde::Deserialize;
use crate::plugin::config::PluginConfig;

/// Main config for host
/// Include parameters for frontends, orchestrator, agents, tools

#[derive(Deserialize)]
pub struct Config {
    #[allow(dead_code)]
    pub plugins: Vec<PluginConfig>,
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
