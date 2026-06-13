use serde::Deserialize;
use crate::plugin::config::PluginConfig;

/// Main config for host
/// Include parameters for frontends, orchestrator, agents, tools

#[derive(Deserialize)]
pub struct Config {
    #[allow(dead_code)]
    pub models: Vec<ModelConfig>,
    #[allow(dead_code)]
    pub config_loader: PluginConfig,
    #[allow(dead_code)]
    pub frontends: Vec<PluginConfig>,
    #[allow(dead_code)]
    pub orchestrator: PluginConfig,
    #[allow(dead_code)]
    pub agents: Vec<PluginConfig>,
    #[allow(dead_code)]
    pub tools: Vec<PluginConfig>,
}

#[derive(Deserialize)]
pub struct ModelConfig {
    pub name: String, // Model config name (using for model selection from agent)
    pub provider: String,
    pub model_name: String,
    pub api_key: Option<String>,
    pub custom_url: Option<String>,
}
