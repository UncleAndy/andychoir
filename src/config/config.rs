use serde::Deserialize;
use crate::plugin::config::PluginConfig;

/// Main config for host
/// Include parameters for frontends, orchestrator, agents, tools

#[derive(Deserialize)]
pub struct Config {
    #[allow(dead_code)]
    pub orchestrator: PluginConfig,
    #[allow(dead_code)]
    pub models: Vec<PluginConfig>,
    #[allow(dead_code)]
    pub frontends: Vec<PluginConfig>,
    #[allow(dead_code)]
    pub agents: Vec<PluginConfig>,
    #[allow(dead_code)]
    pub tools: Vec<PluginConfig>,
}
