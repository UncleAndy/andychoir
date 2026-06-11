use std::sync::Arc;
use serde::Deserialize;
use serde_json;

/// Main config for host
/// Include parameters for frontends, orchestrator, agents, tools

pub type ConfigLoader = serde_json::Value;
pub type FrontendConfig = serde_json::Value;
pub type OrchestratorConfig = serde_json::Value;
pub type AgentConfig = serde_json::Value;
pub type ToolConfig = serde_json::Value;

#[derive(Deserialize)]
pub struct Config {
    #[allow(dead_code)]
    config_loader: ConfigLoader,
    #[allow(dead_code)]
    frontends: Vec<FrontendConfig>,
    #[allow(dead_code)]
    orchestrator: OrchestratorConfig,
    #[allow(dead_code)]
    agents: Vec<AgentConfig>,
    #[allow(dead_code)]
    tools: Vec<ToolConfig>,
}
