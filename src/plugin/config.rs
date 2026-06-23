use serde::Deserialize;

#[derive(Deserialize)]
pub enum PluginClass {
    #[serde(rename = "frontend")]
    Frontend,
    #[serde(rename = "orchestrator")]
    Orchestrator,
    #[serde(rename = "agent")]
    Agent,
    #[serde(rename = "tool")]
    Tool,
    #[serde(rename = "internal")]
    Internal,
}

#[derive(Deserialize)]
#[allow(unused)]
pub struct PluginConfig {
    #[allow(unused)]
    file: String,
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    class: PluginClass,
    #[allow(unused)]
    config: serde_json::Value, // Параметры инициализации плагина
}
