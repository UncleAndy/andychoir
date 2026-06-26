use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub enum PluginAccess {
    #[serde(rename = "console", untagged)]
    Console(String),
    #[serde(rename = "filesystem", untagged)]
    Filesystem(String, String, String), // path, dir_perms, file_perms ("ro", "rw")
    #[serde(rename = "network", untagged)]
    Network(Vec<(String, u16)>),
}

#[derive(Deserialize, Clone)]
#[allow(unused)]
pub struct PluginConfig {
    #[allow(unused)]
    pub file: String,
    #[allow(unused)]
    pub name: String,
    #[allow(unused)]
    pub class: String,
    #[allow(unused)]
    pub access: Vec<PluginAccess>,
    #[allow(unused)]
    #[serde(default)]
    pub allow_background: bool,
    #[allow(unused)]
    pub config: serde_json::Value, // Параметры инициализации плагина
}
