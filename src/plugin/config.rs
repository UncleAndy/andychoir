use serde::Deserialize;

#[derive(Deserialize)]
pub enum PluginAccess {
    #[serde(rename = "console")]
    Console(String),
    #[serde(rename = "filesystem")]
    Filesystem(String, String, String), // path, dir_perms, file_perms ("ro", "rw")
    #[serde(rename = "network")]
    Network(Vec<(String, u16)>),
}

#[derive(Deserialize)]
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
    pub config: serde_json::Value, // Параметры инициализации плагина
}
