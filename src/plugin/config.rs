use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub enum PluginAccess {
    #[serde(rename = "console_input")]
    ConsoleInput(String), // Prompt text allowed
    #[serde(rename = "console_print")]
    ConsolePrint(u32), // One string max line size allowed
    #[serde(rename = "filesystem")]
    Filesystem(String, String, String), // path, dir_perms, file_perms ("ro", "rw")
    #[serde(rename = "network")]
    Network(Vec<(String, u16)>),
    // Белый список путей, которые плагин может читать через host-control.read-file.
    // Каждый элемент — путь к файлу или префикс каталога (все файлы под ним).
    #[serde(rename = "read_file")]
    ReadFile(Vec<String>),
}

#[derive(Deserialize, Clone)]
#[allow(unused)]
pub struct PluginConfig {
    #[allow(unused)]
    pub file: String,
    #[allow(unused)]
    pub name: String,
    #[allow(unused)]
    pub access: Vec<PluginAccess>,
    #[allow(unused)]
    #[serde(default)]
    pub allow_background: bool,
    #[allow(unused)]
    pub config: serde_json::Value, // Параметры инициализации плагина (внутренние параметры плагина в нужном ему формате
}
