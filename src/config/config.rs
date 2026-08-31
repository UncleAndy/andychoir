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

    #[serde(default)]
    pub net: NetConfig,

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
    #[serde(default)]
    pub startup: StartupConfig,
    #[serde(default)]
    pub history: HistoryConfig,
}

/// Параметры старта (готовность плагинов).
#[derive(Deserialize)]
pub struct StartupConfig {
    /// Сколько секунд ждать готовность ВСЕХ плагинов (их status:"ready").
    /// Если за это время не все отчитались — хост выходит с ошибкой,
    /// перечисляя неготовые плагины.
    #[serde(default = "default_startup_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for StartupConfig {
    fn default() -> Self {
        Self { timeout_secs: default_startup_timeout_secs() }
    }
}

fn default_startup_timeout_secs() -> u64 {
    15
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
    "info".to_string()
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

/// Настройки истории сессий (вариант A: хост хранит события сессий глобально).
#[derive(Deserialize)]
pub struct HistoryConfig {
    /// Максимум событий на одну сессию (FIFO-вытеснение старых).
    #[serde(default = "default_history_max_events")]
    pub max_events_per_session: usize,
    /// Каталог хранения историй (относительный путь, в корне проекта).
    #[serde(default = "default_history_dir")]
    pub dir: String,
    /// Файл текущей сессии в домашнем каталоге пользователя.
    #[serde(default = "default_current_session_file")]
    pub current_session_file: String,
    /// Срок жизни сессии (сек). Сессии, файлы которых старше этого возраста,
    /// удаляются при загрузке. 0 = отключить чистку.
    #[serde(default = "default_session_ttl_secs")]
    pub session_ttl_secs: u64,
    /// Период автосохранения изменённых сессий на диск (сек).
    #[serde(default = "default_save_period_secs")]
    pub save_period_secs: u64,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            max_events_per_session: default_history_max_events(),
            dir: default_history_dir(),
            current_session_file: default_current_session_file(),
            session_ttl_secs: default_session_ttl_secs(),
            save_period_secs: default_save_period_secs(),
        }
    }
}

fn default_history_max_events() -> usize {
    100
}
fn default_history_dir() -> String {
    "./.andychour/sessions".to_string()
}
fn default_current_session_file() -> String {
    "~/.andychour/current_session.json".to_string()
}
fn default_session_ttl_secs() -> u64 {
    604800 // 7 дней
}
fn default_save_period_secs() -> u64 {
    1
}

/// Настройки сетевого моста между экземплярами andychour.
#[derive(Deserialize, Default, Clone)]
#[serde(default)]
pub struct NetConfig {
    /// Уникальный идентификатор этого узла (для предотвращения циклов).
    pub node_id: String,
    /// Порт, на котором хост слушает входящие сетевые события (/net).
    pub listen_port: u16,
    /// Список секретов для аутентификации ВХОДЯЩИХ соединений (несколько
    /// токенов — для разных клиентов). В будущем — отдельный плагин хранения
    /// токенов.
    pub token: Vec<String>,
    /// Удалённые хосты (персистентные исходящие WS-соединения).
    pub remotes: Vec<NetRemote>,
}

/// Один удалённый хост, к которому мост держит персистентное соединение.
#[derive(Deserialize, Clone)]
#[serde(default)]
pub struct NetRemote {
    /// WS-URL удалённого хоста (напр. "ws://host-b:8092/net").
    pub url: String,
    /// Секрет для аутентификации на удалённом хосте.
    pub token: String,
    /// Какие target обслуживает удалённый хост (напр. ["tool:calculator"]).
    pub targets: Vec<String>,
}

impl Default for NetRemote {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: String::new(),
            targets: Vec::new(),
        }
    }
}

impl Config {
    pub async fn new_from_file(path: PathBuf) -> Result<Config, Box<dyn Error>> {
        // Асинхронно читаем весь файл в буфер байт (Vec<u8>)
        let content = tokio::fs::read(path.clone()).await?;

        // Определяем расширение файла ("json", "toml", "yaml"/"yml")
        let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("json");

        // 2. Десериализуем из среза байт (это быстрая операция в памяти)
        let mut config: Config = match extension {
            "json" => { serde_json::from_slice(&content)? }
            "toml" => { toml::from_slice(&content)? }
            "yaml" | "yml" => { serde_yaml::from_slice(&content)? }
            _ => return Err("Unsupported config file format".into())
        };

        // 3. Нормализация node_id (docs/NET-concept.md §5): идентификатор хоста
        //    должен быть UUID. Если задан пустой или невалидный UUID — генерируем
        //    UUID v4 (с предупреждением), чтобы не ломать будущий dedup/маршрутизацию.
        if config.net.node_id.is_empty() || uuid::Uuid::parse_str(&config.net.node_id).is_err() {
            log::warn!(
                "[Конфиг] node_id не задан или не является валидным UUID; сгенерирован UUID v4: {}",
                config.net.node_id
            );
            config.net.node_id = uuid::Uuid::new_v4().to_string();
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Записать конфиг во временный файл и загрузить через new_from_file.
    async fn load_temp(ext: &str, body: &str) -> Config {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("andychoir-test-{}.{}", uuid::Uuid::new_v4(), ext));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        drop(f);
        let cfg = Config::new_from_file(path.to_path_buf()).await.unwrap();
        let _ = std::fs::remove_file(&path);
        cfg
    }

    // P0: пустой node_id -> генерируется валидный UUID v4.
    #[tokio::test]
    async fn p0_empty_node_id_generates_uuid() {
        let cfg = load_temp("yaml", "logger: {}\nplugins: []\nnet:\n  node_id: \"\"\n").await;
        assert!(uuid::Uuid::parse_str(&cfg.net.node_id).is_ok(), "node_id должен быть UUID, получено: {}", cfg.net.node_id);
    }

    // P0: невалидный node_id (строка) -> генерируется UUID v4.
    #[tokio::test]
    async fn p0_invalid_node_id_generates_uuid() {
        let cfg = load_temp("yaml", "logger: {}\nplugins: []\nnet:\n  node_id: \"host-a\"\n").await;
        assert!(uuid::Uuid::parse_str(&cfg.net.node_id).is_ok(), "node_id должен быть UUID, получено: {}", cfg.net.node_id);
    }

    // P0: валидный UUID сохраняется без изменений.
    #[tokio::test]
    async fn p0_valid_uuid_preserved() {
        let valid = "00000000-0000-0000-0000-0000000000a1";
        let body = format!("logger: {{}}\nplugins: []\nnet:\n  node_id: \"{}\"\n", valid);
        let cfg = load_temp("yaml", &body).await;
        assert_eq!(cfg.net.node_id, valid);
    }

    // P0: node_id для всех поддерживаемых форматов (json/toml/yaml).
    #[tokio::test]
    async fn p0_uuid_across_formats() {
        for (ext, body) in [
            ("json", "{\"logger\":{},\"plugins\":[],\"net\":{\"node_id\":\"\"}}"),
            ("toml", "logger = {}\nplugins = []\n[net]\nnode_id = \"\"\n"),
            ("yaml", "logger: {}\nplugins: []\nnet:\n  node_id: \"\"\n"),
        ] {
            let cfg = load_temp(ext, body).await;
            assert!(uuid::Uuid::parse_str(&cfg.net.node_id).is_ok(), "{}: node_id должен быть UUID", ext);
        }
    }
}
