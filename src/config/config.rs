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
    /// Конфигурация MCP-транспорта хоста (allowlist бинарей для stdio-подпроцессов).
    /// `#[serde(default)]` — пустой список = разрешать любой бинарь (compat, warning).
    #[serde(default)]
    pub mcp: McpHostConfig,
}

/// Конфигурация MCP-транспорта хоста (см. `src/host/mcp_transport.rs`, B3).
/// Защита от того, что плагин `mcp:client` (возможно недоверенный, в mesh)
/// заставит хост спавнить произвольный процесс (эскейп песочницы).
#[derive(Deserialize, Clone, Default)]
#[serde(default)]
pub struct McpHostConfig {
    /// Белый список разрешённых команд/бинарей для `stdio_open`.
    /// Пустой = не ограничивать (обратная совместимость; warning в логе).
    /// Непустой = fail-closed: команда вне списка отвергается (`stdio_open` → "-").
    /// Сравнение: точное совпадение ИЛИ basename(command) ∈ список.
    pub allowed_binaries: Vec<String>,
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
    /// Опциональный токен аутентификации (B7). Если задан, экспортер требует
    /// `Authorization: Bearer <token>`; иначе отвечает `401`. По умолчанию — нет
    /// (но bind по умолчанию 127.0.0.1, так что внешне недоступен).
    #[serde(default)]
    pub token: Option<String>,
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
            token: None,
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
/// Настройки mTLS для межнодового соединения (внутренний приватный CA системы).
/// Сертификаты лежат на диске; при первом старте (если файлов нет) CA и
/// сертификат ноды генерируются автоматически (см. host/net/tls.rs).
#[derive(Deserialize, Clone, Default)]
#[serde(default)]
pub struct MtlsConfig {
    /// Включить mTLS. false/None → plain ws (обратная совместимость).
    pub enabled: bool,
    /// Каталог для хранения сгенерированных/заданных TLS-файлов (CA, cert, key).
    /// При генерации файлы пишутся сюда: `<dir>/ca.pem`, `<dir>/node.pem`,
    /// `<dir>/node.key`. Можно задать готовые файлы по явным путям ниже
    /// (тогда dir игнорируется для загрузки, но используется для генерации).
    pub dir: String,
    /// Путь к внутреннему CA (PEM, root cert), которому доверяем.
    /// Пусто → `<dir>/ca.pem`.
    pub ca_cert: String,
    /// Путь к сертификату ЭТОЙ ноды (PEM, выпущен внутренним CA).
    /// Пусто → `<dir>/node.pem`.
    pub cert: String,
    /// Путь к приватному ключу ЭТОЙ ноды (PEM).
    /// Пусто → `<dir>/node.key`.
    pub key: String,
    /// Строго требовать, чтобы SAN/subject сертификата партнёра == его node_id.
    /// false → достаточно «сертификат от нашего CA».
    pub require_node_id_in_san: bool,
}

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
    /// Интервал между попытками (пере)подключения к remote (секунды).
    /// 0 → использовать значение по умолчанию (10с).
    pub connect_retry_interval_secs: u64,
    /// Окно попыток подключения (секунды). После его истечения remote
    /// считается недоступным и задача завершается. 0 → бесконечные попытки.
    pub connect_timeout_secs: u64,
    /// Настройки mTLS (внутренний CA системы). Отключён по умолчанию.
    pub mtls: MtlsConfig,
}

/// Значение по умолчанию для интервала retry (если 0 в конфиге).
pub(crate) const DEFAULT_CONNECT_RETRY_INTERVAL_SECS: u64 = 10;
/// Значение по умолчанию для окна подключения (если 0 в конфиге). 5 минут.
pub(crate) const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 300;

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
    /// Интервал retry для этого remote (секунды). 0 → из NetConfig.connect_retry_interval_secs.
    pub retry_interval_secs: u64,
    /// Окно подключения для этого remote (секунды). 0 → из NetConfig.connect_timeout_secs.
    pub connect_timeout_secs: u64,
    /// Настройки mTLS для этого remote (переопределяют NetConfig.mtls, если заданы).
    /// Пусто/disabled → берётся NetConfig.mtls.
    pub mtls: MtlsConfig,
}

impl NetRemote {
    /// Извлечь hostname из URL (напр. "wss://host-b:8092/net" → "host-b").
    /// Для mesh договоримся: hostname == node_id удалённого узла.
    pub fn url_host(&self) -> Option<String> {
        // Схема://host[:port]/path
        let after = self.url.split("://").nth(1)?;
        let authority = after.split(['/', '?']).next().unwrap_or(after);
        let host = authority.split(':').next().unwrap_or(authority);
        if host.is_empty() {
            None
        } else {
            Some(host.to_string())
        }
    }

    /// node_id удалённого узла по договору == hostname из URL.
    pub fn url_host_node_id(&self) -> String {
        self.url_host().unwrap_or_default()
    }
}

impl Default for NetRemote {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: String::new(),
            targets: Vec::new(),
            retry_interval_secs: 0,
            connect_timeout_secs: 0,
            mtls: MtlsConfig::default(),
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
    /// Использует каталог `tmp/` в корне проекта (CARGO_MANIFEST_DIR), чтобы не
    /// засорять системный /tmp и корень проекта при запуске в nix-shell.
    async fn load_temp(ext: &str, body: &str) -> Config {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp");
        let _ = std::fs::create_dir_all(&dir);
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

    // P0: node_id с окружающими пробелами (невалидный UUID) → генерируется UUID.
    #[tokio::test]
    async fn p0_whitespace_node_id_generates_uuid() {
        let cfg = load_temp("yaml", "logger: {}\nplugins: []\nnet:\n  node_id: \" host-a \"\n").await;
        assert!(uuid::Uuid::parse_str(&cfg.net.node_id).is_ok(), "node_id с пробелами → UUID, получено: {}", cfg.net.node_id);
    }

    // Ring: 3 кольцевых конфига (A→B→C→A) парсятся корректно и образуют кольцо.
    // A — фронт-терминал, B — агент, C — калькулятор. Каждый dial-ит следующего.
    #[tokio::test]
    async fn ring_configs_form_closed_loop() {
        let a = Config::new_from_file(PathBuf::from("test_ring_a.yaml")).await.unwrap();
        let b = Config::new_from_file(PathBuf::from("test_ring_b.yaml")).await.unwrap();
        let c = Config::new_from_file(PathBuf::from("test_ring_c.yaml")).await.unwrap();
        // node_id корректны.
        assert_eq!(a.net.node_id, "00000000-0000-0000-0000-0000000000a1");
        assert_eq!(b.net.node_id, "00000000-0000-0000-0000-0000000000b2");
        assert_eq!(c.net.node_id, "00000000-0000-0000-0000-0000000000c3");
        // Каждый слушает свой порт.
        assert_eq!(a.net.listen_port, 8092);
        assert_eq!(b.net.listen_port, 8093);
        assert_eq!(c.net.listen_port, 8094);
        // Кольцо: A dials B:8093, B dials C:8094, C dials A:8092.
        assert_eq!(a.net.remotes.len(), 1);
        assert_eq!(a.net.remotes[0].url, "ws://127.0.0.1:8093/net");
        assert_eq!(b.net.remotes[0].url, "ws://127.0.0.1:8094/net");
        assert_eq!(c.net.remotes[0].url, "ws://127.0.0.1:8092/net");
        // Замыкание кольца: A→B→C→A.
        let next = |cfg: &Config| cfg.net.remotes[0].url.clone();
        let a_next = next(&a);
        let b_next = next(&b);
        let c_next = next(&c);
        // B — это a_next, C — это b_next, A — это c_next.
        assert!(a_next.contains(&b.net.listen_port.to_string()));
        assert!(b_next.contains(&c.net.listen_port.to_string()));
        assert!(c_next.contains(&a.net.listen_port.to_string()));
    }
}
