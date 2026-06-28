use flexi_logger::{FileSpec, Logger, Criterion, Naming, Cleanup, LoggerHandle, DeferredNow};
use std::path::PathBuf;
use log::Record;

use crate::config::config::LoggerConfig;

pub fn init_log(
    config: &LoggerConfig
) -> Result<LoggerHandle, flexi_logger::FlexiLoggerError> {
    // 1. Динамически собираем спецификацию файла из переменных
    let file_spec = FileSpec::default()
        .directory(PathBuf::from(config.logs_directory.clone()))
        .basename(config.file_base_name.clone());

    // 2. Инициализируем и настраиваем логгер
    Logger::try_with_str(config.log_level.clone())?
        .format(utc_ms_format)
        .log_to_file(file_spec)
        // Настраиваем ротацию через переданные переменные
        .rotate(
            Criterion::Size(config.max_file_size_bytes), // Лимит размера из переменной
            Naming::Numbers,                      // Формат суффикса (app.log, app.1.log)
            Cleanup::KeepLogFiles(config.days_to_keep),  // Время хранения из переменной
        )
        // Дублировать ли логи в консоль (stdout/stderr) параллельно с файлом
        .duplicate_to_stderr(flexi_logger::Duplicate::None)
        .start()
}

#[track_caller]
fn utc_ms_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    // Получаем время в UTC и форматируем: %.3f оставляет ровно 3 знака после запятой
    let utc_time = now.now().with_timezone(&chrono::Utc);

    let file = record.file().unwrap_or("unknown");
    let line = record.line().unwrap_or(0);

    write!(
        w,
        "[{}] {} [{}:{}] - {}",
        utc_time.format("%Y-%m-%d %H:%M:%S%.3f UTC"),
        record.level(),
        file,
        line,
        record.args()
    )
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)+) => { ::log::debug!($($arg)+); };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)+) => { ::log::info!($($arg)+) };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)+) => { ::log::warn!($($arg)+) };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)+) => { ::log::error!($($arg)+) };
}
