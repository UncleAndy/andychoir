use std::fmt;
use flexi_logger::{FileSpec, Logger, Criterion, Naming, Cleanup, LoggerHandle};
use std::path::PathBuf;
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
        // Указываем динамические настройки файла
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

pub fn debug(args: fmt::Arguments<'_>) {
    log::debug!("{}", args);
}
pub fn info(args: fmt::Arguments<'_>) {
    log::info!("{}", args);
}
pub fn warn(args: fmt::Arguments<'_>) {
    log::warn!("{}", args);
}
pub fn error(args: fmt::Arguments<'_>) {
    log::error!("{}", args);
}
