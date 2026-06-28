use crate::PLUGIN_NAME;

pub async fn run() {
    debug!("[WASM] Запуск фонового цикла плагина {}", PLUGIN_NAME);
    // Чтение пользовательского ввода из консоли.
    // Получаем нативный InputStream из подсистемы WASI, которую сгенерировал wit-bindgen.
    // В зависимости от вашей версии wit-bindgen путь может быть:
    // wasi::cli::stdin::get_stdin() ИЛИ вызов std::io::stdin() напрямую,
    // так как стандартная библиотека Rust под target_arch="wasm32-wasip2"
    // автоматически мапит std::io::stdin() на этот интерфейс!
    loop {
        // В контексте WASI Component Model этот вызов блокирует только текущую "микропрограмму" (fiber),
        // оставляя планировщик хоста свободным для вызовов handle_event.
        todo!()
    }
}
