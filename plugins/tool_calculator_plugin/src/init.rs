use crate::PLUGIN_NAME;

pub async fn init(_config_json: String) -> Vec<String> {
    let topics_to_subscribe = vec![PLUGIN_NAME.to_string()];

    log_debug!(
        "[WASM] Плагин {} инициализирован. Запрошено подписок: {}",
        PLUGIN_NAME,
        topics_to_subscribe.len()
    );

    // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
    topics_to_subscribe
}
