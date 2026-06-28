use crate::{InnerPluginConfig, CONFIG};

pub async fn init(config_json: String) -> Vec<String> {
    // 1. Парсим конфигурацию
    let parsed_config: InnerPluginConfig = serde_json::from_str(&config_json)
        .unwrap_or_else(|_| InnerPluginConfig {
            option_1: "".to_string(),
        });

    let topics_to_subscribe = vec![];

    // 2. Инициализируем конфиг
    {
        let mut config_lock = CONFIG.lock().unwrap();
        *config_lock = Some(parsed_config);
    }

    debug!(
        "[WASM] Плагин инициализирован. Запрошено подписок: {}",
        topics_to_subscribe.len()
    );

    // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
    topics_to_subscribe
}
