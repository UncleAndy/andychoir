use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::{PLUGIN_CLASS, CONFIG, STATUS, PluginInitStatus};
use crate::init::init_tool;

pub async fn handle_event(ev: Event) {
    // Если это сообщения от инструментов - отправляем в процедуру инициализации
    if ev.source.starts_with("tool:") && ev.topic == "tool_definition" {
        init_tool(ev);
        return;
    }

    // Защита от работы неинициализированного плагина
    {
        let lock = STATUS.read().unwrap();
        if PluginInitStatus::Initialized != *lock  {
            log_error!("Plugin receive event, but is not initialized yet: 'agent:{}'", ev.target);
            return;
        }
    };

    // TODO: Здесь проверяем очередь входящих сообщений и если она не пуста - отправляем их в шину еще раз для обычной обработки

    log_debug!("[WASM] Получен ивент от хоста: {:?}", ev);

    // TODO - здесь будет обработка входящих событий

    log_debug!("{}: {}", ev.topic, ev.payload);

    let config = {
        let look = CONFIG.lock().unwrap();
        match look.clone() {
            Some(c) => c,
            None => {
                log_warn!("[WASM] Получено сообщение, но агент еще не инициализирован. Сохраняю сообщение в очередь для последующей обработки.");

                // TODO: сохраняем сообщение в очередь сообщений и обрабатываем их после инициализации плагина

                return;
            }
        }
    };

    // Имитация пинга
    if ev.topic == "request" {
        let host_event = Event {
            request_id: ev.request_id,
            session_id: ev.session_id,
            source: format!("{}:{}", PLUGIN_CLASS.to_string(), config.name),
            target: "*".to_string(),
            topic: "response".to_string(),
            payload: ev.payload,
        };
        publish_event(&host_event);
    }
}
