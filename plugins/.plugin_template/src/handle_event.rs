use crate::ai::host::log;
use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::{PLUGIN_NAME};

pub async fn handle_event(ev: Event) {
    // ХОСТ ВЫЗВАЛ ЭТОТ МЕТОД ПАРАЛЛЕЛЬНО
    // Данный метод выполняется асинхронно и независимо от того,
    // ждет ли сейчас функция read_line() ввода в консоли.

    log_debug!("[WASM] Получен ивент от хоста: {:?}", ev);

    // TODO - здесь будет обработка входящих событий

    log_debug!("{}: {}", ev.topic, ev.payload);

    // Имитация пинга
    if ev.topic == "request" {
        let host_event = Event {
            request_id: ev.request_id,
            session_id: ev.session_id,
            source: PLUGIN_NAME.to_string(),
            target: "*".to_string(),
            topic: "response".to_string(),
            payload: ev.payload,
        };
        publish_event(&host_event);
    }
}
