use crate::{
    get_tools,
    AgentPluginConfig,
    ModelConfig,
    PluginInitStatus,
    ToolDefinition,
    CONFIG, PLUGIN_CLASS, STATUS};
use crate::ai::host::event_bus::publish_event;
use crate::ai::host::types::Event;

pub async fn init(config_json: String) -> Vec<String> {
    // 1. Парсим конфигурацию
    let config: AgentPluginConfig = serde_json::from_str(&config_json)
        .unwrap_or_else(|_| AgentPluginConfig {
            name: "".to_string(),
            model: ModelConfig {
                model_name: "".to_string(),
                api_url: "".to_string(),
                api_key: None,
            },
            system_prompt: "".to_string(),
            tools: vec![],
        });

    let mut topics_to_subscribe = vec![];

    let full_name = format!("{}:{}", config.name, PLUGIN_CLASS);
    let mask_name = format!("{}:*", config.name);

    // 2. Инициализируем конфиг
    {
        let mut config_lock = CONFIG.lock().unwrap();
        *config_lock = Some(config.clone());
    }

    // 3. Список подписок
    topics_to_subscribe.push(full_name);
    topics_to_subscribe.push(mask_name);

    log_debug!(
        "[WASM] init: config.tools = {:?}, system_prompt_len = {}",
        config.tools,
        config.system_prompt.len()
    );

    {
        let mut status_lock = STATUS.write().unwrap();
        // Если инструменты не заданы в конфиге — сразу готовы (не ждём
        // ответа от инструментов, иначе возникает замкнутый круг:
        // пустой config.tools → нет discovery → нет definition → не Initialized).
        if config.tools.is_empty() {
            *status_lock = PluginInitStatus::Initialized;
            log_info!("[WASM] Агент инициализирован без инструментов (config.tools пуст)");
            // Публикуем готовность сразу (нет инструментов — ждать нечего).
            publish_event(&Event {
                request_id: "-".to_string(),
                session_id: "-".to_string(),
                source: format!("{}:{}", PLUGIN_CLASS, config.name),
                target: "*".to_string(),
                topic: "status".to_string(),
                payload: "ready".to_string(),
            });
        } else {
            *status_lock = PluginInitStatus::NotInitializedYet;
        }
    }

    // Отправляем discovery запросы инструментам
    for tool in config.tools {
        let host_event = Event {
            request_id: "-".to_string(),
            session_id: "-".to_string(),
            source: format!("{}:{}", PLUGIN_CLASS.to_string(), config.name),
            target: format!("tool:{}", tool),
            topic: "discovery".to_string(),
            payload: "".to_string(),
        };
        publish_event(&host_event);
    }

    // Возвращаем вектор хосту. Фоновый цикл чтения консоли запускается хостом через run.
    topics_to_subscribe
}

pub fn init_tool(ev: Event) {
    // Обрабатываем ответ от инструмента
    log_debug!("[WASM] Получен ответ от инструмента: {:?}", ev);

    // Безопасный парсинг: payload может быть пустым/null/невалидным JSON
    // (например, инструмент ещё не проинициализирован). Не паникуем.
    let tool_definition: ToolDefinition = match serde_json::from_str(&ev.payload) {
        Ok(td) => td,
        Err(e) => {
            log_error!(
                "[WASM] Не удалось распарсить ToolDefinition от {}: {} (payload={:?})",
                ev.source,
                e,
                ev.payload
            );
            return;
        }
    };

    let tools = get_tools();
    tools.insert(tool_definition.name.clone(), tool_definition);

    // CONFIG может быть ещё не заполнен, если discovery-ответ пришёл
    // до завершения init() агента. Не паникуем — выходим, статус
    // обновится при следующем валидном ответе.
    let config = {
        let config_lock = CONFIG.lock().unwrap();
        match config_lock.clone() {
            Some(c) => c,
            None => {
                log_debug!("[WASM] CONFIG ещё не готов, пропускаем обновление статуса");
                return;
            }
        }
    };

    // Агент считается готовым, когда получил хотя бы одно определение
    // инструмента (независимо от config.tools.len(), который может быть
    // пустым из-за особенностей парсинга конфига).
    let tool_count = tools.len();
    log_debug!(
        "[WASM] init_tool: получено определений {}/{}, config.tools.len()={}",
        tool_count,
        config.tools.len(),
        config.tools.len()
    );
    if tool_count >= 1 {
        let mut status_lock = STATUS.write().unwrap();
        *status_lock = PluginInitStatus::Initialized;
        log_info!("[WASM] Агент инициализирован (инструментов: {})", tool_count);

        // Сообщаем хосту о готовности. Хост агрегирует готовность всех
        // плагинов и публикует глобальный host:"status"/"ready".
        let ready_ev = Event {
            request_id: ev.request_id.clone(),
            session_id: ev.session_id.clone(),
            source: format!("{}:{}", PLUGIN_CLASS, config.name),
            target: "*".to_string(),
            topic: "status".to_string(),
            payload: "ready".to_string(),
        };
        publish_event(&ready_ev);
    }
}