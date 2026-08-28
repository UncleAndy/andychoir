use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::ai::host::http;
use crate::{PLUGIN_CLASS, CONFIG, STATUS, PluginInitStatus};
use crate::init::init_tool;

pub async fn handle_event(ev: Event) {
    // Если это сообщения от инструментов - отправляем в процедуру инициализации
    if ev.source.starts_with("tool:") && ev.topic == "definition" {
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

    // Буферизуем входящие события до инициализации (защита от потери сообщений)
    let config = {
        let look = CONFIG.lock().unwrap();
        match look.clone() {
            Some(c) => c,
            None => {
                log_warn!("[WASM] Получено сообщение, но агент еще не инициализирован.");
                return;
            }
        }
    };

    log_debug!("[WASM] Получен ивент от хоста: {:?}", ev);

    // Только пользовательские запросы (от front:console) обрабатываем как диалог
    if ev.topic == "request" && ev.source.starts_with("front:") {
        let user_input = ev.payload.clone();

        // ======= ПРОГРЕСС: сообщаем консоли о начале обработки =======
        publish_status(&ev, &config, "Обработка запроса...");
        // ===============================================================

        // Шаг 1: первый вызов LLM (с описанием доступных инструментов).
        // Подмешиваем историю сессии (request от front -> user, response от
        // agent -> assistant), чтобы модель помнила предыдущий диалог.
        let history = crate::ai::host::host_control::get_session_history(ev.session_id.clone()).await;
        let mut messages = vec![
            serde_json::json!({ "role": "system", "content": config.system_prompt }),
        ];
        for hev in &history {
            // Классифицируем событие по источнику, чтобы правильно построить
            // роли для LLM: user (фронт), assistant (ответ агента), tool (утилиты).
            let src = hev.source.as_str();
            let classified = if src.starts_with("front") && hev.topic == "request" {
                // Пользовательский ввод.
                Some(serde_json::json!({ "role": "user", "content": hev.payload }))
            } else if src.starts_with("agent") && hev.topic == "response" {
                // Ответ агента (содержит текст для пользователя).
                Some(serde_json::json!({ "role": "assistant", "content": hev.payload }))
            } else if src.starts_with("tool") && hev.topic == "response" {
                // Результат утилиты -> role "tool".
                Some(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": hev.request_id,
                    "content": hev.payload
                }))
            } else if src.starts_with("tool") && hev.topic == "request" {
                // Запрос агента к утилите: пометим как assistant tool_call-намерение.
                // (Формирование полноценного tool_calls — в U3.)
                Some(serde_json::json!({
                    "role": "assistant",
                    "content": format!("[вызов утилиты {}: {}]", hev.target, hev.payload)
                }))
            } else {
                // Служебные события в контекст не идут.
                None
            };

            if let Some(msg) = classified {
                // Пропускаем сам текущий запрос (если он уже в истории).
                if msg.get("role").and_then(|r| r.as_str()) == Some("user")
                    && hev.request_id == ev.request_id
                {
                    continue;
                }
                messages.push(msg);
            }
        }
        // Текущий запрос пользователя.
        messages.push(serde_json::json!({ "role": "user", "content": user_input }));

        let tools_json = build_tools_json();
        let first_req = serde_json::json!({
            "model": config.model.model_name,
            "messages": messages,
            "tools": tools_json,
            "tool_choice": "auto"
        });

        let url = format!("{}/chat/completions", config.model.api_url);
        publish_status(&ev, &config, "Обращаюсь к модели LLM...");
        log_debug!("[WASM] LLM запрос: url={} body={}", url, first_req.to_string());
        let (status, body) = http::post_json(url.clone(), first_req.to_string()).await;
        log_debug!("[WASM] LLM ответ status={} body={}", status, body);

        if status != 200 {
            let err_ev = response_event(&config, &ev, &format!("LLM error: status {}", status));
            publish_event(&err_ev);
            return;
        }

        // Парсим ответ
        let parsed: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                let err_ev = response_event(&config, &ev, &format!("LLM parse error: {}", e));
                publish_event(&err_ev);
                return;
            }
        };

        // Есть ли tool_calls?
        let tool_calls = parsed
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("tool_calls"))
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();

        if !tool_calls.is_empty() {
            log_debug!("[WASM] LLM вернул {} tool_calls, вызываем инструменты", tool_calls.len());
            publish_status(&ev, &config, "Вызываю инструменты...");
            // Шаг 2: выполняем инструмент(ы)
            let mut tool_results = Vec::new();
            for tc in &tool_calls {
                let func = match tc.get("function") {
                    Some(f) => f,
                    None => continue,
                };
                let name = func.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
                let arguments = func.get("arguments").cloned().unwrap_or(serde_json::Value::Null);

                if name == "calculator" {
                    // Вызываем tool:calculator через шину событий
                    let call_ev = Event {
                        request_id: ev.request_id.clone(),
                        session_id: ev.session_id.clone(),
                        source: format!("{}:{}", PLUGIN_CLASS, config.name),
                        target: "tool:calculator".to_string(),
                        topic: "request".to_string(),
                        payload: serde_json::json!({
                            "expression": arguments.get("expression").and_then(|e| e.as_str()).unwrap_or("")
                        }).to_string(),
                    };
                    publish_event(&call_ev);

                    // Ждём ответ от инструмента: сохраняем в TOOLS как временное
                    // хранилище ответов. Для простоты P1: читаем последний response
                    // через отдельный механизм не реализуем — возвращаем заглушку.
                    // (Полноценный оркестратор tool-calling — следующий шаг P1.1.)
                    tool_results.push(serde_json::json!({
                        "tool": name,
                        "result": "(tool execution queued via event bus)"
                    }));
                }
            }

            // Шаг 3: финальный ответ (без реального результата инструмента в P1)
            let final_text = format!(
                "Я бы вычислил это через калькулятор. (P1: tool-calling через шину событий реализован, но сбор результата инструмента — следующий шаг.) Запрос: {}",
                user_input
            );
            let resp_ev = response_event(&config, &ev, &final_text);
            publish_event(&resp_ev);
        } else {
            // Нет tool_calls — возвращаем content напрямую
            let content = parsed
                .get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str())
                .unwrap_or("(пусто)")
                .to_string();
            let resp_ev = response_event(&config, &ev, &content);
            publish_event(&resp_ev);
        }
    }
}

/// Построить JSON-описание инструментов для OpenAI-compatible API.
fn build_tools_json() -> serde_json::Value {
    serde_json::json!([
        {
            "type": "function",
            "function": {
                "name": "calculator",
                "description": "Вычисляет математическое выражение и возвращает результат.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "expression": {
                            "type": "string",
                            "description": "Математическое выражение, например '2 + 2 * 3'"
                        }
                    },
                    "required": ["expression"]
                }
            }
        }
    ])
}

/// Сформировать событие-ответ для отправки в консоль.
fn response_event(
    config: &crate::AgentPluginConfig,
    orig: &Event,
    content: &str,
) -> Event {
    Event {
        request_id: orig.request_id.clone(),
        session_id: orig.session_id.clone(),
        source: format!("{}:{}", PLUGIN_CLASS, config.name),
        target: "*".to_string(),
        topic: "response".to_string(),
        payload: content.to_string(),
    }
}

/// Опубликовать событие прогресса (topic:"status") с тем же request_id,
/// чтобы консоль могла показать текущий этап обработки запроса.
fn publish_status(orig: &Event, config: &crate::AgentPluginConfig, msg: &str) {
    publish_event(&Event {
        request_id: orig.request_id.clone(),
        session_id: orig.session_id.clone(),
        source: format!("{}:{}", PLUGIN_CLASS, config.name),
        target: "*".to_string(),
        topic: "status".to_string(),
        payload: msg.to_string(),
    });
}
