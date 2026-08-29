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

    // Ответ от инструмента на запрос-вызов (tool-calling): сохраняем результат
    // по request_id, чтобы цикл tool-calling смог его прочитать после
    // wait_for_response(request_id).
    if ev.source.starts_with("tool:") && ev.topic == "response" {
        crate::get_tool_results().insert(ev.request_id.clone(), ev.payload.clone());
        log_debug!(
            "[WASM] Сохранён результат инструмента {} для request_id={}: {:?}",
            ev.source,
            ev.request_id,
            ev.payload
        );
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

        let tools_json = build_tools_json(&ev, &config).await;
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
        let mut parsed: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                let err_ev = response_event(&config, &ev, &format!("LLM parse error: {}", e));
                publish_event(&err_ev);
                return;
            }
        };

        // Есть ли tool_calls?
        let mut tool_calls = parsed
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("tool_calls"))
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();

        // Цикл tool-calling: выполняем инструменты и переспрашиваем LLM, пока
        // он возвращает tool_calls (а не итоговый текст). Это позволяет модели
        // использовать НЕСКОЛЬКО инструментов подряд (напр. после «результат
        // слишком большой» — попробовать другой инструмент), а не обрываться
        // после первого tool_calls. Защитный лимит итераций — от бесконечного
        // цикла (когда модель бесконечно просит инструменты).
        const MAX_TOOL_ITERATIONS: usize = 10;
        let mut iterations = 0;

        loop {
            if tool_calls.is_empty() {
                break;
            }
            iterations += 1;
            if iterations > MAX_TOOL_ITERATIONS {
                log_warn!("[WASM] Превышен лимит итераций tool-calling ({}), прерываем", MAX_TOOL_ITERATIONS);
                let resp_ev = response_event(
                    &config,
                    &ev,
                    "(Превышено максимальное число шагов работы с инструментами.)",
                );
                publish_event(&resp_ev);
                return;
            }

            log_debug!("[WASM] LLM вернул {} tool_calls, вызываем инструменты", tool_calls.len());
            publish_status(&ev, &config, "Вызываю инструменты...");

            // Первый tool_calls принадлежит сообщению assistant — добавим его
            // в messages, затем для каждого вызова добавим role:"tool".
            let assistant_msg = parsed
                .get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .cloned()
                .unwrap_or(serde_json::json!({"role":"assistant","content":""}));
            messages.push(assistant_msg);

            for tc in &tool_calls {
                let func = match tc.get("function") {
                    Some(f) => f,
                    None => continue,
                };
                let name = func.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
                let arguments_raw = func.get("arguments").cloned().unwrap_or(serde_json::Value::Null);
                let tool_call_id = tc.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();

                if name.is_empty() {
                    continue;
                }

                // Парсим аргументы: OpenAI может вернуть arguments как
                // JSON-объект ИЛИ как JSON-строку. Нормализуем в строку для
                // payload события (инструмент сам разберёт свой формат).
                let args_payload = match &arguments_raw {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };

                // Обобщённый вызов инструмента: target = "tool:<name>".
                // request_id делаем уникальным для ЭТОГО вызова, чтобы
                // wait_for_response не конфликтовал с основным запросом.
                let call_request_id = format!("{}:tool:{}", ev.request_id, tool_call_id);
                let call_ev = Event {
                    request_id: call_request_id.clone(),
                    session_id: ev.session_id.clone(),
                    source: format!("{}:{}", PLUGIN_CLASS, config.name),
                    target: format!("tool:{}", name),
                    topic: "request".to_string(),
                    payload: args_payload,
                };
                publish_event(&call_ev);

                // Ждём ответ от инструмента с таймаутом (не зависаем).
                let got = crate::ai::host::host_control::wait_for_response_timeout(
                    call_request_id.clone(),
                    30000,
                ).await;

                let result = if got {
                    // Читаем payload ответа напрямую с хоста (не через
                    // TOOL_RESULTS, т.к. wasmtime сериализует handle_event:
                    // агент, ожидающий ответ, не обработает входящий response).
                    crate::ai::host::host_control::take_response_payload(call_request_id.clone())
                        .await
                        .unwrap_or_else(|| "(инструмент не вернул результат)".to_string())
                } else {
                    format!("(ошибка: таймаут ожидания ответа от инструмента {})", name)
                };

                log_debug!("[WASM] Результат {} для {}: {} байт", name, tool_call_id, result.len());

                // Лимит размера результата инструмента (в байтах), который
                // допускается передавать LLM. По умолчанию 100 КБ.
                const MAX_TOOL_RESULT_BYTES: usize = 100 * 1024;
                let result_content = if result.len() > MAX_TOOL_RESULT_BYTES {
                    // Результат превысил лимит — НЕ передаём его содержимое LLM
                    // (он раздувает messages и валит ollama), а вместо него
                    // отправляем уведомление о том, что результат слишком большой.
                    format!(
                        "(Результат выполнения инструмента слишком большой: {} байт, лимит {} байт. Содержимое не передано модели.)",
                        result.len(),
                        MAX_TOOL_RESULT_BYTES
                    )
                } else {
                    result.clone()
                };

                // Добавляем в messages пару: уже есть assistant с tool_calls,
                // теперь role:"tool" с результатом.
                messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": result_content
                }));
            }

            // Повторный запрос к LLM с результатами инструментов.
            publish_status(&ev, &config, "Обрабатываю результат инструментов...");
            let second_req = serde_json::json!({
                "model": config.model.model_name,
                "messages": messages,
                "tools": tools_json,
                "tool_choice": "auto"
            });
            let second_req_str = second_req.to_string();
            // Логируем тело повторного запроса (для диагностики). Усекаем.
            const MAX_LOG_CHARS: usize = 3000;
            if second_req_str.chars().count() > MAX_LOG_CHARS {
                let truncated: String = second_req_str.chars().take(MAX_LOG_CHARS).collect();
                log_debug!("[WASM] LLM повторный запрос (обрезан, {} символов): {}", second_req_str.chars().count(), truncated);
            } else {
                log_debug!("[WASM] LLM повторный запрос: {}", second_req_str);
            }
            let (s2, b2) = http::post_json(url.clone(), second_req_str).await;
            log_debug!("[WASM] LLM повторный ответ status={} body={}", s2, b2);

            if s2 != 200 {
                let resp_ev = response_event(
                    &config,
                    &ev,
                    &format!("Ошибка LLM при обработке результата инструмента: status {}", s2),
                );
                publish_event(&resp_ev);
                return;
            }

            // Разбираем повторный ответ: есть ли снова tool_calls.
            let parsed2: Option<serde_json::Value> = serde_json::from_str(&b2).ok();
            let new_tool_calls = parsed2
                .as_ref()
                .and_then(|v| v.get("choices"))
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("tool_calls"))
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default();

            if !new_tool_calls.is_empty() {
                // Модель снова просит инструменты — продолжаем цикл.
                tool_calls = new_tool_calls;
                parsed = parsed2.unwrap_or(parsed);
                continue;
            }

            // Итоговый текстовый ответ (нет tool_calls).
            let content = parsed2
                .as_ref()
                .and_then(|v| v.get("choices"))
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str())
                .unwrap_or("(пусто)")
                .to_string();
            let resp_ev = response_event(&config, &ev, &content);
            publish_event(&resp_ev);
            return;
        }

        // Первый ответ не содержал tool_calls — возвращаем content напрямую.
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

/// Построить JSON-описание инструментов для OpenAI-compatible API.
///
/// Инструменты берутся ДИНАМИЧЕСКИ у хоста для данной сессии
/// (get_session_tools: <сессия> + <локальные>, приоритет у сессии), затем
/// фильтруются белым списком агента (config.tools).
///
/// Фильтр (config.tools) — белый список:
///   - пуст -> НЕТ инструментов (агент ничего не может вызывать);
///   - содержит "*" (значение ИЛИ маска, напр. "tool:*", "*:calculator") ->
///     доступ ко всем / по маске;
///   - иначе — только перечисленные инструменты.
async fn build_tools_json(ev: &Event, config: &crate::AgentPluginConfig) -> serde_json::Value {
    let defs = crate::ai::host::host_control::get_session_tools(ev.session_id.clone()).await;

    // Фильтр белого списка агента.
    let whitelist = &config.tools;
    let allow_all = whitelist.iter().any(|t| t == "*");
    let filtered: Vec<_> = defs
        .into_iter()
        .filter(|d| {
            if allow_all {
                return true;
            }
            // match_allowlist: точное имя, или маска с '*'.
            whitelist.iter().any(|w| wildcard_match(w, &d.name))
        })
        .collect();

    let mut arr = Vec::new();
    for d in filtered {
        // parameters_json — JSON-схема параметров. Может быть "null" — тогда {}.
        let params = serde_json::from_str(&d.parameters_json)
            .unwrap_or(serde_json::json!({ "type": "object" }));
        arr.push(serde_json::json!({
            "type": "function",
            "function": {
                "name": d.name,
                "description": d.description,
                "parameters": params,
            }
        }));
    }
    serde_json::json!(arr)
}

/// Совпадение имени инструмента с шаблоном, где "*" = любая подстрока.
fn wildcard_match(pattern: &str, name: &str) -> bool {
    if pattern == name {
        return true;
    }
    if let Some(idx) = pattern.find('*') {
        let prefix = &pattern[..idx];
        let suffix = &pattern[idx + 1..];
        // "*..." (звёздка в начале) — префиксный матч, "...*" — суффиксный,
        // "a*b" — по обеим сторонам.
        name.starts_with(prefix) && name.ends_with(suffix)
    } else {
        false
    }
}

/// Сформировать событие-ответ и вернуть его.
///
/// target ставим = orig.source (конкретный фронт-источник запроса), а НЕ "*"
/// (broadcast). Так ответ HTTP-запроса уходит только во front:http и НЕ
/// дублируется в консольном фронтенде; ответ консольного запроса — только в
/// front:console. Консольный wait_for_response работает по request_id
/// (host-control.signal_response), поэтому на target он не завязан.
fn response_event(
    config: &crate::AgentPluginConfig,
    orig: &Event,
    content: &str,
) -> Event {
    Event {
        request_id: orig.request_id.clone(),
        session_id: orig.session_id.clone(),
        source: format!("{}:{}", PLUGIN_CLASS, config.name),
        target: orig.source.clone(),
        topic: "response".to_string(),
        payload: content.to_string(),
    }
}

/// Опубликовать событие прогресса (topic:"status") с тем же request_id,
/// чтобы фронт-источник мог показать текущий этап обработки запроса.
/// target = orig.source (как в response_event): прогресс HTTP-запроса не
/// должен видеть консольный фронтенд.
fn publish_status(orig: &Event, config: &crate::AgentPluginConfig, msg: &str) {
    publish_event(&Event {
        request_id: orig.request_id.clone(),
        session_id: orig.session_id.clone(),
        source: format!("{}:{}", PLUGIN_CLASS, config.name),
        target: orig.source.clone(),
        topic: "status".to_string(),
        payload: msg.to_string(),
    });
}
