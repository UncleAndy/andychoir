pub mod tools;
pub mod model;
pub mod contexts;

use std::sync::{OnceLock};
use dashmap::DashMap;
use rig::client::{CompletionClient};
use rig::message::Message;
use rig::completion::{Chat, Prompt};
use serde::{Deserialize, Serialize};

/*
    Данный внутренний модуль хореографа будет отвечать на запросы от плагинов, относящиеся
    к взаимодействию с LLM.

    В конфиге хореографа будут описываться параметры и короткие имена llm и обращение будет
    происходить по ним.

    Все данные для взаимодействия с агентом должны передаваться в сообщении: системный промпт,
    запрос пользователя, описания инструментов, динамический контекст, история и т.д.
*/

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentRequest {
    pub model: String, // Имя LLM для взаимодействия (общее идентифицирующее имя из конфига)
    pub system_prompt: String,
    pub tools: Vec<String>, // Список доступных инструментов по именам
    pub dynamic_context: String, // Имя БД для поиска контекста

    pub history: Vec<Message>,
    pub user_prompt: String,
}

/// Хранилище всех контекстов.
/// Ключ - session_id
static HISTORY_DB: OnceLock<DashMap<String, Vec<Message>>> = OnceLock::new();
fn history_db() -> &'static DashMap<String, Vec<Message>> {
    HISTORY_DB.get_or_init(|| DashMap::new())
}

pub struct AgentWorker;

impl AgentWorker {
    pub fn new() -> Self {
        AgentWorker {}
    }

    /// Метод запуска взаимодействия с агентом
    /// Должен возвращать сразу ответ
    pub async fn run(&self, session_id: String, request: AgentRequest) -> Result<Message, Box<dyn std::error::Error>> {
        let mut history = history_db().entry(session_id.clone()).or_insert_with(Vec::new);
        history.push(request.user_prompt.clone().into());

        let model_res = model::get_model(request.model.clone());
        let model = if let Ok(model) = model_res {
            model
        } else {
            return Err("Model not found".to_string().into());
        };

        let client = rig::providers::openrouter::Client::new(
            &model.api_key.clone().unwrap_or("".to_string())
        )?;

        let mut builder = client
            .agent(model.model_name.as_str())
            .preamble(&request.system_prompt);

        if !request.tools.is_empty() {
            // Инициализируем инструменты
            todo!();
        }

        if !request.dynamic_context.is_empty() {
            // Инициализируем динамический контекст по имени
            todo!();
        }

        // Создаем агента и запускаем запрос в него
        let agent = builder.build();

        let history_opt = history_db().get_mut(&session_id);
        let response = if let Some(mut history_ref) = history_opt {
            agent
                .chat(request.user_prompt.clone(), history_ref.value_mut())
                .await
        } else {
            agent
                .prompt(request.user_prompt.clone())
                .await
        };

        match response {
            Ok(response) => Ok(Message::assistant(response)),
            Err(e) => Err(Box::from(e)),
        }
    }


}
