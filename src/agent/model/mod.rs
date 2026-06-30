use std::sync::{Arc, OnceLock};
use dashmap::DashMap;

/*
    Хранилище конфигов моделей по именам.
*/

pub struct ModelConfig {
    pub provider_name: String,
    pub api_key: Option<String>,
    pub custom_url: Option<String>,
    pub model_name: String,
}

static MODELS: OnceLock<DashMap<String, Arc<ModelConfig>>> = OnceLock::new();
pub fn get_model(name: String) -> Result<Arc<ModelConfig>, Box<dyn std::error::Error>> {
    let models = MODELS.get_or_init(|| DashMap::new());

    models
        .get(&name)
        .map(|model| model.value().clone())
        .ok_or_else(|| "Model not found".into())
}

pub fn register_model(name: String, model: ModelConfig) {
    let models = MODELS.get_or_init(|| DashMap::new());
    models.insert(name, Arc::new(model));
}
