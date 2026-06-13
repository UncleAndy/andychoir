use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub path: String, // Path, URL or ID of config
}
