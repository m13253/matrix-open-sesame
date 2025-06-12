use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use eyre::Result;
use matrix_sdk::ruma::OwnedRoomId;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub log_room: Option<OwnedRoomId>,

    #[serde(default)]
    pub passphrases: Passbook,
}

pub type Passbook = HashMap<String, OwnedRoomId>;

impl Config {
    pub async fn new(path: &Path) -> Result<Arc<Config>> {
        let config_str = tokio::fs::read_to_string(path).await?;
        let config = toml::from_str(&config_str)?;
        Ok(Arc::new(config))
    }
}
