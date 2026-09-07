use std::path::PathBuf;
use anyhow::{Result};
use figment::Figment;
use figment::providers::{Toml, Env, Format, Serialized};
use serde::{Deserialize, Serialize};


#[derive(Deserialize, Serialize, Default, Clone, Debug)]
pub struct AppConfig {
    pub config_dir: String,
    pub data_dir: String,
}


impl AppConfig {

    pub fn load(config_dir: Option<PathBuf>, data_dir: Option<PathBuf>) -> Result<AppConfig> {

        let figment = Figment::new();

        let config_dir_fin = if let Some(config_dir) = config_dir {
            config_dir
        } else {
            dirs::config_dir().map(|mut p| { p.push("flowlite"); p }).unwrap_or_default()
        };

        let data_dir_fin = if let Some(data_dir) = data_dir {
            data_dir
        } else {
            dirs::data_dir().map(|mut p| { p.push("flowlite"); p }).unwrap_or_default()
        };

        let app_config: AppConfig = figment
            .merge(Toml::file(config_dir_fin.join("config.toml")))
            .merge(Env::prefixed("FLOWLITE_"))
            .merge(Serialized::default("config_dir", config_dir_fin.to_string_lossy()))
            .merge(Serialized::default("data_dir", data_dir_fin.to_string_lossy()))
            .extract()?;

        Ok(app_config)
    }

}
