use crate::error::{AppError, Result};
use std::{
    env, fs,
    path::{self, Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConf {
    pub thread: usize,
    pub chunk: usize,
    pub download_dir: PathBuf,
}

impl Default for AppConf {
    fn default() -> Self {
        let home = env::var("HOME").unwrap_or_else(|_| "/".to_string());
        let def_down_dir = PathBuf::from(home).join("Downloads");
        let def_down_dir = if def_down_dir.exists() {
            def_down_dir
        } else {
            PathBuf::from("./Downloads")
        };

        Self {
            download_dir: def_down_dir,
            chunk: 32,
            thread: 8,
        }
    }
}

impl AppConf {
    pub fn save(&self) -> Result<()> {
        let config_path = get_config_path();
        if let Some(p) = config_path.parent() {
            fs::create_dir_all(p)?;
        }

        let content = toml::to_string_pretty(self)?;
        fs::write(config_path, content)?;
        Ok(())
    }

    pub fn load() -> Self {
        let config_path = get_config_path();

        if config_path.exists() {
            if let Ok(content) = fs::read_to_string(&config_path)
                && let Ok(config) = toml::from_str::<AppConf>(&content)
            {
                return config;
            }
            eprintln!("config content invalided\nload default config");
        }
        let default_conf = AppConf::default();
        let _ = default_conf.save();
        default_conf
    }
}
pub fn get_config_path() -> PathBuf {
    if let Some(mut path) = env::home_dir() {
        path.push(".config");
        path.push("hoptix");
        path.push("config.toml");
        path
    } else {
        PathBuf::from("config.toml")
    }
}
