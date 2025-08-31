use std::error::Error;
use std::fs;
use std::{collections::HashMap, path::PathBuf};
use common_rs::err::create_error;
use common_rs::err::core::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub connection: HashMap<String, ConnectionConfig>,
    pub plan: HashMap<String, PlanConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ConnectionConfig {
    pub ip: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    
    #[serde(rename = "type")]
    pub db_type: String,
}

#[derive(Debug, Deserialize)]
pub struct PlanConfig {
    pub interval: u32,
    pub chain: Vec<ChainItem>,
}

#[derive(Debug, Deserialize)]
pub struct ChainItem {
    pub connection: String,
    pub query: String,
    pub auto_commit : bool
}

pub fn parse_toml(path : PathBuf) -> Result<Config, Box<dyn Error>> {
    let data = fs::read_to_string(path).map_err(|x| {
        create_error(COMMON_ERROR_CATEGORY, FILE_IO_ERROR, "can't read".to_string(), Some(Box::new(x)))
    })?;

    let cfg = toml::from_str(data.as_str()).map_err(|x| {
        create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "toml parsing failed".to_string(), Some(Box::new(x)))
    })?;

    Ok(cfg)
}