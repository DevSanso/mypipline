use std::path::PathBuf;
use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppDbConnectionConfig {
    pub db_type : String,
    pub db_address : String,
    pub db_name : String,
    pub db_user : String,
    pub db_password : String
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(alias = "log_level")]
    pub log_level : String,
    #[serde(alias = "log_max_size_mb")]
    pub log_max_size_mb : u64,
    #[serde(alias = "log_type")]
    pub log_type : String,
    #[serde(alias = "script_lib")]
    pub script_lib : Option<String>,
    #[serde(alias = "db_config")]
    pub db_config : Option<AppDbConnectionConfig>
}
