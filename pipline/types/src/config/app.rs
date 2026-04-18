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
pub struct AppLogDbConnectionConfig {
    pub db_type : String,
    pub db_address : String,
    pub db_name : String,
    pub db_user : String,
    pub db_password : String,
    pub db_ttl : Option<u64>
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppLogConfig {
    #[serde(alias = "log_type")]
    pub log_type : String,
    #[serde(alias = "log_level")]
    pub log_level : String,
    #[serde(alias = "log_file_size_mb")]
    pub log_file_size_mb : Option<u64>,
    #[serde(alias = "log_db_config")]
    pub log_db_config: Option<AppLogDbConnectionConfig>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(alias = "log")]
    pub log_conf : AppLogConfig,
    #[serde(alias = "script_lib")]
    pub script_lib : Option<String>,
    #[serde(alias = "db_config")]
    pub db_config : Option<AppDbConnectionConfig>
}