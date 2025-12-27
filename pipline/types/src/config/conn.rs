use std::collections::HashMap;
use serde::{Serialize, Deserialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    #[serde(alias = "max_conn")]
    pub max_size : usize,

    #[serde(alias = "type")]
    pub conn_type : String,
    #[serde(alias = "conn_name")]
    pub conn_name : String,
    #[serde(alias = "user")]
    pub conn_user : String,
    #[serde(alias = "addr")]
    pub conn_addr : String,
    #[serde(alias = "password")]
    pub conn_passwd : String,
    #[serde(alias = "timeout")]
    pub conn_timeout : u32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfos {
    pub connection: HashMap<String, ConnectionInfo>
}