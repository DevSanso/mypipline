use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub conn_type : String,
    pub conn_name : String,
    pub conn_max_size : usize,

    pub conn_db_type : String,
    pub conn_db_name : String,
    pub conn_db_user : String,
    pub conn_db_addr : String,
    pub conn_db_passwd : String,
    pub conn_db_timeout : u32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfos {
    pub infos : Vec<ConnectionInfo>
}