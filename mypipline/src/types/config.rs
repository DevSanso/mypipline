use serde::{ Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub conn_type : String,
    pub conn_name : String,
    
    pub conn_db_type : String,
    pub conn_db_name : String,
    pub conn_db_user : String,
    pub conn_db_host : String,
    pub conn_db_port : i32,
    pub conn_db_passwd : String,
    pub conn_db_max_conn : usize
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfos {
    infos : Vec<ConnectionInfo>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub conn_name : String,
    pub cmd : String,

    pub args : Vec<String>
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRoot {
    pub plan_list : Vec<Plan>,
}