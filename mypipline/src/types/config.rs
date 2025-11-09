use serde::{ Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub conn_type : String,
    pub conn_name : String,
    pub conn_max_size : usize,
    
    pub conn_db_type : String,
    pub conn_db_name : String,
    pub conn_db_user : String,
    pub conn_db_host : String,
    pub conn_db_port : i32,
    pub conn_db_passwd : String,
    pub conn_db_timeout : u32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfos {
    pub infos : Vec<ConnectionInfo>
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanElement {
    pub conn_name : String,
    pub cmd : String,
    pub args : Vec<String>,
    pub timeout : u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub plan_name : String,
    pub root_conn : String,
    pub root_interval_sec : u64,
    pub elements : Vec<PlanElement>
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRoot {
    pub plan_list : Vec<Plan>
}