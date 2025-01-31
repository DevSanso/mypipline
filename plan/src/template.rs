use serde;

#[derive(serde::Deserialize, Debug)]
pub struct FetchValue {
    #[serde( alias = "val_type")]
    pub val_type : String
}
#[derive(serde::Deserialize, Debug)]
pub struct SqlDbConnection {
    #[serde( alias = "ip")]
    pub connection_ip : String,
    #[serde( alias = "port")] 
    pub connection_port : i32,
    #[serde( alias = "user")]
    pub connection_user : String,
    #[serde( alias = "password")]
    pub connection_password : String,
    #[serde( alias = "dbname")]
    pub connection_dbname : String,
}
#[derive(serde::Deserialize, Debug)]
pub struct SqlCollectPlanTemplate {
    #[serde( alias = "db_type")]
    pub dbtype : String, 
    #[serde( alias = "connection")]
    pub connection : Vec<SqlDbConnection>,
    #[serde( alias = "query")]
    pub query : String,

    #[serde( alias = "fetch")]
    pub fetch : std::collections::HashMap<String, FetchValue>
}

#[derive(serde::Deserialize, Debug)]
pub struct SqlSendPlanTemplate {
    #[serde( alias = "db_type")]
    pub dbtype : String, 
    #[serde( alias = "connection")]
    pub connection : Vec<SqlDbConnection>,
    #[serde( alias = "query")]
    pub query : String
}

#[derive(serde::Deserialize, Debug)]
pub struct CollectPlanTemplate {
    #[serde( alias = "type")]
    pub collect_type : String,
    #[serde( alias = "interval")]
    pub interval : u64,
    #[serde( alias = "interval_is_system")]
    pub interval_is_system : bool,
    pub sql : Option<SqlCollectPlanTemplate>

}

#[derive(serde::Deserialize, Debug)]
pub struct SendPlanTemplate {
    #[serde( alias = "type")]
    pub send_type : String,
    pub sql : Option<SqlSendPlanTemplate>
}

#[derive(serde::Deserialize, Debug)]
pub struct PlanTemplate{
    #[serde(skip)]
    pub name : String,
    #[serde( alias = "collect")]
    pub collect : CollectPlanTemplate, 
    #[serde( alias = "send")]
    pub send : SendPlanTemplate
}
