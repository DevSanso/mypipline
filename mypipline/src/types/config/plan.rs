use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanChain {
    pub conn_name : String,
    pub cmd : String,
    pub args : Vec<String>
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanInterval {
    pub connection : String,
    pub second     : u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanScript  {
    pub lang : String,
    pub file : String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub interval : PlanInterval,
    #[serde(alias = "type")]
    pub type_name : String,
    
    pub script    : Option<PlanScript>,
    pub chain     : Option<Vec<PlanChain>>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRoot {
    pub plan : HashMap<String, Plan>
}