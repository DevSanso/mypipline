use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanChainBindParam {
    pub idx : usize,
    pub row : usize,
    pub col : usize,
    pub chain_idx : usize
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanChainArgs {
    pub data  : String,
    pub idx   : usize
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanChain {
    pub conn_name : String,
    pub cmd : String,
    pub bind : Vec<PlanChainBindParam>,
    pub args : Vec<PlanChainArgs>
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PlanRoot {
    pub plan : HashMap<String, Plan>
}