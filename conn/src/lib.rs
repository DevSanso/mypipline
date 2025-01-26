use std::error::Error;
use std::collections::HashMap;
use std::sync::Arc;

use common::collection::pool::ThreadSafePool;

#[derive(Clone, Debug)]
pub enum CommonValue {
    Double(f64),
    Int(i32),
    BigInt(i64),
    String(String),
    Binrary(Vec<u8>),
    Bool(bool),
    Null
}

#[derive(Default,Clone)]
pub struct CommonSqlExecuteResultSet {
    pub cols_name : Vec<String>,
    pub cols_data : Vec<Vec<CommonValue>>
}
pub trait CommonSqlConnection {
    //fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>>;
    fn execute(&mut self, query : &'_ str, param : &'_ [CommonValue]) -> Result<CommonSqlExecuteResultSet, Box<dyn Error>>;
}

#[derive(Debug,Clone)]
pub struct CommonSqlConnectionInfo {
    pub addr : String,
    pub db_name : String,
    pub user : String,
    pub password : String,
    pub timeout_sec : u32
}

pub type CommonSqlConnectionPool = Arc<dyn ThreadSafePool<Box<dyn CommonSqlConnection>,()>>;

pub struct CommonHttpResponseResult {
    pub header : HashMap<String,String>,
    pub body : Vec<u8>
}

pub trait CommonHttpConnection {
    fn do_request(&mut self, add_header : HashMap<String,CommonValue>, body : &'_ str) -> Result<CommonHttpResponseResult, Box<dyn Error>>;
}
