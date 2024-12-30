use std::error::Error;

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

#[derive(Default)]
pub struct CommonExecuteResultSet {
    pub cols_name : Vec<String>,
    pub cols_data : Vec<Vec<CommonValue>>
}
pub trait CommonConnection {
    fn execute(&mut self, query : &'_ str, param : Vec<CommonValue>) -> Result<CommonExecuteResultSet, Box<dyn Error>>;
}

#[derive(Debug,Clone)]
pub struct CommonConnectionInfo {
    pub addr : String,
    pub db_name : String,
    pub user : String,
    pub password : String,
    pub timeout_sec : u32
}