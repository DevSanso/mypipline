mod sql;


use std::error::Error;
use std::collections::HashMap;

use conn::CommonValue;

pub trait SendPlan {
    fn do_send(&mut self, param : HashMap<String, Vec<CommonValue>>) -> Result<(), Box<dyn Error>>;
}