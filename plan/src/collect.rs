mod sql;

use std::error::Error;
use std::collections::HashMap;

use conn::CommonValue;

pub trait CollectPlan {
    fn do_collect(&mut self) -> Result<HashMap<String, Vec<CommonValue>>, Box<dyn Error>>;
}