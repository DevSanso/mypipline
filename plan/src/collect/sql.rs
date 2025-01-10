use std::collections::{HashMap,HashSet};

use crate::*;
use conn::{CommonSqlConnectionPool, CommonValue};
use crate::collect::CollectPlan;

pub struct SqlCollectPlan<'a> {
    db_type : &'static str,
    query : &'a str,
    fetch_define : HashSet<&'a str>,
    db_pool : &'a CommonSqlConnectionPool
}

impl<'a> SqlCollectPlan<'a> {
    pub fn new(db_type : &'static str, query : &'a str, fetch_define : HashSet<&'a str>, p : &'a CommonSqlConnectionPool) -> Self {
        SqlCollectPlan {
            db_type,
            query,
            fetch_define,
            db_pool : p
        }
    }
}

impl<'a> Plan for SqlCollectPlan<'a> {
    fn plan_type(&self) -> PlanType {
        PlanType::SQL(self.db_type)
    }
}

impl<'a> CollectPlan for SqlCollectPlan<'a> {
    fn do_collect(&mut self) -> Result<HashMap<String, Vec<CommonValue>>, Box<dyn std::error::Error>> {
        let recv_data = {
            let mut conn_item = self.db_pool.get_owned(())?;
            let conn = conn_item.get_value();
            conn.execute(&self.query, &[])
        }?;
        
        let mut idx = 0;
        let mut ret = HashMap::new();
        
        for name in recv_data.cols_name.as_slice() {
            if self.fetch_define.get(name.as_str()).is_none() {
                continue;
            }

            let mut v = Vec::new();
            for val in recv_data.cols_data.as_slice() {
                v.push(val[idx].clone());
            }

            ret.insert(name.clone(), v);
            idx += 1;
        }

        Ok(ret)
    }
}