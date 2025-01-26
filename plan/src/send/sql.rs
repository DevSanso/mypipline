use std::collections::{HashMap,HashSet};

use crate::*;
use conn::{CommonSqlConnectionPool, CommonValue};
use crate::send::SendPlan;
use crate::utils::sql::{change_sql_to_num_bind_support_sql, change_sql_to_question_mark_bind_support_sql};

pub struct SqlSendPlan {
    db_type : &'static str,
    query : String,
    db_pool : CommonSqlConnectionPool
}

impl SqlSendPlan {
    pub fn new(db_type : &'static str, query : String, p : CommonSqlConnectionPool) -> Self {
        SqlSendPlan { db_type: db_type, query: query, db_pool: p }
    }

    fn get_bind_parm_query(&self, param : &HashMap<String, Vec<CommonValue>>) -> String {
        let ks :Vec<&'_ str> = param.keys().fold(Vec::new(), |mut acc,x| {
            acc.push(x.as_str());
            acc
        });

        match self.db_type {
            "scylla" | "sqlite" => change_sql_to_question_mark_bind_support_sql(self.query.as_str(), &ks),
            _ => change_sql_to_num_bind_support_sql(self.query.as_str(), &ks),
        }
    }
}

impl Plan for SqlSendPlan {
    fn plan_type(&self) -> PlanType {
        PlanType::SQL(self.db_type)
    }
}

impl SendPlan for SqlSendPlan {
    fn do_send(&mut self, param : HashMap<String, Vec<CommonValue>>) -> Result<(), Box<dyn std::error::Error>> {
        let query = self.get_bind_parm_query(&param);

        let mut item = self.db_pool.get_owned(())?;
        let conn = item.get_value();

        let sql_param = param.values().fold(Vec::new(), |mut acc,x| {
            acc.push(x[0].clone());
            acc
        });
        conn.execute(query.as_str(), sql_param.as_slice())?;

        Ok(())
    }
}