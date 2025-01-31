mod sql;

use std::{collections::HashSet, error::Error};
use std::collections::HashMap;

use common::{err::define as err_def, make_err_msg};
use conn::CommonValue;

use crate::utils::sql as utils_sql;

pub trait CollectPlan : Send {
    fn do_collect(&mut self) -> Result<HashMap<String, Vec<CommonValue>>, Box<dyn Error>>;
    fn is_interval(&mut self) -> Result<bool, Box<dyn Error>>;
}

fn new_sql_collect_plan(name : String, data : &'_ crate::template::CollectPlanTemplate) -> Result<Box<dyn CollectPlan>, Box<dyn std::error::Error>> { 
    let sql_data = data.sql.as_ref().unwrap();
    let conn_info = utils_sql::template_info_convert_conn_info(sql_data.connection.as_slice());

    if conn_info.len() <= 0 {
        return Err(err_def::system::NoDataError::new(
            make_err_msg!("no data : {}", name)
        ));
    }

    let p = utils_sql::new_conn_pool(&sql_data.dbtype, name, conn_info)?;
    let set : HashSet<String> = sql_data.fetch.keys().fold(HashSet::new(), |mut acc, x| {
        acc.insert(x.clone());
        acc
    });

    Ok(Box::new(sql::SqlCollectPlan::new(if sql_data.dbtype == "postgres" {
        "postgres"
    } else {
        "scylla"
    }, sql_data.query.clone(), set, (data.interval, data.interval_is_system), p)))
}

pub(crate) fn new_collect_plan(name : String, data : &'_ crate::template::CollectPlanTemplate) -> Result<Box<dyn CollectPlan>, Box<dyn std::error::Error>> {
    if data.collect_type != "sql" {
        return Err(err_def::system::ApiCallError::new(make_err_msg!("not support type : {}", data.collect_type)));
    }

    new_sql_collect_plan(name, data)
}