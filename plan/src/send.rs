mod sql;

use std::{collections::HashSet, error::Error};
use std::collections::HashMap;

use common::{err::define as err_def, make_err_msg};
use conn::CommonValue;

use crate::utils::sql as utils_sql;

pub trait SendPlan : Send {
    fn do_send(&mut self, param : HashMap<String, Vec<CommonValue>>) -> Result<(), Box<dyn Error>>;
}

fn new_sql_send_plan(name : String, data : &'_ crate::template::SendPlanTemplate) -> Result<Box<dyn SendPlan>, Box<dyn std::error::Error>> { 
    let sql_data = data.sql.as_ref().unwrap();
    let conn_info = utils_sql::template_info_convert_conn_info(sql_data.connection.as_slice());

    if conn_info.len() <= 0 {
        return Err(err_def::system::NoDataError::new(
            make_err_msg!("no data : {}", name), None
        ));
    }

    let p = utils_sql::new_conn_pool(&sql_data.dbtype, name, conn_info)?;

    Ok(Box::new(sql::SqlSendPlan::new(if sql_data.dbtype == "postgres" {
        "postgres"
    } else {
        "scylla"
    }, sql_data.query.clone(), p)))
}

pub(crate) fn new_send_plan(name : String, data : &'_ crate::template::SendPlanTemplate) -> Result<Box<dyn SendPlan>, Box<dyn std::error::Error>> {
    if data.send_type != "sql" {
        return Err(err_def::system::ApiCallError::new(make_err_msg!("not support type : {}", data.send_type), None));
    }

    new_sql_send_plan(name, data)
}