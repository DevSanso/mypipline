use std::error::Error;

use common::err::{define as err_def, make_err_msg};
use conn_postgres::create_pg_conn_pool;
use conn_scylla::create_scylla_conn_pool;
use conn::{CommonSqlConnectionPool, CommonSqlConnectionInfo};

pub fn change_sql_to_num_bind_support_sql(sql: &str, param: &Vec<&'_ str>) -> String {
    let mut result = sql.to_string();
    let mut offset = 0;

    for (index, key) in param.iter().enumerate() {
        let placeholder = format!("#{{{}}}", key);
        while let Some(start) = result[offset..].find(&placeholder) {
            let real_start = offset + start;
            let end = real_start + placeholder.len();
            let replacement = format!("${}", index + 1);
            result.replace_range(real_start..end, &replacement);
            offset = real_start + replacement.len();
        }
        offset = 0;
    }

    result.replace("##", "#")
} 

pub fn change_sql_to_question_mark_bind_support_sql(sql: &str, param: &Vec<&'_ str>) -> String {
    let mut result = sql.to_string();

    for key in param.iter() {
        let placeholder = format!("#{{{}}}", key);
        while let Some(start) = result.find(&placeholder) {
            let end = start + placeholder.len();
            result.replace_range(start..end, "?");
        }
    }

    result.replace("##", "#")
}

pub fn new_conn_pool(db_type : &'_ str, plan_name : String, info : Vec<CommonSqlConnectionInfo>) -> Result<CommonSqlConnectionPool, Box<dyn Error>> {
    match db_type {
        "postgres" => Ok(create_pg_conn_pool(format!("{}:{}:{}", db_type, &plan_name, &info[0].addr), info[0].clone(), 2)),
        "scylla" => Ok(create_scylla_conn_pool(format!("{}:{}:{}", db_type, &plan_name, &info[0].addr), info, 2)),
        _ => Err(err_def::system::NoSupportError::new(make_err_msg!("not support {}", db_type)))
    }
}

pub fn template_info_convert_conn_info(template : &'_ [crate::template::SqlDbConnection]) -> Vec<conn::CommonSqlConnectionInfo> {
    template.iter().fold(Vec::new(), |mut acc, x| {
        let info = conn::CommonSqlConnectionInfo {
            addr : format!("{}:{}", x.connection_ip, x.connection_port.to_string()),
            db_name : x.connection_dbname.clone(),
            user : x.connection_user.clone(),
            password : x.connection_password.clone(),
            timeout_sec : 60
        };
        acc.push(info);

        acc
    })
}