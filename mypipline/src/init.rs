use std::error::Error;
use std::collections::HashMap;
use std::sync::Arc;

use common_rs::init::LoggerConfig;
use common_rs::db::{self, DatabaseType, core::CommonSqlConnectionInfo, core::CommonSqlConnectionPool};
use common_rs::err::create_error;
use common_rs::err::core::*;

use crate::args;
use crate::config;
use crate::map::{DbConnPool, PlanPool};
use crate::plan::Plan;

fn precheck_support_db_type(cfgs : std::collections::hash_map::Values<'_, String, config::ConnectionConfig>) -> Result<(), Box<dyn Error>> {
    for val in cfgs {
        match val.db_type.as_str() {
            "postgres" => Ok(()),
            "scylla" => Ok(()),
            "duckdb" => Ok(()),
            _ => create_error(COMMON_ERROR_CATEGORY, NO_SUPPORT_ERROR, format!("not support {}", val.db_type), None).as_error()
        }?;
    }
    Ok(())
}

fn create_db_pool_callback(list :HashMap<String, config::ConnectionConfig>) -> HashMap<String, CommonSqlConnectionPool> {
    let mut ret = HashMap::new();

    for (key, value) in list {
        let pool_name = format!("{}:{}",value.db_type,value.database);

        let info = CommonSqlConnectionInfo { 
            addr: format!("{}:{}", value.ip, value.port), 
            db_name: value.database, 
            user: value.user, 
            password: value.password,
            timeout_sec: value.timeout 
        };

        let conn_info = match value.db_type.as_str() {
            "postgres" => Ok(DatabaseType::POSTGRES(info)),
            "scylla" => Ok(DatabaseType::SCYLLA(vec![info])),
            "duckdb" => Ok(DatabaseType::DUCKDB(info)),
            _ => create_error(COMMON_ERROR_CATEGORY, NO_SUPPORT_ERROR, format!("not support {}", value.db_type), None).as_error()
        };

        let conn = db::create_common_sql_pool(conn_info.unwrap(), pool_name, value.max_conn);
        ret.insert(key,conn);
    }
    ret
}

fn create_plan_pool_callback(list : HashMap<String, config::PlanConfig>) -> HashMap<String, Arc<Plan>> {
    let mut ret = HashMap::new();
    for (key, value) in list {
        ret.insert(key, Arc::new(Plan::new(value)) );
    }
    ret
}

pub(crate) fn get_args_and_config() -> Result<(args::Args, config::Config), Box<dyn Error>> {
    let proc_args = args::parsing();

    let cfg = config::parse_toml(proc_args.config.clone())?;
    Ok((proc_args, cfg))
}

pub(crate) fn init_common_lib(proc_args : &args::Args) -> Result<(), Box<dyn Error>> {
    let mut log_file : Option<&str> = None;

    if proc_args.log_file.is_some() {
        log_file = Some(proc_args.log_file.as_ref().unwrap().as_str());
    }

    let logger = LoggerConfig {
        log_level : proc_args.log_level.as_str(),
        log_file : log_file
    };

    common_rs::init::init_common(logger, None)?;

    Ok(())
}

pub(crate)fn create_process_maps(cfg_data : config::Config) -> Result<(DbConnPool, PlanPool), Box<dyn Error>> {
    precheck_support_db_type(cfg_data.connection.values().into_iter())?;

    let db_p = DbConnPool::init(cfg_data.connection, &create_db_pool_callback);
    let plan_p = PlanPool::init(cfg_data.plan, &create_plan_pool_callback);

    Ok((db_p, plan_p))
}