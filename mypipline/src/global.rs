use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{LazyLock, RwLock, Arc};
use std::error::Error;

use common_rs::core::collection::pool::PoolItem;
use common_rs::db::core::CommonSqlConnectionPool;
use common_rs::db::{self, DatabaseType};
use common_rs::db::create_common_sql_pool;
use common_rs::db::core::CommonSqlConnectionInfo;
use common_rs::db::core::CommonSqlConnection;
use common_rs::err::create_error;
use common_rs::err::core::*;

use crate::config::*;
use crate::plan::Plan;

pub type GlobalPoolType<T> = LazyLock<RwLock<HashMap<String,T>>>;

macro_rules! init_global_type {
    () => {
        RwLock::new(HashMap::new())
    };
}

static GLOBAL_DB_POOL : GlobalPoolType<CommonSqlConnectionPool> = LazyLock::new(||{init_global_type!()});
static GLOBAL_PLAN : GlobalPoolType<Arc<Plan>> = LazyLock::new(||init_global_type!());

pub fn init_global_db_pool(list :HashMap<String, ConnectionConfig>) -> Result<(), Box<dyn Error>> {
    let mut g_plan = GLOBAL_DB_POOL.write()?;
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
        }?;

        let conn = db::create_common_sql_pool(conn_info, pool_name, value.max_conn);

        g_plan.insert(key, conn);
    }
    Ok(())
}

pub fn init_global_plan(list : HashMap<String, PlanConfig>) -> Result<(), Box<dyn Error>> {
    let mut g_plan = GLOBAL_PLAN.write()?;
    for (key, value) in list {
        g_plan.insert(key, Arc::new(Plan::new(value)) );
    }
    Ok(())
}

pub fn get_db_conn_from_pool(name : &'_ str) -> Result<Box<dyn PoolItem<Box<(dyn CommonSqlConnection + 'static)>>>, Box<dyn Error>> {
    let g_plan = GLOBAL_DB_POOL.read()?;

    let p = g_plan.get(name);
    if p.is_none() {
        return create_error(COMMON_ERROR_CATEGORY, NO_DATA_ERROR, format!("not exists {}", name), None).as_error();
    }
    let conn = p.unwrap().get_owned(())?;
    Ok(conn)
}

pub fn get_plan_keys() -> Result<Vec<String>, Box<dyn Error>>{
    let g_plan = GLOBAL_PLAN.read()?;
    let v = g_plan.keys().fold(Vec::new(), |mut acc,x | {
        acc.push(x.clone());
        acc
    });

    Ok(v)
}

pub fn get_plan(key : &'_ str) -> Result<Arc<Plan>, Box<dyn Error>>{
    let g_plan = GLOBAL_PLAN.read()?;

    let p = g_plan.get(key);

    if p.is_none() {
        return create_error(COMMON_ERROR_CATEGORY, NO_DATA_ERROR, format!("{} not exsits", key), None).as_error();
    }

    Ok(Arc::clone(p.unwrap()))
}


