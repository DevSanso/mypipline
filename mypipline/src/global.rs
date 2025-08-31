use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{LazyLock, RwLock, Arc};
use std::error::Error;

use common_rs::core::collection::pool::PoolItem;
use common_rs::db::core::CommonSqlConnectionPool;
use common_rs::db::DatabaseType;
use common_rs::db::create_common_sql_pool;
use common_rs::db::core::CommonSqlConnectionInfo;
use common_rs::db::core::CommonSqlConnection;
use common_rs::err::create_error;
use common_rs::err::core::*;

use crate::config::*;
use crate::plan::Plan;

pub type GlobalPoolType<T> = LazyLock<RwLock<(HashMap<String,T>, AtomicBool)>>;

macro_rules! init_global_type {
    () => {
        RwLock::new((HashMap::new(),AtomicBool::new(false)))
    };
}

static GLOBAL_DB_POOL : GlobalPoolType<CommonSqlConnectionPool> = LazyLock::new(||{init_global_type!()});
static GLOBAL_PLAN : GlobalPoolType<Arc<Plan>> = LazyLock::new(||{init_global_type!()});

pub fn init_global_db_pool(list :HashMap<String, ConnectionConfig>) -> Result<(), Box<dyn Error>> {
    let mut global_pool_tup = GLOBAL_DB_POOL.write()?;

    if global_pool_tup.1.load(std::sync::atomic::Ordering::SeqCst) {
        return create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "already init pool".to_string(), None).as_error();
    }

    let global_pool = &mut global_pool_tup.0;

    for conn in list {
        let info = match conn.1.db_type.as_str(){
            "postgres" => Ok(DatabaseType::POSTGRES(CommonSqlConnectionInfo {
                addr : format!("{}:{}", conn.1.ip, conn.1.port),
                db_name : conn.1.database.clone(),
                user : conn.1.user.clone(),
                password : conn.1.password.clone(),
                timeout_sec : 60
            })),
            _ => create_error(COMMON_ERROR_CATEGORY, NO_SUPPORT_ERROR, format!(" dbtype :{}", conn.1.db_type), None).as_error()
        }?;

        let p = create_common_sql_pool(info, conn.0.clone(), 30);
        global_pool.insert(conn.0.clone(), p);
    }    

    global_pool_tup.1.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

pub fn init_global_plan(list : HashMap<String, PlanConfig>) -> Result<(), Box<dyn Error>> {
    let mut global_plan_tup = GLOBAL_PLAN.write()?;

    if global_plan_tup.1.load(std::sync::atomic::Ordering::SeqCst) {
        return create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "already init pool".to_string(), None).as_error();
    }

    let global_plan = &mut global_plan_tup.0;

    for p in list {
        let conn_plan = Plan::new(p.1);
        global_plan.insert(p.0, Arc::new(conn_plan));

    }
    
    Ok(())
}

pub fn get_db_conn_from_pool(name : &'_ str) -> Result<Box<dyn PoolItem<Box<(dyn CommonSqlConnection + 'static)>>>, Box<dyn Error>> {
    let g_pool = match GLOBAL_DB_POOL.read() {
        Ok(ok) => Ok(ok),
        Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, 
            "get rwlock read handle failed".to_string(),  Some(Box::new(e))).as_error()
    }?;

    if !g_pool.1.load(std::sync::atomic::Ordering::SeqCst) {
        return create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "not init pool".to_string(), None).as_error();
    }

    let p = g_pool.0.get(name);
    if p.is_none() {
        return create_error(COMMON_ERROR_CATEGORY, NO_DATA_ERROR, format!("not exsits pool:{}", name), None).as_error();
    }

    let p_some = p.unwrap();

    let conn_box = match p_some.get_owned(()) {
        Ok(ok) => Ok(ok),
        Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, 
            format!("get failed db connection {}", name),  Some(e)).as_error()
    }?;
    
    Ok(conn_box)
}

pub fn get_plan_keys() -> Result<Vec<String>, Box<dyn Error>>{

    let g_plan = match GLOBAL_PLAN.read() {
    Ok(ok) => Ok(ok),
    Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, 
        "get rwlock read handle failed".to_string(),  Some(Box::new(e))).as_error()
    }?;

    if !g_plan.1.load(std::sync::atomic::Ordering::SeqCst) {
        return create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "not init plan".to_string(), None).as_error();
    }

    let ks = g_plan.0.keys().clone();
    Ok(ks.into_iter().fold(Vec::new(), |mut acc,x| {
        acc.push(x.clone());

        acc
    }))
}

pub fn get_plan(key : &'_ str) -> Result<Arc<Plan>, Box<dyn Error>>{

    let g_plan = match GLOBAL_PLAN.read() {
    Ok(ok) => Ok(ok),
    Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, 
        "get rwlock read handle failed".to_string(),  Some(Box::new(e))).as_error()
    }?;

    if !g_plan.1.load(std::sync::atomic::Ordering::SeqCst) {
        return create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "not init plan".to_string(), None).as_error();
    }

    match g_plan.0.get(key) {
        Some(s) => Ok(Arc::clone(s)),
        None => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "not init plan".to_string(), None).as_error()
    }
}


