use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use common_rs::c_core::collection::pool::ThreadSafePool;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::duckdb::create_duckdb_conn_pool;
use common_rs::exec::interfaces::relational::{RelationalExecutorInfo, RelationalExecutorPool, RelationalValue};
use common_rs::exec::pg::create_pg_conn_pool;
use common_rs::exec::scylla::create_scylla_conn_pool;
use crate::constant;
use crate::loader::ConfLoader;
use crate::types::config::conn::ConnectionInfos;

use crate::interpreter::pool::{create_lua_interpreter_pool, InterpreterPool};

#[derive(Default)]
pub struct GlobalStore {
    exec_pool_map : HashMap<String, RelationalExecutorPool<RelationalValue>>,
    exec_interpreter_map : HashMap<&'static str, InterpreterPool>
}

impl GlobalStore {
    fn from_conf_loader(loader : &'_ dyn ConfLoader) -> Result<GlobalStore, CommonError> {
        let data : ConnectionInfos = loader.load_connection().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "global store load fail conn",e)
        })?;

        let mut exec_pool_map : HashMap<String, RelationalExecutorPool<RelationalValue>> = HashMap::new();
        for info in data.infos {
            let p = match info.conn_type.as_str() {
                constant::CONN_TYPE_PG => Ok(create_pg_conn_pool(info.conn_name.clone(), RelationalExecutorInfo {
                    addr: info.conn_db_addr,
                    name: info.conn_db_name,
                    user: info.conn_db_user,
                    password: info.conn_db_passwd,
                    timeout_sec: info.conn_db_timeout,
                }, info.conn_max_size)),
                constant::CONN_TYPE_SCYLLA => Ok(create_scylla_conn_pool(info.conn_name.clone(), vec![RelationalExecutorInfo {
                    addr: info.conn_db_addr,
                    name: info.conn_db_name,
                    user: info.conn_db_user,
                    password: info.conn_db_passwd,
                    timeout_sec: info.conn_db_timeout,
                }], info.conn_max_size)),
                constant::CONN_TYPE_DUCKDB => Ok(create_duckdb_conn_pool(info.conn_name.clone(), RelationalExecutorInfo {
                    addr: info.conn_db_addr,
                    name: info.conn_db_name,
                    user: info.conn_db_user,
                    password: info.conn_db_passwd,
                    timeout_sec: info.conn_db_timeout,
                }, info.conn_max_size)),
                _          => Err(CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support {}", info.conn_type)))
            }?;
            exec_pool_map.insert(info.conn_name.clone(), p);
        }
        
        let mut script_interpreter = HashMap::new();
        script_interpreter.insert("lua", create_lua_interpreter_pool(100));
        Ok(GlobalStore { exec_pool_map,  exec_interpreter_map : script_interpreter })
    }
}
pub struct GlobalLayout {
    store : Arc<RwLock<GlobalStore>>,
    once : AtomicBool
}

impl GlobalLayout {
    pub fn get_exec_pool<S : AsRef<str>>(&self, name : S) -> Result< RelationalExecutorPool<RelationalValue>, CommonError > {
        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let opt = reader.exec_pool_map.get(&name.as_ref().to_string());
        if opt.is_none() {
            return CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists {}", name.as_ref())).to_result();
        }
        Ok(opt.unwrap().clone())
    }
    
    pub fn get_interpreter_pool<S : AsRef<str>>(&self, name : S) -> Result<InterpreterPool, CommonError> {
        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let opt = reader.exec_interpreter_map.get(name.as_ref());
        if opt.is_none() {
            return CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists {}", name.as_ref())).to_result();
        }
        Ok(opt.unwrap().clone())
    }

    pub fn initialize(&self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        if self.once.swap(true, Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "already initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let store = GlobalStore::from_conf_loader(loader).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InitFailed, "global store load failed",e)
        })?;

        *writer = store;

        Ok(())
    }
}

pub static GLOBAL: LazyLock<GlobalLayout> = LazyLock::new(|| {
    GlobalLayout {
        store : Arc::new(RwLock::new(GlobalStore::default()) ),
        once : AtomicBool::new(false)
    }
});