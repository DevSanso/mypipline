use std::collections::HashMap;
use std::sync::{LazyLock, OnceLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
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
use crate::types::config::plan::{Plan, PlanRoot};

#[derive(Default)]
pub struct GlobalStore {
    exec_pool_map : HashMap<String, RelationalExecutorPool<RelationalValue>>,
    exec_interpreter_map : HashMap<&'static str, InterpreterPool>,

    plans : PlanRoot
}

impl GlobalStore {
    fn from_conf_loader(loader : &'_ dyn ConfLoader) -> Result<GlobalStore, CommonError> {
        let mut store = GlobalStore { exec_pool_map: HashMap::new(),  exec_interpreter_map : HashMap::new(), plans : PlanRoot::default() };
        store.reset(loader)?;

        store.exec_interpreter_map.insert("lua", create_lua_interpreter_pool(100));
        Ok(store)
    }

    fn reset_db_pool(&mut self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        let data : ConnectionInfos = loader.load_connection().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "global store load fail conn",e)
        })?;
        for info in data.infos {
            if self.exec_pool_map.contains_key(&info.conn_name) {continue}

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
                _ => Err(CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support {}", info.conn_type)))
            }?;

            self.exec_pool_map.insert(info.conn_name.clone(), p);
        }

        Ok(())
    }
    fn reset_plan(&mut self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        let load = loader.load_plan()?;

        self.plans.plan.retain(|x,_| {
            !load.plan.contains_key(x)
        });

        self.plans.plan.extend(load.plan);
        Ok(())
    }
    fn reset(&mut self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        self.reset_db_pool(loader)?;
        self.reset_plan(loader)?;
        Ok(())
    }
}
pub struct GlobalLayout {
    store : Arc<RwLock<GlobalStore>>,
    loader : OnceLock<Box<dyn ConfLoader>>,
    once : AtomicBool
}

impl GlobalLayout {
    pub fn get_exec_pool<S : AsRef<str>>(&self, name : S) -> Result< RelationalExecutorPool<RelationalValue>, CommonError > {
        if self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let opt = reader.exec_pool_map.get(&name.as_ref().to_string());
        if opt.is_none() {
            return CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists {}", name.as_ref())).to_result();
        }
        Ok(opt.unwrap().clone())
    }
    pub fn get_plan(&self) -> Result<HashMap<String, Plan>, CommonError> {
        if self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

       Ok( reader.plans.plan.clone())
    }
    
    pub fn get_interpreter_pool<S : AsRef<str>>(&self, name : S) -> Result<InterpreterPool, CommonError> {
        if self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let opt = reader.exec_interpreter_map.get(name.as_ref());
        if opt.is_none() {
            return CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists {}", name.as_ref())).to_result();
        }
        Ok(opt.unwrap().clone())
    }
    pub fn close(&self) -> Result<(), CommonError> {
        if self.once.load(Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "already initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        writer.exec_pool_map.clear();
        Ok(())
    }

    pub fn reset(&self) -> Result<(), CommonError> {
        if self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        writer.reset(self.loader.get().unwrap().as_ref())?;

        Ok(())
    }

    pub fn initialize(&self, loader : Box<dyn ConfLoader>) -> Result<(), CommonError> {
        if self.once.load(Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "already initialized").to_result();
        }
        let loader =self.loader.get_or_init(move || loader);

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let store = GlobalStore::from_conf_loader(loader.as_ref()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InitFailed, "global store load failed",e)
        })?;

        *writer = store;
        self.once.store(true, Ordering::Relaxed);

        Ok(())
    }
}

pub static GLOBAL: LazyLock<GlobalLayout> = LazyLock::new(|| {
    GlobalLayout {
        store : Arc::new(RwLock::new(GlobalStore::default()) ),
        once : AtomicBool::new(false),
        loader : OnceLock::new(),
    }
});