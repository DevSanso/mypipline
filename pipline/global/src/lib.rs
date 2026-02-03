pub mod constant;
mod etc;

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{LazyLock, OnceLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::duckdb::create_duckdb_pair_conn_pool;
use common_rs::exec::interfaces::pair::*;
use common_rs::exec::pg::create_pg_pair_conn_pool;
use common_rs::exec::redis::create_redis_pair_conn_pool;
use common_rs::exec::scylla::create_scylla_pair_conn_pool;
use common_rs::exec::odbc::create_odbc_pair_conn_pool;
use common_rs::init::InitConfig;
use mypip_loader::toml_file_loader;
use mypip_types::config::app::AppConfig;
use mypip_types::interface::ConfLoader;
use mypip_types::typealias::InterpreterPool;
use mypip_types::config::conn::ConnectionInfos;

use mypip_types::config::plan::{Plan, PlanRoot};
use crate::etc::InterpreterType::{LUA, PYTHON};

#[derive(Default)]
struct GlobalStore {
    exec_pool_map : HashMap<String, PairExecutorPool>,
    exec_interpreter_map : HashMap<&'static str, InterpreterPool>,
    script_data_map : HashMap<String, String>,
    plans : PlanRoot,
}

impl GlobalStore {

    fn reset_db_pool(&mut self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        let data : ConnectionInfos = loader.load_connection().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "global store load fail conn",e)
        })?;
        for info in data.connection {
            let conn_key = info.0;
            let conn_info = info.1;
            if self.exec_pool_map.contains_key(&conn_key) {continue}

            let p = match conn_info.conn_type.as_str() {
                constant::CONN_TYPE_PG => Ok(create_pg_pair_conn_pool(conn_info.conn_name.clone(), PairExecutorInfo {
                    addr: conn_info.conn_addr,
                    name: conn_info.conn_name,
                    user: conn_info.conn_user,
                    password: conn_info.conn_passwd,
                    timeout_sec: conn_info.conn_timeout,
                    extend : None
                }, conn_info.max_size)),
                constant::CONN_TYPE_SCYLLA => Ok(create_scylla_pair_conn_pool(conn_info.conn_name.clone(), vec![PairExecutorInfo {
                    addr: conn_info.conn_addr,
                    name: conn_info.conn_name,
                    user: conn_info.conn_user,
                    password: conn_info.conn_passwd,
                    timeout_sec: conn_info.conn_timeout,
                    extend : None
                }], conn_info.max_size)),
                constant::CONN_TYPE_DUCKDB => Ok(create_duckdb_pair_conn_pool(conn_info.conn_name.clone(), PairExecutorInfo {
                    addr: conn_info.conn_addr,
                    name: conn_info.conn_name,
                    user: conn_info.conn_user,
                    password: conn_info.conn_passwd,
                    timeout_sec: conn_info.conn_timeout,
                    extend : None
                }, conn_info.max_size)),
                constant::CONN_TYPE_REDIS => Ok(create_redis_pair_conn_pool(conn_info.conn_name.clone(), PairExecutorInfo {
                    addr: conn_info.conn_addr,
                    name: conn_info.conn_name,
                    user: conn_info.conn_user,
                    password: conn_info.conn_passwd,
                    timeout_sec: conn_info.conn_timeout,
                    extend : None
                }, conn_info.max_size)),
                constant::CONN_TYPE_ODBC => Ok(create_odbc_pair_conn_pool(conn_info.conn_name.clone(), PairExecutorInfo {
                    addr: String::from(""),
                    name: String::from(""),
                    user: String::from(""),
                    password: String::from(""),
                    timeout_sec: conn_info.conn_timeout,
                    extend : if let Some(odbc_info) = conn_info.odbc {
                        let  addr : Vec<&'_ str> = conn_info.conn_addr.split(":").collect();
                        if addr.len() < 2 {
                            return CommonError::new(&CommonDefaultErrorKind::NoData, "ODBC addr split count <2 ").to_result();
                        }

                        let data_source = format!("Driver={{{}}};Server={};Port={};Database={};Uid={};Pwd={}",
                            odbc_info.driver, addr[0], addr[1], conn_info.conn_name, conn_info.conn_user, conn_info.conn_passwd);
                        Some(vec![data_source, odbc_info.current_time_query, odbc_info.current_time_col_name])
                    } else {
                        return CommonError::new(&CommonDefaultErrorKind::NoData, "ODBC INFO not exists").to_result();
                    }
                }, conn_info.max_size)),
                _ => Err(CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support {}", conn_info.conn_type)))
            }?;

            self.exec_pool_map.insert(conn_key.clone(), p);
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
    fn reset_scripts_file(&mut self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        let map = loader.load_script_data().map_err(|e| { 
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "global store load fail scripts",e)
        })?;
        
        self.script_data_map.retain(|x,_| {
           map.get(x).is_some() 
        });
        
        self.script_data_map.extend(map);
        Ok(())
    }
    fn reset(&mut self, loader : &'_ dyn ConfLoader) -> Result<(), CommonError> {
        self.reset_db_pool(loader)?;
        self.reset_plan(loader)?;
        self.reset_scripts_file(loader)?;
        Ok(())
    }
}

pub struct GlobalOnceLockStore {
    identifier : String,
    base_dir : String,
    config_dir: String,
    log_dir    : String,
    script_dir : String
}
pub struct GlobalImpl {
    store : Arc<RwLock<GlobalStore>>,
    loader : OnceLock<Box<dyn ConfLoader>>,
    once : AtomicBool,

    once_store : OnceLock<GlobalOnceLockStore>,
}

impl mypip_types::interface::GlobalLayout for GlobalImpl {
    fn get_exec_pool(&self, name : Cow<'_, str>) -> Result<PairExecutorPool, CommonError > {
        if !self.once.load(Ordering::Relaxed) {
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
    fn get_plan(&self) -> Result<HashMap<String, Plan>, CommonError> {
        if !self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Ok( reader.plans.plan.clone())
    }

    fn get_interpreter_pool(&self, name : Cow<'_, str>) -> Result<InterpreterPool, CommonError> {
        if !self.once.load(Ordering::Relaxed) {
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
    fn close(&self) -> Result<(), CommonError> {
        if !self.once.load(Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        writer.exec_pool_map.clear();
        Ok(())
    }

    fn reset(&self) -> Result<(), CommonError> {
        if !self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        writer.reset(self.loader.get().unwrap().as_ref())?;

        Ok(())
    }

    fn initialize(&self, identifier : String, base_dir : String) -> Result<(), CommonError> {
        if self.once.load(Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "already initialized").to_result();
        }

        let config_dir = std::path::Path::new(&base_dir).join("config").join(identifier.as_str()).to_string_lossy().to_string();
        let log_dir = std::path::Path::new(&base_dir).join("log").join(identifier.as_str()).to_string_lossy().to_string();
        let script_dir = std::path::Path::new(&base_dir).join("scripts").join(identifier.as_str()).to_string_lossy().to_string();
        
        let loader_config_dir = config_dir.clone();
        let loader_script_dir = script_dir.clone();
        let loader_identifier = identifier.clone();
        let loader =self.loader.get_or_init(move || {
            let loader = toml_file_loader
            ::TomlFileConfLoader::new(loader_config_dir, loader_script_dir, loader_identifier, true);
            Box::new(loader)
        });
        
        let app_config = loader.load_app_config()?;

        common_rs::init::init_common(InitConfig {
            log_level: app_config.log_level.as_str(),
            log_file: if app_config.log_type == "console" {
                None
            } else {
                Some(log_dir.as_str())
            },
            log_file_size : (app_config.log_max_size_mb as usize * 1024 * 1024),
        })?;

        self.once_store.get_or_init(move || {
           GlobalOnceLockStore {
               identifier,
               base_dir,
               config_dir,
               log_dir,
               script_dir,
           }
        });

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let mut store = GlobalStore {
            exec_pool_map: HashMap::new(),
            exec_interpreter_map : HashMap::new(),
            plans : PlanRoot::default(),
            script_data_map: HashMap::new(),
        };

        store.reset(loader.as_ref())?;
        store.exec_interpreter_map.insert("lua", crate::etc::create_interpreter_pool(LUA,100));
        store.exec_interpreter_map.insert("python", crate::etc::create_interpreter_pool(PYTHON, 100));

        *writer = store;

        self.once.store(true, Ordering::Relaxed);

        Ok(())
    }

    fn get_script_data(&self, name: &'_ str) -> Result<String, CommonError> {
        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;
        
        if let Some(data) = reader.script_data_map.get(name) {
            Ok(data.clone())
        } else {
            CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists {}", name)).to_result()
        }
    }
}

pub static GLOBAL: LazyLock<GlobalImpl> = LazyLock::new(|| {
    GlobalImpl {
        store : Arc::new(RwLock::new(GlobalStore::default()) ),
        once : AtomicBool::new(false),
        loader : OnceLock::new(),
        once_store: Default::default(),
    }
});