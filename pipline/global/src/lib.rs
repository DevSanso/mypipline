pub mod constant;
mod etc;

use std::borrow::Cow;
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
use common_rs::init::{InitConfig, LoggerConf, convert_str_to_log_level};
use mypip_loader::{toml_file_loader, pair_db_loader};
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
                constant::CONN_TYPE_SCYLLA => Ok(create_scylla_pair_conn_pool(conn_info.conn_name.clone(), PairExecutorInfo {
                    addr: conn_info.conn_addr,
                    name: conn_info.conn_name,
                    user: conn_info.conn_user,
                    password: conn_info.conn_passwd,
                    timeout_sec: conn_info.conn_timeout,
                    extend : None
                }, conn_info.max_size)),
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
                    addr: vec![],
                    name: String::from(""),
                    user: String::from(""),
                    password: String::from(""),
                    timeout_sec: conn_info.conn_timeout,
                    extend : if let Some(odbc_info) = conn_info.odbc {
                        let  addr : Vec<&'_ str> = conn_info.conn_addr[0].split(":").collect();
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
    script_lib_base_dir: Option<String>,
}
pub struct GlobalImpl {
    store : Arc<RwLock<GlobalStore>>,
    loader : OnceLock<Box<dyn ConfLoader>>,
    once : AtomicBool,

    once_store : OnceLock<GlobalOnceLockStore>,
}

impl mypip_types::interface::GlobalLayoutInit for GlobalImpl {
    fn initialize(&'static self, identifier : String, base_dir : String, loader_type : String, once_conf_load : bool, app_config: AppConfig) -> Result<(), CommonError> {
        if self.once.load(Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "already initialized").to_result();
        }
        let config_dir = std::path::Path::new(&base_dir).join("config").join(identifier.as_str()).to_string_lossy().to_string();
        let log_dir = std::path::Path::new(&base_dir).join("log").join(identifier.as_str()).to_string_lossy().to_string();
        let script_dir = std::path::Path::new(&base_dir).join("scripts").join(identifier.as_str()).to_string_lossy().to_string();

        let new_loader : Box<dyn ConfLoader> = match loader_type.as_str() {
            constant::LOADER_TYPE_DB => {
                pair_db_loader::rdb::PairDbLoader::new(identifier.clone(), config_dir.as_str(), once_conf_load, false).map(|l| {
                    Box::new(l) as Box<dyn ConfLoader>
                }).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::Etc, "", e)
                })
            },
            constant::LOADER_TYPE_DB_TOML => {
                pair_db_loader::rdb::PairDbLoader::new(identifier.clone(), config_dir.as_str(), once_conf_load, true).map(|l| {
                    Box::new(l) as Box<dyn ConfLoader>
                }).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::Etc, "", e)
                })
            },
            constant::LOADER_TYPE_FILE => {
                let ok : Result<Box<dyn ConfLoader>, CommonError> = Ok(Box::new(toml_file_loader::TomlFileConfLoader::new(config_dir, script_dir, identifier.clone(), once_conf_load)) as Box<dyn ConfLoader>);
                ok
            },
            _ => {
                CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support {}", loader_type)).to_result::<Box<dyn ConfLoader>, CommonError>()
            }
        }?;
        
        let loader =self.loader.get_or_init(move || {
            new_loader
        });
        
        let logger_cnf = match app_config.log_conf.log_type.as_str() {
            "console" => {
                Ok(LoggerConf::Console)
            },
            "file" => {
                app_config.log_conf.log_file_size_mb.map_or_else(
                    || {
                        CommonError::new(&CommonDefaultErrorKind::NoData, "no file size config").to_result()
                    },
                    |o| {
                        Ok(LoggerConf::File(log_dir, convert_str_to_log_level(app_config.log_conf.log_level.as_str()), o * 1024 * 1024))
                    }
                )
            },
            "scylla" => {
                app_config.log_conf.log_db_config.map_or_else(
                    || {
                        CommonError::new(&CommonDefaultErrorKind::NoData, "no db config").to_result()
                    },
                    |o| {
                        Ok(
                            LoggerConf::Scylla(identifier.clone(), o.db_address, o.db_name, o.db_user, o.db_password,
                                               convert_str_to_log_level(app_config.log_conf.log_level.as_str()), o.db_ttl)
                        )
                    }
                )
            },
            _ => {
                CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support {}", loader_type)).to_result()
            }
        }?;
  
        common_rs::init::init_common(InitConfig {
            logger_conf : logger_cnf
        })?;

        self.once_store.get_or_init(move || {
            GlobalOnceLockStore {
                script_lib_base_dir : app_config.script_lib
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
        mypip_interpreter::init::interpreter_init(self).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::Critical, "interpreter init failed", e)
        })?;

        store.exec_interpreter_map.insert("lua", crate::etc::create_interpreter_pool(LUA,100));
        store.exec_interpreter_map.insert("python", crate::etc::create_interpreter_pool(PYTHON, 100));

        *writer = store;

        self.once.store(true, Ordering::Relaxed);

        Ok(())
    }
}

impl mypip_types::interface::GlobalLayout for GlobalImpl {
    fn get_exec_pool(&'static self, name : Cow<'_, str>) -> Result<PairExecutorPool, CommonError > {
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
    fn get_plan(&'static self) -> Result<HashMap<String, Plan>, CommonError> {
        if !self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Ok( reader.plans.plan.clone())
    }

    fn get_interpreter_pool(&'static self, name : Cow<'_, str>) -> Result<InterpreterPool, CommonError> {
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
    fn close(&'static self) -> Result<(), CommonError> {
        if !self.once.load(Ordering::Relaxed) == true {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        writer.exec_pool_map.clear();
        mypip_interpreter::init::interpreter_exit().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::Critical, "interpreter exit failed", e)
        })?;
        Ok(())
    }

    fn reset(&'static self) -> Result<(), CommonError> {
        if !self.once.load(Ordering::Relaxed) {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not initialized").to_result();
        }

        let mut writer = self.store.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        writer.reset(self.loader.get().unwrap().as_ref())?;

        Ok(())
    }
    fn get_script_data(&'static self, name: &'_ str) -> Result<String, CommonError> {
        let reader = self.store.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;
        
        if let Some(data) = reader.script_data_map.get(name) {
            Ok(data.clone())
        } else {
            CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists {}", name)).to_result()
        }
    }
    
    fn get_script_lib_path(&'static self) -> Result<Option<String>, CommonError> {
        let s = match self.once_store.get() {
            None => {
                return CommonError::new(&CommonDefaultErrorKind::Critical, "global once store not init").to_result();
            }
            Some(s) => {s}
        };

        Ok(s.script_lib_base_dir.clone())
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