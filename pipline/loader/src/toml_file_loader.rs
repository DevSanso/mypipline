use std::collections::HashMap;
use std::sync::OnceLock;
use common_rs::c_err::{CommonError, gen::CommonDefaultErrorKind};
use mypip_types::config::app::AppConfig;
use mypip_types::interface::ConfLoader;
use mypip_types::config::plan::*;
use mypip_types::config::conn::*;

pub struct TomlFileConfLoader {
    root_path : String,
    script_dir : String,

    identifier : String,
    is_once_load : bool,
    once_cache : (OnceLock<PlanRoot>, OnceLock<ConnectionInfos>, OnceLock<AppConfig>)
}

impl TomlFileConfLoader {
    pub fn new(root : String, script_dir : String, identifier : String, load_once : bool) ->Self {
        TomlFileConfLoader {
            root_path : root,
            is_once_load : load_once,
            identifier,
            script_dir,
            once_cache : (OnceLock::new(), OnceLock::new(), OnceLock::new()) }
    }

    pub fn read_data(&self, data_file : String) -> Result<String, CommonError> {
        let p = std::path::Path::new(&self.root_path).join(data_file);
        match std::fs::read_to_string(p) {
            Ok(data) => Ok(data),
            Err(e) => Err(CommonError::new(&CommonDefaultErrorKind::NoData, e.to_string())),
        }
    }

    pub fn parsing_data<'a, T: for<'de> serde::Deserialize<'de>>(&self, data : &'a str) -> Result<T, CommonError> {
        let ret = match toml::from_str(data) {
            Ok(data) => Ok(data),
            Err(e) => Err(CommonError::new(&CommonDefaultErrorKind::ParsingFail, e.to_string())),
        };

        ret
    }
}

impl ConfLoader for TomlFileConfLoader {
    fn load_plan(&self) -> Result<PlanRoot, CommonError> {
        let ret : Result<PlanRoot, CommonError> = if self.is_once_load {
            let c = self.once_cache.0.get();
            if c.is_none() {
                let data = self.read_data("plan.toml".to_string())?;
                let mut root : PlanRoot = self.parsing_data(data.as_str())?;
                root.plan.retain(|_, val| {
                    val.enable == true
                });
                let _ = self.once_cache.0.set(root.clone());
                Ok(root)
            } else {
                Ok(c.unwrap().clone())
            }
        }
        else {
            let data = self.read_data("plan.toml".to_string())?;
            let mut root : PlanRoot = self.parsing_data(data.as_str())?;
            root.plan.retain(|_, val| {
                val.enable == true
            });
            Ok(root)
        };

        ret
    }

    fn load_connection(&self) -> Result<ConnectionInfos,  CommonError> {
        let ret : Result<ConnectionInfos, CommonError> = if self.is_once_load {
            let c = self.once_cache.1.get();
            if c.is_none() {
                let data = self.read_data("conn.toml".to_string())?;
                let root : ConnectionInfos = self.parsing_data(data.as_str())?;
                let _ = self.once_cache.1.set(root.clone());
                Ok(root)
            } else {
                Ok(c.unwrap().clone())
            }
        }
        else {
            let data = self.read_data("conn.toml".to_string())?;
            let root : ConnectionInfos = self.parsing_data(data.as_str())?;
            Ok(root)
        };

        ret
    }

    fn load_script_data(&self) -> Result<HashMap<String, String>, CommonError> {
        let plans = self.load_plan().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InitFailed, "load plan failed", e)
        })?;

        let mut map = HashMap::new();
        for (name, p) in plans.plan {
            if let Some(script) = p.script {
                let script_path = std::path::Path::new(self.script_dir.as_str()).join(script.file.as_str());
                let data = std::fs::read_to_string(script_path).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("read failed script {}, {}", script.file.as_str(), e))
                })?;

                map.insert(script.file, data);
            }
        }

        Ok(map)
    }

    fn load_app_config(&self) -> Result<AppConfig, CommonError> {
        let ret : Result<AppConfig, CommonError> = if self.is_once_load {
            let c = self.once_cache.2.get();
            if c.is_none() {
                let data = self.read_data("app.toml".to_string())?;
                let root : AppConfig = self.parsing_data(data.as_str())?;
                let _ = self.once_cache.2.set(root.clone());
                Ok(root)
            } else {
                Ok(c.unwrap().clone())
            }
        }
        else {
            let data = self.read_data("app.toml".to_string())?;
            let root : AppConfig = self.parsing_data(data.as_str())?;
            Ok(root)
        };

        ret
    }
}