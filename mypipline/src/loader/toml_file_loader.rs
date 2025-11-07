use std::cell::RefMut;
use std::error::Error;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonErrorList;

use crate::loader::ConfLoader;
use crate::types::config::{ConnectionInfos, PlanRoot};

pub struct TomlFileConfLoader {
    root_path : String,

    is_once_load : bool,
    once_cache : (OnceLock<PlanRoot>, OnceLock<ConnectionInfos>)
}

impl TomlFileConfLoader {
    pub fn new(root : String, load_once : bool) ->Self {
        TomlFileConfLoader {
            root_path : root,
            is_once_load : load_once,
            once_cache : (OnceLock::new(), OnceLock::new()) }
    }

    pub fn read_data(&self, data_file : &'static str) -> Result<String, CommonError> {
        match std::fs::read_to_string(std::path::Path::new(&self.root_path).join(data_file)) {
            Ok(data) => Ok(data),
            Err(e) => Err(CommonError::new(&CommonErrorList::NoData, e.to_string())),
        }
    }

    pub fn parsing_data<'a, T: for<'de> serde::Deserialize<'de>>(&self, data : &'a str) -> Result<T, CommonError> {
        match toml::from_str(data) {
            Ok(data) => Ok(data),
            Err(e) => Err(CommonError::new(&CommonErrorList::ParsingFAil, e.to_string())),
        }
    }
}

impl ConfLoader for TomlFileConfLoader {
    fn load_plan(&self) -> Result<PlanRoot, Box<dyn Error>> {
        let ret : Result<PlanRoot, Box<dyn Error>> = if self.is_once_load {
            let c = self.once_cache.0.get();
            if c.is_none() {
                let data = self.read_data("plan.toml")?;
                let root : PlanRoot = self.parsing_data(data.as_str())?;
                let _ = self.once_cache.0.set(root.clone());
                Ok(root)
            } else {
                Ok(c.unwrap().clone())
            }
        }
        else {
            let data = self.read_data("plan.toml")?;
            let root : PlanRoot = self.parsing_data(data.as_str())?;
            Ok(root)
        };

        ret
    }

    fn load_connection(&self) -> Result<ConnectionInfos,  Box<dyn Error>> {
        let ret : Result<ConnectionInfos, Box<dyn Error>> = if self.is_once_load {
            let c = self.once_cache.1.get();
            if c.is_none() {
                let data = self.read_data("plan.toml")?;
                let root : ConnectionInfos = self.parsing_data(data.as_str())?;
                let _ = self.once_cache.1.set(root.clone());
                Ok(root)
            } else {
                Ok(c.unwrap().clone())
            }
        }
        else {
            let data = self.read_data("plan.toml")?;
            let root : ConnectionInfos = self.parsing_data(data.as_str())?;
            Ok(root)
        };

        ret
    }
}