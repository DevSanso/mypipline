use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::duckdb::create_duckdb_pair_conn_pool;
use common_rs::exec::interfaces::pair::{PairExecutorInfo, PairExecutorPool, PairValueEnum};
use common_rs::exec::pg::create_pg_pair_conn_pool;
use mypip_types::config::app::AppConfig;
use mypip_types::config::conn::{ConnectionInfo, ConnectionInfos};
use mypip_types::config::plan::{Plan, PlanRoot};
use mypip_types::interface::ConfLoader;

macro_rules! get_pair_db_connection {
    ($obj:expr) => {
        $obj.db_pool.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        })
    };
}

macro_rules! duckdb_select_query {
    ($table:expr) => {
        concat!("SELECT name as name, value::VARCHAR as data FROM ", $table, " WHERE identifier = ? AND use_yn = 'Y' ")
    };
}

macro_rules! postgres_select_query {
    ($table:expr) => {
        concat!("SELECT name as name, value::TEXT as data FROM ", $table, " WHERE identifier = $1 AND use_yn = 'Y' ")
    };
}

pub struct PairDbLoader {
    identifier : String,
    db_pool : PairExecutorPool,
    app_config: AppConfig,

    once_init_flag : bool,
    once_cache : (OnceLock<PlanRoot>, OnceLock<ConnectionInfos>,  OnceLock<HashMap<String,String>>)
}

impl PairDbLoader {
    pub fn new(identifier : String ,conf_path : &'_ str, load_once : bool) -> Result<Self, CommonError> {
        let mut app_path = PathBuf::from_str(conf_path).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        app_path.push(identifier.as_str());
        app_path.push("app.toml");

        let data = std::fs::read_to_string(app_path).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let convert : AppConfig = toml::from_str(data.as_str()).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail,e.to_string())
        })?;

        if convert.db_config.is_none() {
            return CommonError::new(&CommonDefaultErrorKind::NoData, "not exists db config").to_result();
        }

        let db_conf = convert.db_config.as_ref().expect("db_config is broken");
        let conn_info = PairExecutorInfo {
            addr: db_conf.db_address.clone(),
            name: db_conf.db_name.clone(),
            user: db_conf.db_user.clone(),
            password: db_conf.db_password.clone(),
            timeout_sec: 10,
            extend : None
        };

        let p = match db_conf.db_type.as_str() {
            "postgres" => create_pg_pair_conn_pool("pair_db_loader".to_string(), conn_info, 1),
            "duckdb" => create_duckdb_pair_conn_pool("pair_db_loader".to_string(),conn_info, 1),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("not support {}", db_conf.db_type)).to_result();
            }
        };

        Ok(PairDbLoader {
            identifier,
            db_pool : p,
            app_config : convert,
            once_init_flag : load_once,
            once_cache : (OnceLock::new(), OnceLock::new(), OnceLock::new()),
        })
    }
    fn parsing_data<'a, T: for<'de> serde::Deserialize<'de>>(data : &'a str) -> Result<T, CommonError> {
        let ret = match serde_json::from_str(data) {
            Ok(data) => Ok(data),
            Err(e) => Err(CommonError::new(&CommonDefaultErrorKind::ParsingFail, e.to_string())),
        };

        ret
    }

    fn get_data_rows(data : &'_ PairValueEnum) -> Result<Vec<(&'_ String, &'_ String)>, CommonError> {
        let ret = if let PairValueEnum::Map(m) = data {
            let names = if let Some(PairValueEnum::Array(name)) = m.get("name") {
                name
            } else {
                return CommonError::new(&CommonDefaultErrorKind::ParsingFail, "get name array failed").to_result();
            };
            let data = if let Some(PairValueEnum::Array(d)) = m.get("data") {
                d
            } else {
                return CommonError::new(&CommonDefaultErrorKind::ParsingFail, "get data array failed").to_result();
            };

            if names.len() != data.len() {
                return CommonError::new(&CommonDefaultErrorKind::Critical, "array not matching").to_result();
            }

            if names.len() == 0 {
                vec![]
            } else {
                let mut v = Vec::with_capacity(names.len());
                for idx in 0..names.len() {
                    let unboxing_name = if let PairValueEnum::String(n) = &names[idx] {
                        n
                    } else {
                        return CommonError::new(&CommonDefaultErrorKind::ParsingFail, "name unboxing failed").to_result();
                    };

                    let unboxing_data = if let PairValueEnum::String(d) = &data[idx] {
                        d
                    } else {
                        return CommonError::new(&CommonDefaultErrorKind::ParsingFail, "name unboxing failed").to_result();
                    };
                    v.push((unboxing_name, unboxing_data));
                }
                v
            }
        } else {
            return CommonError::new(&CommonDefaultErrorKind::ParsingFail, "root parsing failed").to_result();
        };

        Ok(ret)
    }

    fn get_use_reset_data<'a, T: for<'de> serde::Deserialize<'de>>(v : &'a Vec<(&'a String, &'a String)>) -> Result<Vec<(&'a String, T)>, CommonError> {
        let mut ret = Vec::with_capacity(v.len());

        for item in v.iter() {
            let convert : T = Self::parsing_data(item.0.as_str()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "", e)
            })?;
            ret.push((item.0, convert))
        }

        Ok(ret)
    }
}

impl ConfLoader for PairDbLoader {
    fn load_plan(&self) -> Result<PlanRoot, CommonError> {
        if self.once_init_flag {
            if let Some(ret) = self.once_cache.0.get() {
                return Ok(ret.clone());
            }
        }

        let db_type = self.app_config.db_config.as_ref().expect("not exists db config").db_type.as_str();
        let query = match db_type {
            "postgres" => postgres_select_query!("mypip_plan"),
            "duckdb" => duckdb_select_query!("mypip_plan"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", db_type)).to_result();
            }
        };

        let mut item = get_pair_db_connection!(self)?;
        let conn = item.get_value();

        let param = PairValueEnum::Array(vec![PairValueEnum::String(self.identifier.clone())]);

        let data = conn.execute_pair(query, &param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        let rows = PairDbLoader::get_data_rows(&data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "", e)
        })?;

        let mut root = PlanRoot::default();
        let use_p = Self::get_use_reset_data::<Plan>(&rows).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::FetchFailed, "", e)
        })?;

        for use_p_item in use_p {
            root.plan.insert(use_p_item.0.clone(), use_p_item.1);
        }

        if self.once_init_flag {
            self.once_cache.0.get_or_init(|| {root.clone()});
        }

        Ok(root)
    }

    fn load_connection(&self) -> Result<ConnectionInfos, CommonError> {
        if self.once_init_flag {
            if let Some(ret) = self.once_cache.1.get() {
                return Ok(ret.clone());
            }
        }

        let db_type = self.app_config.db_config.as_ref().expect("not exists db config").db_type.as_str();
        
        let query = match db_type {
            "postgres" => postgres_select_query!("mypip_connection"),
            "duckdb" => duckdb_select_query!("mypip_connection"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", db_type)).to_result();
            }
        };

        let mut item = get_pair_db_connection!(self)?;
        let conn = item.get_value();

        let param = PairValueEnum::Array(vec![PairValueEnum::String(self.identifier.clone())]);

        let data = conn.execute_pair(query, &param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        let rows = PairDbLoader::get_data_rows(&data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "", e)
        })?;

        let mut root = ConnectionInfos::default();
        let use_c = Self::get_use_reset_data::<ConnectionInfo>(&rows).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::FetchFailed, "", e)
        })?;

        for use_p_item in use_c {
            root.connection.insert(use_p_item.0.clone(), use_p_item.1);
        }

        if self.once_init_flag {
            self.once_cache.1.get_or_init(|| {root.clone()});
        }

        Ok(root)
    }

    fn load_script_data(&self) -> Result<HashMap<String, String>, CommonError> {
        if self.once_init_flag {
            if let Some(ret) = self.once_cache.2.get() {
                return Ok(ret.clone());
            }
        }

        let db_type = self.app_config.db_config.as_ref().expect("not exists db config").db_type.as_str();
        let query = match db_type {
            "postgres" => postgres_select_query!("mypip_script"),
            "duckdb" => duckdb_select_query!("mypip_script"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", db_type)).to_result();
            }
        };

        let mut item = get_pair_db_connection!(self)?;
        let conn = item.get_value();

        let param = PairValueEnum::Array(vec![PairValueEnum::String(self.identifier.clone())]);

        let data = conn.execute_pair(query, &param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        let rows = PairDbLoader::get_data_rows(&data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "", e)
        })?;
        let mut root = HashMap::with_capacity(rows.len());

        let use_c = Self::get_use_reset_data::<String>(&rows).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::FetchFailed, "", e)
        })?;

        for use_p_item in use_c {
            root.insert(use_p_item.0.clone(), use_p_item.1);
        }

        if self.once_init_flag {
            self.once_cache.2.get_or_init(|| {root.clone()});
        }

        Ok(root)
    }

    fn load_app_config(&self) -> Result<AppConfig, CommonError> {
        Ok(self.app_config.clone())
    }
}