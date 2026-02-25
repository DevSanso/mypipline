use std::collections::HashMap;
use std::sync::OnceLock;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::{PairExecutorPool, PairValueEnum};
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
        concat!("SELECT name as name, value::VARCHAR as data FROM ", $table, " WHERE identifier = ? ")
    };
}

macro_rules! postgres_select_query {
    ($table:expr) => {
        concat!("SELECT name as name, value::TEXT as data FROM ", $table, " WHERE identifier = $1 ")
    };
}



pub struct PairDbLoader {
    identifier : String,
    db_type : String,
    db_pool : PairExecutorPool,
    app_config_cache : OnceLock<AppConfig>,
}

impl PairDbLoader {
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
        let query = match self.db_type.as_str() {
            "postgres" => postgres_select_query!("mypip_plan"),
            "duckdb" => duckdb_select_query!("mypip_plan"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", self.db_type.as_str())).to_result();
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

        Ok(root)
    }

    fn load_connection(&self) -> Result<ConnectionInfos, CommonError> {
        let query = match self.db_type.as_str() {
            "postgres" => postgres_select_query!("mypip_connection"),
            "duckdb" => duckdb_select_query!("mypip_connection"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", self.db_type.as_str())).to_result();
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

        Ok(root)
    }

    fn load_script_data(&self) -> Result<HashMap<String, String>, CommonError> {
        let query = match self.db_type.as_str() {
            "postgres" => postgres_select_query!("mypip_script"),
            "duckdb" => duckdb_select_query!("mypip_script"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", self.db_type.as_str())).to_result();
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
        Ok(root)
    }

    fn load_app_config(&self) -> Result<AppConfig, CommonError> {
        if let Some(cache) = self.app_config_cache.get() {
            return Ok(cache.clone());
        }

        let query = match self.db_type.as_str() {
            "postgres" => postgres_select_query!("mypip_app_config"),
            "duckdb" => duckdb_select_query!("mypip_app_config"),
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                        format!("{}", self.db_type.as_str())).to_result();
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

        let conf = Self::get_use_reset_data::<AppConfig>(&rows).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::FetchFailed, "", e)
        })?;

        if conf.len() <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::Critical, "not exists AppConfig").to_result();
        }

        let c_ref = self.app_config_cache.get_or_init(|| {conf[0].1.clone()});
        Ok(c_ref.clone())
    }
}