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
use mypip_types::config::conn::{ConnectionInfo, ConnectionInfos, OdbcConnectionInfo};
use mypip_types::config::plan::{Plan, PlanChain, PlanChainArgs, PlanChainBindParam, PlanInterval, PlanRoot, PlanScript};
use mypip_types::interface::ConfLoader;
use crate::pair_db_loader::utils;

macro_rules! get_pair_db_connection {
    ($obj:expr) => {
        $obj.db_pool.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        })
    };
}



pub struct PairDbLoader {
    identifier : String,
    db_pool : PairExecutorPool,
    app_config: AppConfig,

    plan_query : &'static str,
    conn_query : &'static str,
    script_query : &'static str,

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

        let (plan_query, conn_query, script_query) = match db_conf.db_type.as_str() {
            "postgres" => (utils::plan_select_query!("$1"), utils::conn_select_query!("$1"), utils::script_data_select_query!("$1")),
            "duckdb" => (utils::plan_select_query!("?"), utils::conn_select_query!("?"), utils::script_data_select_query!("?")),
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
            plan_query,
            conn_query,
            script_query,
            once_cache : (OnceLock::new(), OnceLock::new(), OnceLock::new()),
        })
    }

    fn create_plan_data(plan_type : String, interval_conn : Option<String>,
                        interval_sec : u64) -> Result<Plan, CommonError> {

        let mut p = Plan::default();

        p.type_name = plan_type;
        p.enable = true;
        p.interval = PlanInterval {
            connection: interval_conn,
            second: interval_sec,
        };
        if p.type_name == "query" {
            p.chain = Some(vec![]);
        }
        Ok(p)
    }

    fn link_script_to_plan(p : &'_ mut Plan, s_lang : &'_ Option<&'_ String>,
                           s_file: &'_ Option<&'_ String>) -> Result<(), CommonError> {
        p.script = Some(PlanScript {
            lang: s_lang.map(|x| {
                x.clone()
            }).ok_or_else(|| CommonError::new(&CommonDefaultErrorKind::NoData, "script_lang not found"))?,
            file:  s_file.map(|x| {
                x.clone()
            }).ok_or_else(|| CommonError::new(&CommonDefaultErrorKind::NoData, "script_file not found"))?,
        });
        Ok(())
    }



    fn get_plan_name_indexs<'a>(s : &'a [&'a String]) -> Vec<(&'a String, usize, usize)> {
        let mut ret = vec![];
        if s.len() <= 0 {
            return ret;
        }
        let mut current_name = s[0];
        let mut current_index = 0;

        for idx in 1..s.len() {
            if s[idx] != current_name {
                ret.push((current_name, current_index, idx));
                current_name = s[idx];
                current_index = idx;
            }
        }
        ret
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

        let mut item = get_pair_db_connection!(self)?;
        let conn = item.get_value();

        let param = PairValueEnum::Array(vec![PairValueEnum::String(self.identifier.clone())]);

        let data = conn.execute_pair(self.plan_query, &param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        let plan_name = utils::get_col_ref!("plan_name", &data, str)?;
        let plan_type = utils::get_col_ref!("type", &data, str)?;
        let interval_connection = utils::get_col_ref!("interval_connection", &data, str, null)?;
        let interval_second = utils::get_col_ref!("interval_second", &data, i32)?;
        let chain_id = utils::get_col_ref!("chain_id", &data, str, null)?;
        let chain_connection = utils::get_col_ref!("chain_connection", &data, str, null)?;
        let chain_query = utils::get_col_ref!("chain_query", &data, str, null)?;
        let mapping_type = utils::get_col_ref!("mapping_type", &data, str, null)?;
        let arg_data = utils::get_col_ref!("arg_data", &data, str, null)?;
        let arg_idx = utils::get_col_ref!("arg_idx", &data, i32, null)?;
        let bind_id = utils::get_col_ref!("bind_id", &data, str, null)?;
        let bind_key = utils::get_col_ref!("bind_key", &data, str, null)?;
        let bind_row = utils::get_col_ref!("bind_row", &data, i32, null)?;
        let bind_idx = utils::get_col_ref!("bind_idx", &data, i32, null)?;
        let script_lang = utils::get_col_ref!("script_lang", &data, str, null)?;
        let script_file = utils::get_col_ref!("script_file", &data, str, null)?;

        if !utils::vec_if_same_len!(plan_name, plan_type, interval_connection, interval_second,
            chain_connection, chain_query,
            mapping_type, arg_data, arg_idx, bind_id,
            bind_key, bind_row, bind_idx, script_lang, script_file) {
            return CommonError::new(&CommonDefaultErrorKind::Critical, "").to_result()
        }

        let mut ret = PlanRoot::default();
        let plan_idx_range = Self::get_plan_name_indexs(plan_name.as_slice());

        for p_range in plan_idx_range {
            let mut p = Self::create_plan_data(
                plan_type[p_range.1].clone(),
                interval_connection[p_range.1].map(|x| x.clone()),
                *interval_second[p_range.1] as u64
            ).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "", e)
            })?;

            if plan_type[p_range.1].as_str() == "script" {
                Self::link_script_to_plan(&mut p,
                                          &script_lang[p_range.1],
                                          &script_file[p_range.1]).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "", e)
                })?;
            } else {
                let mut chains = Vec::<PlanChain>::with_capacity(p_range.2 - p_range.1);
                let mut current_chain : Option<PlanChain>= None;

                for idx in p_range.1..p_range.2 {
                    let ch_id = chain_id[idx].map_or(
                        CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })?;

                    if current_chain.is_some() && ch_id != &current_chain.as_ref().expect("broken eq chain").id {
                        chains.push(current_chain.take().expect("broken chain"));
                    }

                    if current_chain.is_none() {
                        let mut temp_chain = PlanChain::default();
                        temp_chain.id = ch_id.clone();
                        temp_chain.connection = chain_connection[idx].map_or(
                            CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })?.clone();
                        temp_chain.query = chain_query[idx].map_or(
                            CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })?.clone();
                        current_chain = Some(temp_chain);
                    }
                    let current_c = current_chain.as_mut().expect("get failed current_chain");;

                    if mapping_type[idx].expect("broken current mapping type") == "args" {
                        if current_c.args.is_none() {
                            current_c.args = Some(Vec::with_capacity(3));
                        }
                        current_c.args.as_mut().expect("broken current args").push(PlanChainArgs {
                            data: arg_data[idx].map_or(
                                CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })?.clone(),
                            idx : *arg_idx[idx].map_or(
                                CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })? as usize,
                        });
                    } else {
                        if current_c.bind.is_none() {
                            current_c.bind = Some(Vec::with_capacity(3));
                        }
                        current_c.bind.as_mut().expect("broken current bind").push(PlanChainBindParam {
                            idx :  *bind_idx[idx].map_or(
                                CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })? as usize,
                            key: bind_key[idx].map_or(
                                CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })?.clone(),
                            id: bind_id[idx].map_or(
                                CommonError::new(&CommonDefaultErrorKind::NoData, "").to_result(), |x| { Ok(x) })?.clone(),
                            row: bind_row[idx].map(|v| {*v as usize}),
                        });
                    }
                }
                p.chain = Some(chains);
            }

            ret.plan.insert(p_range.0.clone(), p);
        }

        Ok(ret)
    }

    fn load_connection(&self) -> Result<ConnectionInfos, CommonError> {
        if self.once_init_flag {
            if let Some(ret) = self.once_cache.1.get() {
                return Ok(ret.clone());
            }
        }

        let db_type = self.app_config.db_config.as_ref().expect("not exists db config").db_type.as_str();

        let mut item = get_pair_db_connection!(self)?;
        let conn = item.get_value();

        let param = PairValueEnum::Array(vec![PairValueEnum::String(self.identifier.clone())]);

        let data = conn.execute_pair(self.conn_query, &param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        let max_size = utils::get_col_ref!("max_size", &data, i32)?;
        let name = utils::get_col_ref!("name", &data, str)?;
        let conn_type = utils::get_col_ref!("conn_type", &data, str)?;
        let conn_name = utils::get_col_ref!("conn_name", &data, str)?;
        let conn_user = utils::get_col_ref!("conn_user", &data, str)?;
        let conn_addr = utils::get_col_ref!("conn_addr", &data, str)?;
        let conn_passwd = utils::get_col_ref!("conn_passwd", &data, str)?;
        let conn_timeout = utils::get_col_ref!("conn_timeout", &data, i32)?;
        let odbc_driver = utils::get_col_ref!("odbc_driver", &data, str, null)?;
        let odbc_current_time_query = utils::get_col_ref!("odbc_current_time_query", &data, str, null)?;
        let odbc_current_time_col_name = utils::get_col_ref!("odbc_current_time_col_name", &data, str, null)?;

        if !utils::vec_if_same_len!(max_size, name, conn_type, conn_name,
            conn_user, conn_addr,conn_passwd, conn_timeout,
            odbc_driver, odbc_current_time_query, odbc_current_time_col_name) {
            return CommonError::new(&CommonDefaultErrorKind::Critical, "").to_result()
        }

        let mut root = ConnectionInfos::default();

        for idx in 0..name.len() {
            let info = ConnectionInfo {
                max_size: *max_size[idx] as usize,
                conn_type: conn_type[idx].clone(),
                conn_name: conn_name[idx].clone(),
                conn_user: conn_user[idx].clone(),
                conn_addr: conn_addr[idx].clone(),
                conn_passwd: conn_passwd[idx].clone(),
                conn_timeout: *conn_timeout[idx] as u32,
                odbc: if conn_type[idx] == "odbc" {
                    Some(OdbcConnectionInfo {
                        driver: odbc_driver[idx].map(|x| {
                            x.clone()
                        }).ok_or_else(|| CommonError::new(&CommonDefaultErrorKind::NoData, "odbc_driver not found"))?,
                        current_time_query: odbc_current_time_query[idx].map(|x| {
                            x.clone()
                        }).ok_or_else(|| CommonError::new(&CommonDefaultErrorKind::NoData, "odbc_driver not found"))?,
                        current_time_col_name: odbc_current_time_col_name[idx].map(|x| {
                            x.clone()
                        }).ok_or_else(|| CommonError::new(&CommonDefaultErrorKind::NoData, "odbc_driver not found"))?,
                    })
                } else {
                    None
                },
            };

            root.connection.insert(name[idx].clone(), info);
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

        let mut item = get_pair_db_connection!(self)?;
        let conn = item.get_value();

        let param = PairValueEnum::Array(vec![PairValueEnum::String(self.identifier.clone())]);

        let data = conn.execute_pair(self.script_query, &param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        let script_file = utils::get_col_ref!("script_file", &data, str)?;
        let script_data = utils::get_col_ref!("script_data", &data, str)?;

        if !utils::vec_if_same_len!(script_file, script_data) {
            return CommonError::new(&CommonDefaultErrorKind::Critical, "").to_result()
        }

        let mut root = HashMap::with_capacity(script_file.len());

        for idx in 0..script_file.len() {
            root.insert(script_file[idx].clone(), script_data[idx].clone());
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