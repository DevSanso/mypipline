use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{self, Duration};
use std::error::Error;

use common_rs::db::core::{CommonSqlConnection, CommonSqlConnectionPool, CommonSqlExecuteResultSet, CommonValue};
use common_rs::err::core::{API_CALL_ERROR, COMMON_ERROR_CATEGORY, CRITICAL_ERROR, NO_DATA_ERROR};
use common_rs::err::create_error;
use common_rs::err::db::{COMMAND_RUN_ERROR, COMMON_CONN_ERROR_CATEGORY};

use crate::config::PlanConfig;
use crate::map::DbConnPool;

#[derive(Default, Clone)]
pub struct PlanState {
    pub total_elap : Duration,
    pub avg_elap : Duration,
    pub max_elap : Duration,
    pub min_elap : Duration,

    pub run_count : u64
}

struct DbPoolParam<'a> {
    pub query : &'a str, 
    pub is_autocommit : bool,
    pub params : Option<CommonSqlExecuteResultSet>
}

pub struct Plan {
    cfg :PlanConfig,
    state : RefCell<PlanState>,
    is_use : AtomicBool
}

unsafe impl Send for Plan {}
unsafe impl Sync for Plan {}

impl Plan {
    pub fn new(cfg : PlanConfig) -> Self {
        Plan { cfg: cfg, state : RefCell::new(PlanState::default()), is_use : AtomicBool::new(false)}
    }
    #[inline]
    pub fn get_interval(&self) -> u32 {self.cfg.interval}

    fn execute_auto_commit_off(conn : &mut Box<dyn CommonSqlConnection>, query : &'_ str, params : Vec<Vec<CommonValue>>) -> Result<Option<CommonSqlExecuteResultSet>, Box< dyn Error>> {
        let tx = conn.trans()?;
        let p : Vec<&[CommonValue]> = params.iter().map(|v| v.as_slice()).collect();

        tx.execute_tx(query, p.as_slice()).map_err(|x| {
            create_error(COMMON_ERROR_CATEGORY, COMMAND_RUN_ERROR, "".to_string(), Some(x))
            .as_error::<()>().unwrap_err()
        })?;

        Ok(None)
    }

    fn execute_auto_commit(conn : &mut Box<dyn CommonSqlConnection>, query : &'_ str, params : Vec<Vec<CommonValue>>) -> Result<Option<CommonSqlExecuteResultSet>, Box< dyn Error>> {
        let mut ret : Option<CommonSqlExecuteResultSet> = None;
        
        for param in params {
            let output = match conn.execute(query, param.as_slice()) {
                Ok(ok) => Ok(ok),
                Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                    Some(e)).as_error()
            }?;

            if output.cols_data.len() > 0 {
                ret = Some(output)
            }
        }
        Ok(ret)
    }


    fn execute_callback(db_pool : Option<&mut CommonSqlConnectionPool>, p : Option<DbPoolParam>) -> Result<Option<CommonSqlExecuteResultSet>, Box< dyn Error>> {
        let mut ret = None;
        
        if db_pool.is_none() {
            return create_error(COMMON_ERROR_CATEGORY, NO_DATA_ERROR, "db pool none".to_string(),
                    None).as_error();
        }

        if p.is_none() {
            return create_error(COMMON_ERROR_CATEGORY, NO_DATA_ERROR, "param none".to_string(),
                    None).as_error();
        }

        let db_p = db_pool.unwrap();

        let mut db_conn = match db_p.get_owned(()) {
            Ok(ok) => Ok(ok),
            Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                Some(e)).as_error()
        }?;

        let conn = db_conn.get_value();

        let param = p.unwrap();

        if param.params.is_none() {
            let output = match conn.execute(param.query, &[]) {
                Ok(ok) => Ok(ok),
                Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                    Some(e)).as_error()
            }?;

            if output.cols_data.len() > 0 {
                ret = Some(output)
            }
        }
        else {
            let query_input = param.params.unwrap();

            ret = if param.is_autocommit {
                Self::execute_auto_commit(conn, param.query, query_input.cols_data)
            } else {
                Self::execute_auto_commit_off(conn, param.query, query_input.cols_data)
            }?;
        }

        Ok(ret)
    }

    pub fn execute_plan(&self, db_map : &'_ DbConnPool) -> Result<PlanState, Box<dyn Error>> {
        if self.is_use.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return create_error(COMMON_ERROR_CATEGORY, CRITICAL_ERROR, 
                format!("already use, call:{:?}", thread::current().id()), None).as_error();
        }

        let timer = time::SystemTime::now();
        let mut input : Option<CommonSqlExecuteResultSet> = None;

        for query in self.cfg.chain.iter() {
            let call_param = DbPoolParam {
                query : query.query.as_str(),
                is_autocommit : query.auto_commit,
                params : input.take()
            };
            let call_ret = db_map.call_fn(&query.connection, Some(call_param), &Self::execute_callback).map_err(|x| {
                create_error(COMMON_CONN_ERROR_CATEGORY, COMMAND_RUN_ERROR, "".to_string(), Some(x))
                    .as_error::<()>()
                    .unwrap_err()
            })?;

            input = call_ret;
        }

        let elap = timer.elapsed().map_err(|x| {
            create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "elaped failed".to_string(),
             Some(Box::new(x))).as_error::<()>().unwrap_err()
        })?;

        let mut old = self.state.borrow_mut();
        old.max_elap = old.max_elap.max(elap);
        old.min_elap = old.min_elap.min(elap);
        old.run_count += 1;
        old.total_elap += elap;
        old.avg_elap = old.total_elap / old.run_count as u32;
        self.is_use.store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(old.clone())
    }
}