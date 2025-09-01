use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{self, Duration};
use std::error::Error;

use common_rs::db::core::{CommonSqlExecuteResultSet, CommonValue, CommonSqlConnection};
use common_rs::err::core::{API_CALL_ERROR, COMMON_ERROR_CATEGORY, CRITICAL_ERROR};
use common_rs::err::create_error;
use common_rs::err::db::COMMAND_RUN_ERROR;

use crate::config::PlanConfig;
use crate::global::get_db_conn_from_pool;

#[derive(Default, Clone)]
struct PlanState {
    total_elap : Duration,
    avg_elap : Duration,
    max_elap : Duration,
    min_elap : Duration,

    run_count : u64
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

    pub fn execute_plan(&self) -> Result<PlanState, Box<dyn Error>> {
        if self.is_use.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return create_error(COMMON_ERROR_CATEGORY, CRITICAL_ERROR, 
                format!("already use, call:{:?}", thread::current().id()), None).as_error();
        }

        let timer = time::SystemTime::now();
        let mut input : Option<Vec<Vec<CommonValue>>> = None;

        for query in self.cfg.chain.iter() {
            let mut db_conn = match get_db_conn_from_pool(query.connection.as_str()) {
                Ok(ok) => Ok(ok),
                Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                    Some(e)).as_error()
            }?;


            let conn = db_conn.get_value();

            if input.is_none() {
                let output = match conn.execute(query.query.as_str(), &[]) {
                    Ok(ok) => Ok(ok),
                    Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                        Some(e)).as_error()
                }?;

                if output.cols_data.len() > 0 {
                    input = Some(output.cols_data);
                }
            }
            else {
                let query_input = input.take().unwrap();
                let execute_ret = if query.auto_commit {
                    Self::execute_auto_commit(conn, &query.query, query_input)
                } else {
                    Self::execute_auto_commit_off(conn, &query.query, query_input)
                }?;

                if execute_ret.is_some() {
                    input = Some(execute_ret.unwrap().cols_data)
                }
            }
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