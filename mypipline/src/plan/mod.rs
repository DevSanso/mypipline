use std::time::{self, Duration};
use std::error::Error;

use common_rs::db::core::{CommonSqlExecuteResultSet, CommonValue};
use common_rs::err::core::{API_CALL_ERROR, COMMON_ERROR_CATEGORY};
use common_rs::err::create_error;

use crate::config::PlanConfig;
use crate::global::get_db_conn_from_pool;

#[derive(Default, Clone)]
struct PlanState {
    total_epel : Duration,
    avg_epel : Duration,
    max_epel : Duration,
    min_epel : Duration,

    run_count : u64
}

pub struct Plan {
    cfg :PlanConfig,
    state : PlanState
}

impl Plan {
    pub fn new(cfg : PlanConfig) -> Self {
        Plan { cfg: cfg, state : PlanState::default()}
    }
    #[inline]
    pub fn get_interval(&self) -> u32 {self.cfg.interval}

    pub fn execute_plan(&self) -> Result<PlanState, Box<dyn Error>> {
        let state = &self.state;
        let timer = time::SystemTime::now();
        let input : Vec<Vec<CommonValue>> = Vec::with_capacity(10);

        for query in self.cfg.chain.iter() {
            let mut db_conn = match get_db_conn_from_pool(query.connection.as_str()) {
                Ok(ok) => Ok(ok),
                Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                    Some(e)).as_error()
            }?;


            let conn = db_conn.get_value();
            let output_ret : Option<Result<CommonSqlExecuteResultSet, Box<dyn Error>>> = None;

            if input.len() <= 0 {
                let output = match conn.execute(query.query.as_str(), &[]) {
                    Ok(ok) => Ok(ok),
                    Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                        Some(e)).as_error()
                }?;
            }
            else {
                for param in input {
                    let output = match conn.execute(query.query.as_str(), param.as_slice()) {
                        Ok(ok) => Ok(ok),
                        Err(e) => create_error(COMMON_ERROR_CATEGORY, API_CALL_ERROR, "".to_string(),
                            Some(e)).as_error()
                    }?;
                }
            }


            
        }




        todo!()
    }
}