
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use std::error::Error;
use std::sync::Arc;
use std::thread;
use common_rs::c_err::{CommonError, CommonErrors};
use common_rs::c_err::gen::CommonErrorList;
use common_rs::exec::c_exec_duckdb::create_duckdb_conn_pool;
use common_rs::exec::c_exec_pg::create_pg_conn_pool;
use common_rs::exec::c_exec_scylla::create_scylla_conn_pool;
use common_rs::logger;
use common_rs::exec::c_relational_exec::{RelationalExecutorInfo, RelationalExecutorPool, RelationalValue};
use common_rs::exec::c_exec_shell::create_shell_conn_pool;
use common_rs::th::simple::{new_simple_thread_manager, SimpleManagerKind, SimpleThreadManager};

use crate::executor::types::ExecutorState;
use crate::executor::types::PlanThreadEntryArgs;
use crate::loader::ConfLoader;
use crate::executor::types::PlanState;
use crate::types::config::{Plan, PlanRoot};
use crate::constant;

mod plan_thread;
mod types;
mod utils;

trait ExecutorPrivate {
    fn signal(&mut self) -> bool;
    fn update(&mut self) -> Result<(), Box<dyn Error>>;
    fn run(&mut self) -> Result<(), Box<dyn Error>>;
    fn logging(&mut self) -> Result<(), Box<dyn Error>>;
}

pub trait Executor {
    fn start(&mut self) -> Result<(), Box<dyn Error>>;
}

pub struct ExecutorImpl {
    loader : Box<dyn ConfLoader>,
    state: Arc<ExecutorState>,
    
    queue : VecDeque<Plan>,
    th_pool : Arc<dyn SimpleThreadManager<PlanThreadEntryArgs>>
}

pub fn executor_create(loader : Box<dyn ConfLoader>) -> Box<dyn Executor> {
    Box::new(ExecutorImpl {
        loader, 
        state: ExecutorState::new(),
        queue : VecDeque::new(),
        th_pool : new_simple_thread_manager(SimpleManagerKind::Pool, 30)
    })
}

impl ExecutorImpl {
    fn init_executor(&mut self) -> Result<(), Box<dyn Error>> {
        let mut errs = CommonErrors::new("ExecutorImpl - init");
        let conns_ret = self.loader.load_connection();

        if conns_ret.is_err() {
            if conns_ret.is_err() { errs.push(conns_ret.unwrap_err()); }

            return errs.to_result()
        }
        
        let conns = conns_ret.unwrap();

        for conn in conns.infos {
            if conn.conn_type == constant::CONN_TYPE_SHELL {
                let p = create_shell_conn_pool(conn.conn_name.clone(), conn.conn_max_size);
                let p_set_ret = self.state.set_shell_conn_pool(&conn.conn_name, p);
                if p_set_ret.is_err() { errs.push(p_set_ret.unwrap_err()); }
            } 
            else {
                let conn_info = RelationalExecutorInfo {
                    addr: format!("{}:{}", conn.conn_db_host, conn.conn_db_port),
                    name: conn.conn_db_name,
                    user: conn.conn_db_user,
                    password: conn.conn_db_passwd,
                    timeout_sec: conn.conn_db_timeout,
                };
                
                let p = match conn.conn_db_type.as_str() {
                    constant::CONN_TYPE_PG => Ok(create_pg_conn_pool(conn.conn_name.clone(), conn_info, conn.conn_max_size)),
                    constant::CONN_TYPE_DUCKDB => Ok(create_duckdb_conn_pool(conn.conn_name.clone(), conn_info, conn.conn_max_size)),
                    constant::CONN_TYPE_SCYLLA => Ok(create_scylla_conn_pool(conn.conn_name.clone(), vec![conn_info], conn.conn_max_size)),
                    _ => Err(CommonError::new(&CommonErrorList::InvalidApiCall, format!("Unknown connection type: {}", conn.conn_type)))
                };
                
                if p.is_err() { errs.push(p.err().unwrap()); }
                    
                else { 
                    let set_ret = self.state.set_db_conn_pool(&conn.conn_name, p.unwrap());
                    if set_ret.is_err() { errs.push(set_ret.unwrap_err()); }
                }
            }
        }
        Ok(())
    }

    fn get_conn_current_time(&mut self, plan_name : &'_ str, conn_name : &String) -> Result<Duration, CommonError> {
        let is_shell = self.state.is_shell_conn(conn_name)?;

        let cur = if is_shell {
            let p = self.state.get_shell_conn_pool(conn_name)?;
            let mut p_item = p.get_owned(()).map_err(
                |e| CommonError::new(&CommonErrorList::Etc, e.to_string()))?;
            
            let c = p_item.get_value();
            let current = c.get_current_time().map_err(|e| 
                CommonError::new(&CommonErrorList::Etc, e.to_string()))?;
            
            current
        }
        else {
            let p = self.state.get_db_conn_pool(conn_name)?;
            let mut p_item = p.get_owned(()).map_err(
                |e| CommonError::new(&CommonErrorList::Etc, e.to_string()))?;

            let c = p_item.get_value();
            let current = c.get_current_time().map_err(|e|
                CommonError::new(&CommonErrorList::Etc, e.to_string()))?;

            current
        };
        Ok(cur)
    }
    
    fn get_plan_state(&self, plan_name : &String) -> Result<PlanState, CommonError> {
        let s = self.state.get_plan_state(plan_name)?;
        Ok(s)
    }
}

impl Executor for ExecutorImpl {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        self.init_executor()?;

        loop {
            if self.signal() {
                logger::log_info!("Executor - is Stop signal");
                break;
            }

            let update_ret = self.update();
            if update_ret.is_err() {
                logger::log_error!("Executor - update failed");
                logger::log_error!("{}", update_ret.err().unwrap());
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            let run_ret = self.run();
            if run_ret.is_err() {
                logger::log_error!("Executor - run failed");
                logger::log_error!("{}", run_ret.err().unwrap());
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            let logging_ret = self.logging();
            if logging_ret.is_err() {
                logger::log_error!("Executor - logging failed");
                logger::log_error!("{}", logging_ret.err().unwrap());
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            thread::sleep(Duration::from_millis(100));
        }
        
        Ok(())
    }
}

impl ExecutorPrivate for ExecutorImpl {
    fn signal(&mut self) -> bool {
        common_rs::signal::is_set_signal(common_rs::signal::SIGABRT) ||
            common_rs::signal::is_set_signal(common_rs::signal::SIGINT)
    }

    fn update(&mut self) -> Result<(), Box<dyn Error>> {
        let root = self.loader.load_plan()?;
        
        for plan in root.plan_list {
            let plan_name = plan.plan_name.clone();
            let cur_ret = self.get_conn_current_time(
                plan_name.as_str(), &plan.root_conn);
            if cur_ret.is_err() {
                logger::log_error!("{}", cur_ret.err().unwrap());
                continue;
            }
            
            let conn_current = cur_ret.unwrap().as_secs();
            if conn_current % plan.root_interval_sec  == 0 {
                let debug_name = plan_name.as_str();
                let debug_interval = plan.root_interval_sec;
                
                self.queue.push_back(plan);
                logger::log_debug!("current running plan : {}, time:[{}/{}]", debug_name, conn_current, debug_interval);
            }
        }
        
        Ok(())
    }

    fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let mut errs = common_rs::c_err::CommonErrors::new("Executor - Run");

        while let Some(p) = self.queue.pop_front() {
            let state_ret = self.get_plan_state(&p.plan_name);
            if state_ret.is_err() {
                errs.push(state_ret.err().unwrap());
                continue;
            }
            
            let state = state_ret.unwrap();
            if state == PlanState::RUNNING {
                continue;
            }
            
            let plan_name = p.plan_name.clone();
            let _ = self.state.set_plan_state(&plan_name, PlanState::RUNNING);
            
            let args = PlanThreadEntryArgs {
                state : self.state.clone(),
                plan : p,
                name: plan_name
            };
            
            let _ = self.th_pool.execute("ExecutorImpl".to_string(), &plan_thread::plan_thread_entry, args);
        }

        if errs.len() > 0 {
            return Err(errs.into());
        } else {
            Ok(())
        }
    }

    fn logging(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}