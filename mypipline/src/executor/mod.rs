
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
use common_rs::exec::c_relational_exec::{RelationalValue, RelationalExecutorPool, RelationalExecutorInfo};
use common_rs::exec::c_exec_shell::{create_shell_conn_pool, ShellSplit};
use common_rs::th::simple::{SimpleThreadManager, new_simple_thread_manager, SimpleManagerKind};
use crate::executor::types::PlanThreadEntryArgs;
use crate::loader::ConfLoader;
use crate::executor::types::PlanState;
use crate::types::config::{Plan, PlanRoot};

mod plan_thread;
mod exec_sync;
mod types;

trait ExecutorPrivate {
    fn signal(&mut self) -> bool;
    fn update(&mut self) -> Result<(), Box<dyn Error>>;
    fn run(&mut self) -> Result<(), Box<dyn Error>>;
    fn logging(&mut self) -> Result<(), Box<dyn Error>>;
}

pub trait Executor {
    fn start(&mut self);
}


struct ExecutorState {
    plan_states : Arc<exec_sync::ExecutorStateMap<PlanState>>,
    db_conn : Arc<exec_sync::ExecutorStateMap<RelationalExecutorPool<RelationalValue>>>,
    shell_conn : Arc<exec_sync::ExecutorStateMap<RelationalExecutorPool<ShellSplit>>>,
    
    /** true : shell, false : db */
    conn_hint : HashMap<String, bool>
}

pub struct ExecutorImpl {
    loader : Box<dyn ConfLoader>,
    state: ExecutorState,
    
    queue : VecDeque<Plan>,
    th_pool : Arc<dyn SimpleThreadManager<PlanThreadEntryArgs>>
}

pub fn executor_create(loader : Box<dyn ConfLoader>) -> Box<dyn Executor> {
    Box::new(ExecutorImpl {
        loader, 
        state: ExecutorState {
            plan_states : exec_sync::ExecutorStateMap::new(),
            db_conn : exec_sync::ExecutorStateMap::new(),
            shell_conn : exec_sync::ExecutorStateMap::new(),
            conn_hint : HashMap::new()
        },
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

            return errs.into();
        }
        
        let conns = conns_ret.unwrap();

        for conn in conns.infos {
            if conn.conn_type == "cmd" {
                let p = create_shell_conn_pool(conn.conn_name.clone(), conn.conn_max_size);
                let p_set_ret = self.state.shell_conn.set(&conn.conn_name, p);
                if p_set_ret.is_err() { errs.push(p_set_ret.unwrap_err()); }

                self.state.conn_hint.insert(conn.conn_name.clone(), true);
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
                    "postgres" => Ok(create_pg_conn_pool(conn.conn_name.clone(), conn_info, conn.conn_max_size)),
                    "duckdb" => Ok(create_duckdb_conn_pool(conn.conn_name.clone(), conn_info, conn.conn_max_size)),
                    "scylla" => Ok(create_scylla_conn_pool(conn.conn_name.clone(), vec![conn_info], conn.conn_max_size)),
                    _ => CommonError::new(&CommonErrorList::InvalidApiCall, format!("Unknown connection type: {}", conn.conn_type)).into()
                };
                
                if p.is_err() { errs.push(p.unwrap_err()); }
                    
                else { 
                    let set_ret = self.state.db_conn.set(&conn.conn_name, p.unwrap());
                    if set_ret.is_err() { errs.push(set_ret.unwrap_err()); }
                    self.state.conn_hint.insert(conn.conn_name.clone(), false);
                }
            }
        }
        Ok(())
    }

    fn get_conn_current_time(&mut self, plan_name : &'_ str, conn_name : &String) -> Result<Duration, CommonError> {
        let is_shell = self.state.conn_hint.get(conn_name);

        if is_shell.is_none() {
            let err = CommonError::new(
                &CommonErrorList::NoData,
                format!("not exists plan_name : {},conn_name : {}", plan_name, conn_name));

            return err.into();
        }
        
        let cur = if is_shell.unwrap() {
            let p = self.state.shell_conn.get(conn_name)?.unwrap();
            let mut p_item = p.get_owned(()).map_err(
                |e| CommonError::new(&CommonErrorList::Etc, e.to_string()))?;
            
            let c = p_item.get_value();
            let current = c.get_current_time().map_err(|e| 
                CommonError::new(&CommonErrorList::Etc, e.to_string()))?;
            
            current
        }
        else {
            let p = self.state.db_conn.get(conn_name)?.unwrap();
            let mut p_item = p.get_owned(()).map_err(
                |e| CommonError::new(&CommonErrorList::Etc, e.to_string()))?;

            let c = p_item.get_value();
            let current = c.get_current_time().map_err(|e|
                CommonError::new(&CommonErrorList::Etc, e.to_string()))?;

            current
        };
        Ok(cur)
    }
}

impl Executor for ExecutorImpl {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        self.init_executor()?;

        loop {
            if self.signal() {
                common_rs::logger::log_info!("Executor - is Stop signal");
                break;
            }

            let update_ret = self.update();
            if update_ret.is_err() {
                common_rs::logger::log_error!("Executor - update failed");
                common_rs::logger::log_error!("{}", update_ret.err().unwrap());
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            let run_ret = self.run();
            if run_ret.is_err() {
                common_rs::logger::log_error!("Executor - run failed");
                common_rs::logger::log_error!("{}", run_ret.err().unwrap());
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            let logging_ret = self.logging();
            if logging_ret.is_err() {
                common_rs::logger::log_error!("Executor - logging failed");
                common_rs::logger::log_error!("{}", logging_ret.err().unwrap());
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
            let cur_ret = self.get_conn_current_time(plan.plan_name.as_str(), &plan.root_conn);
            if cur_ret.is_err() {
                logger::log_error!("{}", cur_ret.err().unwrap());
                continue;
            }
            
            let conn_current = cur_ret.unwrap().as_secs();
            if conn_current % plan.root_interval_sec  == 0 {
                let debug_name = plan.plan_name.as_str();
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
            let state_ret= self.state.get(&p.plan_name);
            if state_ret.is_err() {
                errs.push(state_ret.err().unwrap());
                continue;
            }

            let state_opt = state_ret.unwrap();
            if state_opt.is_none() {
                logger::log_debug!("Executor - plan state is none : {}", p.plan_name.as_str());
                let ret = self
                    .state.plan_states.set(&p.plan_name, PlanState::STOP);
                if ret.is_err() {
                    errs.push(ret.err().unwrap());
                }
            }
            else {
                let state = state_opt.unwrap();
                if state == PlanState::RUNNING {
                    logger::log_debug!("Executor - plan state is running : {}", p.plan_name.as_str());
                    continue;
                }
            }
            let args = PlanThreadEntryArgs {
                state : self.state.plan_states.clone(),
                db_conn: self.state.db_conn.clone(),
                shell_conn: self.state.shell_conn.clone(),
                plan : p,
                name: p.plan_name.clone(),
            };
            
            let _ = self.th_pool.execute("ExecutorImpl", &plan_thread::plan_thread_entry, args);
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