use std::collections::hash_map::Keys;
use std::collections::HashMap;
use std::time::Duration;
use std::error::Error;
use std::sync::Arc;
use std::thread;

use common_rs::exec::c_relational_exec::{RelationalValue, RelationalExecutorPool};
use common_rs::exec::c_exec_shell::ShellSplit;

use crate::loader::ConfLoader;
use crate::executor::types::ThreadState;
use crate::types::config::Plan;

mod plan_thread;
mod exec_sync;
mod types;

trait ExecutorPrivate {
    fn signal(&mut self) -> bool;
    fn update(&mut self) -> Result<(), Box<dyn Error>>;
    fn run(&mut self, th_ctl : &'_ thread::Scope) -> Result<(), Box<dyn Error>>;
    fn logging(&mut self) -> Result<(), Box<dyn Error>>;
}

pub trait Executor {
    fn start(&mut self);
}

pub struct ExecutorImpl {
    loader : Box<dyn ConfLoader>,
    states : Arc<exec_sync::ExecutorStateMap<ThreadState>>,

    current_plan : Arc<exec_sync::ExecutorStateMap<Plan>>,
    current_db_conn : Arc<exec_sync::ExecutorStateMap<RelationalExecutorPool<RelationalValue>>>,
    current_shell_conn : Arc<exec_sync::ExecutorStateMap<RelationalExecutorPool<ShellSplit>>>,
}

pub fn executor_create(loader : Box<dyn ConfLoader>) -> Box<dyn Executor> {
    Box::new(ExecutorImpl {
        loader, states : exec_sync::ExecutorStateMap::new(),
        current_plan : exec_sync::ExecutorStateMap::new(),
        current_db_conn : exec_sync::ExecutorStateMap::new(),
        current_shell_conn : exec_sync::ExecutorStateMap::new(),
    })
}

impl ExecutorImpl {
}

impl Executor for ExecutorImpl {
    fn start(&mut self) {
        thread::scope(|s| {
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

                let run_ret = self.run(s);
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
        })
    }
}

impl ExecutorPrivate for ExecutorImpl {
    fn signal(&mut self) -> bool {
        common_rs::signal::is_set_signal(common_rs::signal::SIGABRT) ||
            common_rs::signal::is_set_signal(common_rs::signal::SIGINT)
    }

    fn update(&mut self) -> Result<(), Box<dyn Error>> {
        let plans = self.loader.load_plan()?;
        let conns = self.loader.load_connection()?;

        let mut plan_iter =  plans.plan_list.iter();


        Ok(())
    }

    fn run(&mut self, th_ctl : &'_ thread::Scope) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn logging(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}