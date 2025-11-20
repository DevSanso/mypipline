use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use common_rs::logger::*;
use common_rs::c_err::CommonError;
use common_rs::th::simple::{new_simple_thread_manager, SimpleManagerKind, SimpleThreadManager};
use crate::types::config::plan::PlanRoot;
use crate::global::GLOBAL;


pub struct PlanThreadExecutorCancel {
    exec : Arc<PlanThreadExecutor>,
    join_handle: JoinHandle<()>
}
pub struct PlanThreadExecutor {
    plan_map : PlanRoot,
    manager  : Arc<dyn SimpleThreadManager<()> + Send + Sync>,


    stop_flag : AtomicBool
}

impl PlanThreadExecutor {
    fn check_run_interval_plan(&self) -> Vec<&String> {
        for (k, plan) in self.plan_map.plan.iter() {
            let is_run = if plan.interval.connection == "" {
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() % plan.interval.second == 0
            } else {
                //GLOBAL.get_exec_pool(plan.interval.connection.as_str());
                false
            };
        }

        todo!()
    }

    fn start_loop(&self) -> Result<(), CommonError> {
        let plan_ks = self.plan_map.plan.keys().collect::<Vec<&String>>();

        while !self.stop_flag.load(Ordering::Relaxed) {

        }

        Ok(())
    }

    pub fn daemon(config : PlanRoot) -> PlanThreadExecutorCancel {
        let exec = Arc::new(PlanThreadExecutor {
            plan_map: config,
            manager: new_simple_thread_manager(SimpleManagerKind::Pool, 100),
            stop_flag: AtomicBool::new(false),
        });
        let daemon_exec = exec.clone();

        let join = std::thread::spawn( move || {
            let stop_ret = daemon_exec.start_loop();
            if stop_ret.is_err() {
                log_error!("{}", stop_ret.err().unwrap());
            }
            log_info!("stop daemon");
        });

        PlanThreadExecutorCancel {exec, join_handle: join}
    }
}