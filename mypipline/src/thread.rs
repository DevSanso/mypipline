mod plan_thread;
pub(self) mod types;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};
use common_rs::c_core::func;
use common_rs::logger::*;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
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
    run_state : Arc<RwLock<HashSet<String>>>,

    signal_map : types::PlanThreadSignalMap,
    stop_flag : AtomicBool
}

impl PlanThreadExecutor {
    fn check_run_interval_plan(&self) -> Result<Vec<&String>, CommonError> {
        let ks = self.plan_map.plan.keys();
        let reader = self.run_state.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        let mut not_run = Vec::new();
        for k in ks {
            if !reader.contains(k) {
                not_run.push(k);
            }
        }

        Ok(not_run)
    }

    fn start_loop(&self) -> Result<(), CommonError> {
        let plan_ks = self.plan_map.plan.keys().collect::<Vec<&String>>();

        while !self.stop_flag.load(Ordering::Relaxed) {
            let run_plan = self.check_run_interval_plan().map_err(|e| {
               CommonError::extend(&CommonDefaultErrorKind::Etc, "get failed interval plan", e)
            })?;

            for p in run_plan {
                log_debug!("{} - try start plan {}", func!(), p);

            }

            let current_ms = (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() % 1000) as u64;
            let sleep_time = match (1000 - current_ms) >= 1000 {
                true => 1000 - current_ms,
                false => 999
            };
            log_debug!("{} - sleep millie second {}", func!(), sleep_time);
            std::thread::sleep(Duration::from_millis(sleep_time));
        }

        Ok(())
    }

    pub fn daemon(config : PlanRoot) -> PlanThreadExecutorCancel {
        let exec = Arc::new(PlanThreadExecutor {
            plan_map: config,
            manager: new_simple_thread_manager(SimpleManagerKind::Pool, 100),
            run_state: Arc::new(Default::default()),
            signal_map: Arc::new(Default::default()),
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