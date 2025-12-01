use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Keys;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};
use common_rs::c_core::func;
use common_rs::logger::*;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::th::simple::{new_simple_thread_manager, SimpleManagerKind, SimpleThreadManager};
use crate::global::GLOBAL;
use crate::thread::plan_thread::{plan_thread_fn, PlanThreadEntry};
use crate::thread::types::{PlanThreadSignal, PlanThreadStateRunSet};
use crate::types::config::plan::PlanRoot;

pub struct PlanThreadExecutorCancel {
    exec : Arc<PlanThreadExecutor>,
    join_handle: Option<JoinHandle<()>>
}
pub struct PlanThreadExecutor {
    manager  : Arc<dyn SimpleThreadManager<PlanThreadEntry> + Send + Sync>,
    run_state : Arc<PlanThreadStateRunSet>,

    signal_map : PlanThreadSignalMap,
    stop_flag : AtomicBool
}
pub struct PlanThreadSignalMap {
    map : RwLock<HashMap<String, Arc<PlanThreadSignal>>>
}

impl PlanThreadExecutorCancel {
    pub fn cancel(&mut self) {
        self.exec.stop_flag.store(true, Ordering::Release);
        let take = self.join_handle.take();
        take.unwrap().join().unwrap();
    }
}

impl Drop for PlanThreadExecutorCancel {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl PlanThreadSignalMap {
    pub fn new() -> Self {
        PlanThreadSignalMap { map: RwLock::new(HashMap::new()) }
    }

    pub fn get(&self, name : &'_ str) -> Result<Arc<PlanThreadSignal>, CommonError> {
        let reader = self.map.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Ok(reader.get(name).cloned().unwrap())
    }

    pub fn create(&self, name : &'_ str) -> Result<Arc<PlanThreadSignal>, CommonError> {
        let mut writer = self.map.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;
        if writer.contains_key(name) {
            return CommonError::new(&CommonDefaultErrorKind::InitFailed, format!("exists key {}", name)).to_result();
        }
        let sig = PlanThreadSignal::new();
        writer.insert(name.to_string(), sig.clone());
        Ok(sig)
    }

    pub fn delete(&self, name : &'_ str) -> Result<(), CommonError> {
        let mut writer = self.map.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;
        if writer.contains_key(name) {
            return CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists key {}", name)).to_result();
        }
        writer.remove(name);
        Ok(())
    }
}
impl PlanThreadExecutor {
    fn check_run_interval_plan<V>(&self, ks : Keys<String, V>) -> Result<Vec<String>, CommonError> {
        let mut not_run = Vec::new();

        for k in ks {
            let is_run =  self.run_state
                .exist(k)
                .map_err(|e| CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "", e));
            if is_run.is_err() {
                log_error!("{}", is_run.err().unwrap());
                continue;
            }

            if !is_run? {
                not_run.push(k.clone());
            }
        }

        Ok(not_run)
    }

    fn next_sleep(&self) {
        let current_ms = (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() % 1000) as u64;
        let sleep_time = match (1000 - current_ms) >= 1000 {
            true => 1000 - current_ms,
            false => 999
        };
        log_debug!("{} - sleep millie second {}", func!(), sleep_time);
        std::thread::sleep(Duration::from_millis(sleep_time));
    }
    
    fn start_loop(&self) -> Result<(), CommonError> {
        let mut old_sec = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        
        while !self.stop_flag.load(Ordering::Relaxed) {
            self.next_sleep();
            
            let cur_sec = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
            if cur_sec - old_sec > 60 {
                GLOBAL.reset().map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::Etc, "reset failed global", e)
                })?;
                old_sec = cur_sec;
            }
            
            let plan = GLOBAL.get_plan().map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::Etc, "get failed plan", e)
            })?;

            let run_plan = self.check_run_interval_plan(plan.keys()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::Etc, "get failed interval plan", e)
            })?;

            for p in run_plan {
                log_debug!("{} - try start plan {}", func!(), p);
                let signal = self.signal_map.create(p.as_str()).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::Etc, "create signal failed", e)
                })?;

                let entry = PlanThreadEntry::new(p.clone(), plan[&p].clone(), self.run_state.clone(), signal);

                if let Err(e) = self.manager.execute("".to_string(), &plan_thread_fn, entry) {
                    let log = CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "executor failed", e);
                    log_error!("{}", log);
                }
            }
        }

        Ok(())
    }

    pub fn daemon() -> PlanThreadExecutorCancel {
        let exec = Arc::new(PlanThreadExecutor {
            manager: new_simple_thread_manager(SimpleManagerKind::Pool, 100),
            run_state: PlanThreadStateRunSet::new(),
            signal_map: PlanThreadSignalMap::new(),
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

        PlanThreadExecutorCancel {exec, join_handle: Some(join)}
    }
}