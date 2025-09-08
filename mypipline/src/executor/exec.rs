use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::sync::Arc;
use std::time::{self, UNIX_EPOCH};

use crate::map::{DbConnPool, PlanPool};
use crate::plan::Plan;

use super::ExecutorCallbackFn;

pub struct ExecutorImpl {
    plans : PlanPool,
    db_map : Arc<DbConnPool>,
    is_stop : AtomicBool,

    before : &'static ExecutorCallbackFn,
    after : &'static ExecutorCallbackFn
}

unsafe impl Send for ExecutorImpl {}
unsafe impl Sync for ExecutorImpl {}

impl ExecutorImpl {
    pub fn new(p :PlanPool, db_m : &Arc<DbConnPool> , before : &'static ExecutorCallbackFn, after : &'static ExecutorCallbackFn) -> Self {
        ExecutorImpl { plans: p, db_map : Arc::clone(db_m), before, after, is_stop : AtomicBool::new(false)}
    }

    fn plan_callback(p_pool : Option<&mut Arc<Plan>>, _ : Option<()>) -> Result<Arc<Plan>, Box<dyn Error>> {
        let p = p_pool.unwrap();

        Ok(Arc::clone(p))
    }

    fn start_entry(self : Arc<Self>) {
        let ks = self.plans.keys();

        loop {
            let down = (self.before)().map_or(false, |x| {x});
            if down {
                self.is_stop.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            let now = time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

            for k in ks.iter() {
                let p = self.plans.call_fn(k, None, &Self::plan_callback).unwrap();

                if now.as_secs() % p.get_interval() as u64 == 0 {
                    let _ = p.execute_plan(&self.db_map);
                } 
            }

            let _ = (self.after)();
        }
    }
}

pub struct ExecutorHandleImpl {
    exec : Arc<ExecutorImpl>
}

impl ExecutorHandleImpl {
    pub fn new(exec : &Arc<ExecutorImpl>) -> impl super::ExecutorHandle {
        ExecutorHandleImpl{exec : Arc::clone(exec)}
    }
}

impl super::ExecutorHandle for ExecutorHandleImpl {
    fn stop_wait(self) {
        loop {
            if self.exec.is_stop.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            thread::sleep(time::Duration::from_secs(1));
        }
    }
}

impl super::Executor for ExecutorImpl {
    fn run(self) -> Result<impl super::ExecutorHandle, Box<dyn std::error::Error>> {
        let arc = Arc::new(self);
        let handle = ExecutorHandleImpl::new(&arc);
        
        thread::spawn(move || {
            arc.start_entry();
        });


        Ok(handle)
    }
}