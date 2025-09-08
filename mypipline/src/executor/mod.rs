mod exec;

use std::{error::Error, sync::Arc};

use crate::map::{DbConnPool, PlanPool};

pub type ExecutorCallbackFn = dyn Fn() -> Result<bool, Box<dyn Error>>;
pub trait Executor {
    fn run(self) -> Result<impl ExecutorHandle, Box<dyn Error>>;
}

pub trait ExecutorHandle {
    fn stop_wait(self);
}

pub fn new_exector(p :PlanPool, db_m : &Arc<DbConnPool> , before : &'static ExecutorCallbackFn, after : &'static ExecutorCallbackFn) -> impl Executor {
    let e = exec::ExecutorImpl::new(p, db_m, before, after);
    e
}
