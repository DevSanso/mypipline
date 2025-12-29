use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use common_rs::c_core::func;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::logger::{log_debug, log_error};
use mypip_global::GLOBAL;
use query::QueryEntry;
use crate::types::PlanThreadStateRunSet;
use mypip_types::config::plan::{Plan, PlanInterval};
use mypip_types::interface::GlobalLayout;
use crate::entry::script::ScriptEntry;

mod query;
mod script;

pub(super) struct PlanThreadEntry {
    name : String,
    plan : Plan,
    run_state : Arc<PlanThreadStateRunSet>,

    signal   : Arc<crate::types::PlanThreadSignal>
}

fn get_plan_next_sleep_time_millie(conn_name_opt : Option<&String>) -> Result<u128, CommonError> {
    if conn_name_opt.is_none() {
        Ok(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::Critical, format!("Time went backwards: {}", e))
        })?.as_millis())
    } else {
        let conn_name = conn_name_opt.unwrap();
        if conn_name == "" {
            let since = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
            })?;

            return Ok(since.as_millis());
        }

        let p = GLOBAL.get_exec_pool(conn_name.into()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::Etc, "failed get pool", e)
        })?;

        let mut item = p.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::Etc, "failed get item", e)
        })?;

        let conn = item.get_value();
        let current = conn.get_current_time().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "failed get current time", e)
        })?;

        Ok(current.as_millis())
    }
}

fn plan_thread_sleep(interval : &PlanInterval) -> Result<(), CommonError> {
    let millie = get_plan_next_sleep_time_millie(interval.connection.as_ref()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "failed get current interval", e)
    });
    let millie_ok = millie?;
    let interval_ms = (interval.second as u128) * 1000;
    let epel = interval_ms - (millie_ok % interval_ms);

    common_rs::logger::log_debug!("plan_thread_sleep sleep {} ms", epel);

    if epel <= 0 {
        std::thread::sleep(Duration::from_millis(interval_ms as u64 + 10));
    } else {
        std::thread::sleep(Duration::from_millis(epel as u64 + 10));
    }

    Ok(())
}

pub fn plan_thread_fn(entry : PlanThreadEntry) {
    let sig = entry.signal.clone();
    log_debug!("{:?} - start plan thread {}", std::thread::current().id(), entry.name);

    loop {
        if sig.get_kill() {
            log_debug!("{} - {} chk kill signal", func!(), entry.name);
            if let Err(w) = entry.run_state.delete(&entry.name) {
                let panic_msg = CommonError::extend(&CommonDefaultErrorKind::Critical, "thread state b", w);
                log_error!("panic");
                log_error!("{}", panic_msg);
                panic!("{}", panic_msg);
            }
            break;
        }

        let sleep = plan_thread_sleep(&entry.plan.interval);
        if sleep.is_err() {
            log_error!("{}", sleep.err().unwrap());
            break;
        }

        if  let Err(e) = entry.run() {
            log_error!("{}", e.to_string());
            break;
        }
    }
}

impl PlanThreadEntry {
    pub fn new(name : String, plan : Plan, run_state :  Arc<PlanThreadStateRunSet>, signal :  Arc<crate::types::PlanThreadSignal>) -> Self {
        PlanThreadEntry {
            name,
            plan,
            run_state,
            signal,
        }
    }
    fn run(&self) -> Result<(), CommonError> {
        match self.plan.type_name.as_str() {
            crate::constant::PLAN_TYPE_SCRIPT => self.run_script(),
            _ => self.run_query()
        }
    }
    fn run_script(&self) -> Result<(), CommonError> {
        let info = self.plan.script.clone().map_or(Err(
            CommonError::new(&CommonDefaultErrorKind::NoData, "not exists data")
        ), |x| {
            Ok(x)
        })?;

        let entry = ScriptEntry::new(self.name.clone(), info);
        entry.run().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "run script failed", e)
        })?;
        Ok(())
    }

    fn run_query(&self) -> Result<(), CommonError> {
        let info = self.plan.chain.clone().map_or(Err(
            CommonError::new(&CommonDefaultErrorKind::NoData, "not exists data")
        ), |x| {
            Ok(x)
        })?;

        let exec = QueryEntry::new(self.name.as_str(), info.as_slice());
        exec.run()
    }
}