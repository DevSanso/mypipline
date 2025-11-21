use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use common_rs::c_core::func;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::logger::{log_debug, log_error};
use crate::global::GLOBAL;
use crate::types::config::plan::{Plan, PlanInterval};

pub(super) struct PlanThreadEntry {
    name : String,
    plan : Plan,
    run_state : Arc<RwLock<HashSet<String>>>,

    signal   : Arc<crate::thread::types::PlanThreadSignal>
}

fn get_plan_next_sleep_time_millie(conn_name : String) -> Result<u128, CommonError> {
    if conn_name == "" {
        let since = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        return Ok(since.as_millis());
    }

    let p = GLOBAL.get_exec_pool(conn_name.as_str()).map_err(|e| {
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

fn plan_thread_sleep(interval : &PlanInterval) -> Result<(), CommonError> {
    let millie = get_plan_next_sleep_time_millie(interval.connection.clone()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "failed get current interval", e)
    });
    let millie_ok = millie.unwrap();
    let interval_ms = (interval.second as u128) * 1000;
    let epel = interval_ms - (millie_ok % interval_ms + 1);
    if epel <= 0 {
        std::thread::sleep(Duration::from_millis(interval_ms as u64 + 10));
    } else {
        std::thread::sleep(Duration::from_millis(interval_ms as u64 + 10));
    }

    Ok(())
}

pub fn plan_thread_fn(entry : PlanThreadEntry) {
    let sig = entry.signal;
    loop {
        if sig.get_kill() {
            log_debug!("{} - {} chk kill signal", func!(), entry.name);
            break;
        }

        let sleep = plan_thread_sleep(&entry.plan.interval);
        if sleep.is_err() {
            log_error!("{}", sleep.err().unwrap());
            break;
        }



    }
}

impl PlanThreadEntry {

}