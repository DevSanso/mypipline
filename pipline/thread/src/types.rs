use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;

pub struct PlanThreadSignal {
    kill : AtomicBool,
}

impl PlanThreadSignal {
    pub fn new() -> Arc<Self> {
        Arc::new(PlanThreadSignal {
            kill : AtomicBool::new(false),
        })
    }

    pub fn set_kill(self : &Arc<Self>) {
        self.kill.store(true, Ordering::SeqCst);
    }

    pub fn get_kill(self : &Arc<Self>) -> bool  {
        self.kill.load(Ordering::SeqCst)
    }
}

pub struct PlanThreadStateRunSet {
    sets : RwLock<HashSet<String>>
}

impl PlanThreadStateRunSet {
    pub fn new() -> Arc<Self> {
        Arc::new(PlanThreadStateRunSet {sets : RwLock::new(HashSet::new())})
    }
    pub fn exist(&self, name : &'_ str) -> Result<bool, CommonError> {
        let reader = self.sets.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Ok(reader.contains(name))
    }

    pub fn create(&self, name : &'_ str) -> Result<(), CommonError> {
        let mut writer = self.sets.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;
        writer.insert(name.to_string());
        Ok(())
    }

    pub fn delete(&self, name : &'_ str) -> Result<(), CommonError> {
        let mut writer = self.sets.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;
        writer.remove(name);
        Ok(())
    }
}