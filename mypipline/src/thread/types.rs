use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct PlanThreadSignal {
    kill : AtomicBool,
}

pub type PlanThreadSignalMap = Arc<RwLock<HashMap<String, PlanThreadSignal>>>;

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