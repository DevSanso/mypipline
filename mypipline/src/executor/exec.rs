use std::error::Error;
use std::thread;
use crate::map::PlanPool;

pub type ExecutorCallbackFn = dyn Fn() -> Result<(), Box<dyn Error>>;

pub struct ExecutorImpl {
    plans : PlanPool,

    before : &'static ExecutorCallbackFn,
    after : &'static ExecutorCallbackFn
}

impl ExecutorImpl {
    pub fn new(p :PlanPool, before : &'static ExecutorCallbackFn, after : &'static ExecutorCallbackFn) -> Self {
        ExecutorImpl { plans: p, before, after}
    }
}

impl super::Executor for ExecutorImpl {
    fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        thread::scope(move |s| {
            (self.before)();

            let ks = self.plans.keys();


            loop {
                
            }

            (self.after)();
        });
        


        todo!()
    }
}