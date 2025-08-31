mod exec;

use std::error::Error;

pub trait Executor {
    fn run(&mut self) -> Result<(), Box<dyn Error>>;
}

pub(self) trait ExecutorPrivate {
    
}

