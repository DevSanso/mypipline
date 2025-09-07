mod exec;

use std::error::Error;

pub trait Executor {
    fn run(self) -> Result<(), Box<dyn Error>>;
}

pub(self) trait ExecutorPrivate {
    
}

