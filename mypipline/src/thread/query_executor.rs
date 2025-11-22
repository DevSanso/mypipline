use common_rs::c_err::CommonError;
use crate::types::config::plan::PlanChain;

pub(super) struct QueryExecutor<'a> {
    chain : &'a [PlanChain]
}

impl<'a> QueryExecutor<'a> {
    pub fn run(&self) -> Result<(), CommonError> {
        
        
        
        Ok(())
    }
    
    pub fn new(chain : &'a [PlanChain]) -> Self {
        Self { chain }
    }
}