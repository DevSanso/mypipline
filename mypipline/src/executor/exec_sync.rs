use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::error::Error;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonErrorList;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(super) enum ThreadState {
    RUNNING,
    STOP 
}

pub(super) struct ThreadStateMap {
    map : RwLock<HashMap<String, ThreadState>>,
}

impl ThreadStateMap {
    pub fn new() -> Arc<Self> {
        Arc::new(ThreadStateMap {map : RwLock::new(HashMap::new())})
    }
    
    pub fn set(self : &Arc<Self>, name : String, state : ThreadState) -> Result<(), impl Error> {
        let mut writer = self.map.write()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;
        
        writer.insert(name, state);
        Ok::<(), CommonError>(())
    }

    pub fn get(self : &Arc<Self>, name : String) -> Result<Option<ThreadState>, impl Error> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        reader.get(&name)
            .map_or(Ok::<std::option::Option<ThreadState>, CommonError>(None), 
                    |t| Ok(Some(t.clone())))
    }
}


