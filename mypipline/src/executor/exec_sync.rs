use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::error::Error;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonErrorList;

use crate::executor::types::ThreadState;

pub(super) struct ExecutorStateMap<T : Clone> {
    map : RwLock<HashMap<String, T>>,
}

impl<T : Clone> ExecutorStateMap<T> {
    pub fn new() -> Arc<Self> {
        Arc::new(ExecutorStateMap {map : RwLock::new(HashMap::new())})
    }
    
    pub fn set(self : &Arc<Self>, name : &String, state : T) -> Result<(), impl Error> {
        let mut writer = self.map.write()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;
        
        writer.insert(name.clone(), state);
        Ok::<(), CommonError>(())
    }
    
    pub fn exist(self : &Arc<Self>, name: &String) -> Result<bool, impl Error> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;
        
        Ok::<bool, CommonError>(reader.contains_key(name))
    }

    pub fn not_exists(self : &Arc<Self>, names: &'_ mut dyn Iterator<Item=&String>) -> Result<Vec<String>, impl Error> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;
        
        let mut ret = Vec::with_capacity(5);
        for name in names.next() {
            if !reader.contains_key(name) {
                ret.push(name.clone());
            }
        }
        
        Ok::<Vec<String>, CommonError>(ret)
    }
    
    pub fn delete(self : &Arc<Self>, name : &String) -> Result<(), impl Error> {
        let mut writer = self.map.write()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;
        
        writer.remove(name);
        Ok::<(), CommonError>(())
    }

    pub fn get(self : &Arc<Self>, name : &String) -> Result<Option<T>, impl Error> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        reader.get(name).map_or(Ok::<std::option::Option<T>, CommonError>(None) ,|x| {
            Ok(Some(x.clone()))
        })
    }
}


