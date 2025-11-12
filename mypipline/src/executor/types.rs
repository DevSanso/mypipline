use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::error::Error;
use std::fmt::Debug;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonErrorList;
use common_rs::exec::c_exec_shell::{ShellParam};
use common_rs::exec::c_relational_exec::{RelationalExecutorPool, RelationalValue};
use crate::types::config::Plan;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(super) enum PlanState {
    RUNNING,
    STOP
}

pub(super) type ShellPoolMap = ExecutorStateMap<RelationalExecutorPool<ShellParam>>;
pub(super) type RDbPoolMap = ExecutorStateMap<RelationalExecutorPool<RelationalValue>>;

pub(super) struct ExecutorStateMap<T : Clone> {
    map : RwLock<HashMap<String, T>>,
}

impl<T : Clone> ExecutorStateMap<T> {
    pub fn new_arc() -> Arc<Self> {
        Arc::new(ExecutorStateMap {map : RwLock::new(HashMap::new())})
    }
    pub fn new() -> Self {
        ExecutorStateMap {map : RwLock::new(HashMap::new())}
    }

    pub fn set<S : AsRef<str>>(self : &Arc<Self>, name : S, state : T) -> Result<(), CommonError> {
        let mut writer = self.map.write()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        writer.insert(name.clone(), state);
        Ok::<(), CommonError>(())
    }

    pub fn exist<S : AsRef<str>>(self : &Arc<Self>, name: S) -> Result<bool, CommonError> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        Ok::<bool, CommonError>(reader.contains_key(name))
    }

    pub fn not_exists<S : AsRef<str>>(self : &Arc<Self>, names: &'_ mut dyn Iterator<Item=S>) -> Result<Vec<String>, impl Error> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        let mut ret = Vec::with_capacity(5);
        for name in names.next() {
            if !reader.contains_key(name.as_ref()) {
                ret.push(name.clone());
            }
        }

        Ok::<Vec<String>, CommonError>(ret)
    }

    pub fn delete<S : AsRef<str>>(self : &Arc<Self>, name : S) -> Result<(), CommonError> {
        let mut writer = self.map.write()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        writer.remove(name);
        Ok::<(), CommonError>(())
    }

    pub fn get<S : AsRef<str>>(self : &Arc<Self>, name : S) -> Result<Option<T>, CommonError> {
        let reader = self.map.read()
            .map_err(|e| {
                CommonError::new(&CommonErrorList::Critical, e.to_string()) })?;

        reader.get(name).map_or(Ok::<std::option::Option<T>, CommonError>(None) ,|x| {
            Ok(Some(x.clone()))
        })
    }
}

pub(super) struct PlanThreadEntryArgs {
    pub state : Arc<ExecutorState>,
    pub plan : Plan,
    pub name : String,
}

pub(super) struct ExecutorState {
    plan_states : ExecutorStateMap<PlanState>,
    db_conn : RDbPoolMap,
    shell_conn : ShellPoolMap,
    /** true : shell, false : db */
    conn_hint : RwLock<HashMap<String, bool>>
}

impl ExecutorState {
    pub fn new() -> Arc<Self> {
        Arc::new(ExecutorState {
            plan_states : ExecutorStateMap::new(),
            db_conn : ExecutorStateMap::new(),
            shell_conn : ExecutorStateMap::new(),
            conn_hint : RwLock::new(HashMap::new())
        })
    }
    pub fn set_plan_state<S : AsRef<str>>(&self, name : S, state : PlanState) -> Result<(), CommonError> {
        self.plan_states.set(name, state)?;
        Ok(())
    }
    pub fn get_plan_state<S : AsRef<str>>(&self, name : S) -> Result<PlanState, CommonError> {
        let p = self.plan_states.get(name.as_ref())?;

        if p.is_none() {
            CommonError::new(&CommonErrorList::NoData, format!("not exists: {}", name)).to_result()
        }
        else {
            Ok(p.unwrap())
        }
    }
    pub fn set_shell_conn_pool<S : AsRef<str>>(&self, name : S, shell_p : RelationalExecutorPool<ShellParam>) -> Result<(), CommonError> {
        let mut writer = self.conn_hint.write().map_err(|e| {
            CommonError::new(&CommonErrorList::InvalidApiCall, format!("Cannot write connection pool: {}", e))
        })?;

        self.shell_conn.set(name.as_ref(), shell_p)?;
        writer.insert(name, true);
        Ok(())
    }
    pub fn set_db_conn_pool<S : AsRef<str>>(&self, name : S, shell_p : RelationalExecutorPool<RelationalValue>) -> Result<(), CommonError> {
        let mut writer = self.conn_hint.write().map_err(|e| {
            CommonError::new(&CommonErrorList::InvalidApiCall, format!("Cannot write connection pool: {}", e))
        })?;

        self.db_conn.set(name.as_ref(), shell_p)?;
        writer.insert(name, false);
        Ok(())
    }
    pub fn get_shell_conn_pool<S : AsRef<str>>(&self, name : S) -> Result<RelationalExecutorPool<ShellParam>, CommonError>{
        let reader = self.conn_hint.read().map_err(|e| {
            CommonError::new(&CommonErrorList::InvalidApiCall, format!("Cannot read connection pool: {}", e))
        })?;

        let is_shell_opt = reader.get(name.as_ref());
        if is_shell_opt.is_none() {
            return CommonError::new(&CommonErrorList::NoData, format!("not exists conn hint : {}", name)).to_result();
        }
        else if is_shell_opt.unwrap() == &true {
            return CommonError::new(&CommonErrorList::NotMatchArgs, format!("is shell comm : {}", name)).to_result();
        }

        let conn_p = self.shell_conn.get(name.as_ref()).expect("get_shell_conn_pool - db_conn get method broken");

        if conn_p.is_none() {
            CommonError::new(&CommonErrorList::NoData, format!("not exists conn : {}", name)).to_result()
        } else {
            Ok(conn_p.unwrap())
        }
    }

    pub fn is_shell_conn<S : AsRef<str>>(&self, name : S) -> Result<bool, CommonError> {
        let reader = self.conn_hint.read().map_err(|e| {
            CommonError::new(&CommonErrorList::InvalidApiCall, format!("Cannot read connection pool: {}", e))
        })?;

        let is_shell_opt = reader.get(name.as_ref());
        if is_shell_opt.is_none() {
            return CommonError::new(&CommonErrorList::NoData, format!("not exists conn hint : {}", name)).to_result();
        }
        else {
            Ok(is_shell_opt.unwrap().clone())
        }
    }
    pub fn get_db_conn_pool<S : AsRef<str>>(&self, name : S) -> Result<RelationalExecutorPool<RelationalValue>, CommonError>{
        let reader = self.conn_hint.read().map_err(|e| {
            CommonError::new(&CommonErrorList::InvalidApiCall, format!("Cannot read connection pool: {}", e))
        })?;

        let is_shell_opt = reader.get(name.as_ref());
        if is_shell_opt.is_none() {
            return CommonError::new(&CommonErrorList::NoData, format!("not exists conn hint : {}", name)).to_result();
        }
        else if is_shell_opt.unwrap() == &false {
            return CommonError::new(&CommonErrorList::NotMatchArgs, format!("is shell comm : {}", name)).to_result();
        }

        let conn_p = self.db_conn.get(name.as_ref()).expect("get_db_conn_pool - db_conn get method broken");

        if conn_p.is_none() {
            CommonError::new(&CommonErrorList::NoData, format!("not exists conn : {}", name)).to_result()
        } else {
            Ok(conn_p.unwrap())
        }
    }
}