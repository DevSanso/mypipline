use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use common_rs::c_core::collection::pool::ThreadSafePool;
use crate::config::plan::{Plan, PlanRoot};
use common_rs::exec::interfaces::pair::PairExecutorPool;
use common_rs::c_err::CommonError;
use common_rs::init::InitConfig;
use crate::config::conn::ConnectionInfos;
use crate::typealias::InterpreterPool;
use crate::config::app::AppConfig;

pub trait ConfLoader : Send + Sync {
    fn load_plan(&self) -> Result<PlanRoot, CommonError>;
    fn load_connection(&self) -> Result<ConnectionInfos, CommonError>;

    fn load_script_data(&self) -> Result<HashMap<String, String>, CommonError>;
    fn load_app_config(&self) -> Result<AppConfig, CommonError>;
}

pub trait Interpreter {
    fn gc(&self)  -> Result<(),CommonError>;
    fn run(&self, name : &'_ str) -> Result<(),CommonError>;
}

pub trait GlobalLayout {
    fn get_exec_pool(&self, name : Cow<'_, str>) -> Result<PairExecutorPool, CommonError >;
    fn get_plan(&self) -> Result<HashMap<String, Plan>, CommonError>;
    fn get_interpreter_pool(&self, name : Cow<'_, str>) -> Result<InterpreterPool, CommonError>;
    fn close(&self) -> Result<(), CommonError>;
    fn reset(&self) -> Result<(), CommonError>;
    fn initialize(&self, identifier : String, base_dir : String) -> Result<(), CommonError>;
    fn get_script_data(&self, name : &'_ str) -> Result<String, CommonError>;
}