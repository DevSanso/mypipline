pub mod toml_file_loader;

use std::error::Error;
use common_rs::c_err::CommonError;
use crate::types::config::{PlanRoot, ConnectionInfos};

pub trait ConfLoader {
    fn load_plan(&self) -> Result<PlanRoot, CommonError>;
    fn load_connection(&self) -> Result<ConnectionInfos, CommonError>;
}