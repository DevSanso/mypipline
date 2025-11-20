pub mod toml_file_loader;

use common_rs::c_err::CommonError;
use crate::types::config::plan::*;
use crate::types::config::conn::*;

pub trait ConfLoader {
    fn load_plan(&self) -> Result<PlanRoot, CommonError>;
    fn load_connection(&self) -> Result<ConnectionInfos, CommonError>;
}