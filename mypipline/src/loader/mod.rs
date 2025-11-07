pub mod toml_file_loader;

use std::error::Error;

use crate::types::config::{PlanRoot, ConnectionInfos};

pub trait ConfLoader {
    fn load_plan(&self) -> Result<PlanRoot, Box<dyn Error>>;
    fn load_connection(&self) -> Result<ConnectionInfos, Box<dyn Error>>;
}