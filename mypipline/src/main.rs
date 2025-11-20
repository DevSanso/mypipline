use serde::{Deserialize, Serialize};

use common_rs::c_err::{CommonError, gen::CommonDefaultErrorKind};
use common_rs::init::InitConfig;
use crate::loader::ConfLoader;

mod loader;
mod args;
mod types;
mod constant;
mod interpreter;
mod global;
mod thread;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppConfig {
    pub loader_type : String,
    pub file_loader_root_path : String,
}

fn load_app_conf(proc_args : &args::Args) -> Result<AppConfig, impl std::error::Error> {
    let config_data = std::fs::read_to_string(proc_args.config.as_path())
        .map_err(|e| CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, e.to_string()))?;
    
    let ret = toml::from_str(config_data.as_str())
        .map_err(|e| CommonError::new(&CommonDefaultErrorKind::ParsingFail, e.to_string()));
    ret
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proc_args = args::parsing();
    let log_file = proc_args.log_file.clone();
    
    common_rs::init::init_common(InitConfig {
        log_level: (&proc_args).log_level.as_str(),
        log_file: Some(log_file.unwrap().as_str()),
    })?;
    
    let app_config = load_app_conf(&proc_args)?;
    
    let conf_loader = loader::toml_file_loader
        ::TomlFileConfLoader::new(app_config.file_loader_root_path, true);
    
    let loader : Box<dyn ConfLoader> = Box::new(conf_loader);
    global::GLOBAL.initialize(loader.as_ref())?;
    
    
    
    Ok(())
}