mod args;

use serde::{Deserialize, Serialize};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::init::InitConfig;

use mypip_loader::toml_file_loader;
use mypip_types::interface::*;
use mypip_global::GLOBAL;
use mypip_thread::PlanThreadExecutor;

use common_rs::logger::log_info;

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
        log_file_size_mb : proc_args.log_max_size
    })?;

    let app_config = load_app_conf(&proc_args)?;

    let conf_loader = toml_file_loader
    ::TomlFileConfLoader::new(app_config.file_loader_root_path, true);

    let loader : Box<dyn ConfLoader> = Box::new(conf_loader);
    GLOBAL.initialize(loader)?;

    let mut cancel = PlanThreadExecutor::daemon();

    loop {
        if common_rs::signal::is_set_signal(common_rs::signal::SIGINT) {
            log_info!("stop main loop");
            cancel.cancel();
            log_info!("stop daemon thread");
            break;
        }

        std::thread::sleep(std::time::Duration::from_secs(10));
    }

    GLOBAL.close()?;
    Ok(())
}