use std::fs;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::init::{InitConfig, LoggerConf};

use log;
use simplelog;

use simplelog::*;

use std::fs::File;
use mypip_loader::toml_file_loader;
use mypip_types::interface::*;
use mypip_global::GLOBAL;
use mypip_thread::PlanThreadExecutor;

use common_rs::logger::log_info;
use mypip_types::config::app::{AppConfig, AppLogConfig};

fn load_app_config(base_dir : &'_ str) -> Result<AppConfig, CommonError> {
    let conf_path = PathBuf::from(base_dir).join("config").join("app.toml");
    let data = fs::read_to_string(conf_path).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
    })?;

    let convert = toml::from_str::<AppConfig>(&data).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::ParsingFail, e.to_string())
    })?;

    Ok(convert)
}
#[test]
fn test_main() -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = env!("CARGO_MANIFEST_DIR").to_owned() + "/tests/assets";
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Trace, Config::default(), TerminalMode::Mixed, ColorChoice::Auto),
        ]
    )?;

    GLOBAL.initialize("test".to_string(), base_dir.clone(), "db_toml".to_string(), false, load_app_config(base_dir.as_str())?)?;

    let mut cancel = PlanThreadExecutor::daemon();

    loop {
        if common_rs::signal::is_set_signal(common_rs::signal::SIGINT) {
            log_info!("test_main", "stop main loop");
            cancel.cancel();
            log_info!("test_main", "stop daemon thread");
            break;
        }

        std::thread::sleep(std::time::Duration::from_secs(10));
    }

    GLOBAL.close()?;
    Ok(())
}