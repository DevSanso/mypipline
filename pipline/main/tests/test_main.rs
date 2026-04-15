use serde::{Deserialize, Serialize};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::init::{InitConfig, LoggerConf};

use mypip_loader::toml_file_loader;
use mypip_types::interface::*;
use mypip_global::GLOBAL;
use mypip_thread::PlanThreadExecutor;

use common_rs::logger::log_info;
use mypip_types::config::app::{AppConfig, AppLogConfig};

#[test]
fn test_main() -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = env!("CARGO_MANIFEST_DIR").to_owned() + "/tests/assets";
    GLOBAL.initialize("test".to_string(), base_dir, "file".to_string(), true, AppConfig {
        log_conf: AppLogConfig {
            log_type: "console".to_string(),
            log_level: "trace".to_string(),
            log_file_size_mb: None,
            log_scylla_config: None,
        },
        script_lib: None,
        db_config: None,
    })?;

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