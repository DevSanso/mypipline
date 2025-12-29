use serde::{Deserialize, Serialize};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::init::InitConfig;

use mypip_loader::toml_file_loader;
use mypip_types::interface::*;
use mypip_global::GLOBAL;
use mypip_thread::PlanThreadExecutor;

use common_rs::logger::log_info;

#[test]
fn test_main() -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = env!("CARGO_MANIFEST_DIR").to_owned() + "/tests/assets";
    GLOBAL.initialize("test".to_string(), base_dir)?;

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