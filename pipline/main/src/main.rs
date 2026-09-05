mod args;

use serde::{Deserialize, Serialize};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::init::InitConfig;
use common_rs::log_error;
use mypip_loader::toml_file_loader;
use mypip_types::interface::*;
use mypip_global::GLOBAL;
use mypip_thread::PlanThreadExecutor;

use common_rs::logger::log_info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proc_args = args::parsing();
    let app_config = args::load_app_config(proc_args.base_dir.as_str())?;
    
    GLOBAL.initialize(proc_args.identifier, proc_args.base_dir, proc_args.loader_type, proc_args.once_conf_load, app_config)?;

    let mut cancel = PlanThreadExecutor::daemon();

    loop {
        if common_rs::signal::is_set_signal(common_rs::signal::SIGINT) {
            log_info!("main", "SIGINT detected, shutting down gracefully.");
            break;
        }

        let sig_li = common_rs::signal::get_set_signals(&[common_rs::signal::SIGINT, common_rs::signal::SIGTERM]);
        if sig_li.len() > 0 {
            log_error!("main", "process err signal interrupted :{:?}", sig_li);
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(10));
    }

    cancel.cancel();
    GLOBAL.close()?;
    Ok(())
}