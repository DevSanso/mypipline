use std::error::Error;

use common_rs::init::LoggerConfig;

use crate::args;
use crate::config;
use crate::global::{init_global_db_pool, init_global_plan};

pub(crate) fn init_process() -> Result<(), Box<dyn Error>> {
    let proc_args = args::parsing();
    let mut log_file : Option<&str> = None;

        if proc_args.log_file.is_some() {
        log_file = Some(proc_args.log_file.as_ref().unwrap().as_str());
    }

    let logger = LoggerConfig {
        log_level : proc_args.log_level.as_str(),
        log_file : log_file
    };

    common_rs::init::init_common(logger, None)?;
    let cfg = config::parse_toml(proc_args.config)?;
    init_global_db_pool(cfg.connection)?;
    init_global_plan(cfg.plan)?;

    Ok(())
}