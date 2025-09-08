mod config;
mod args;
mod plan;
mod executor;
mod init;
mod constant;
mod map;

use std::sync::Arc;

use common_rs::{logger, signal::SIGINT};

use crate::executor::{Executor, ExecutorHandle};

fn execute_befre() -> Result<bool, Box<dyn std::error::Error>> {
    let ret = common_rs::signal::is_set_signal(SIGINT);
    Ok(ret)
}

fn execute_after() -> Result<bool, Box<dyn std::error::Error>> {
    Ok(true)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (proc_args, config_data) = init::get_args_and_config()?;
    let init_ret = init::init_common_lib(&proc_args);

    if init_ret.is_err() {
        logger::error!("main - init process failed");
        logger::error!("main - init error : {}", init_ret.unwrap_err());
    }
    
    let (db_p, plan_p) = init::create_process_maps(config_data)?;
    let db_arc = Arc::new(db_p);
    let exec = executor::new_exector(plan_p, &db_arc, &execute_befre, &execute_after);
    let handle = exec.run()?;

    handle.stop_wait();

    Ok(())
}