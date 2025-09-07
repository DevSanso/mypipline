mod config;
mod args;
mod plan;
mod executor;
mod global;
mod init;
mod constant;
mod map;

use common_rs::logger;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (proc_args, config_data) = init::get_args_and_config()?;
    let init_ret = init::init_common_lib(&proc_args);

    if init_ret.is_err() {
        logger::error!("main - init process failed");
        logger::error!("main - init error : {}", init_ret.unwrap_err());
    }
    
    let (db_p, plan_p) = init::create_process_maps(config_data)?;

    Ok(())
}