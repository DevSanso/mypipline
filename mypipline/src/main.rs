mod config;
mod args;
mod plan;
mod executor;
mod global;
mod init;
mod constant;

use common_rs::logger;

fn main() {
    let init_ret = init::init_process();

    if init_ret.is_err() {
        logger::error!("main - init process failed");
        logger::error!("main - init error : {}", init_ret.unwrap_err());
    }
    
    


}