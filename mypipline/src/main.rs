mod plan_thread;

use std::fs::DirEntry;
use std::process;
use std::error::Error;

use clap::Parser;
use common;
use common::init;
use common::err::define as err_def;
use common::err::make_err_msg;

use plan::{make_plans, template::PlanTemplate};
use plan_toml::load as plan_load;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(long = "log_level", default_value_t = String::from("error"))]
    log_level: String,
    #[arg(long = "log_file", default_value_t = String::from(""))]
    log_file : String,
    /// Number of times to greet
    #[arg(long = "plan_dir", default_value_t = String::from("../plan"))]
    plan_dir: String
}

fn load_plans<P : AsRef<std::path::Path>>(dir :P) -> Result<Vec<PlanTemplate>, Box<dyn Error>> {
    let read_dir_vec : Vec<Result<DirEntry, std::io::Error>> = {
        let read_dir = match std::fs::read_dir(dir) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(err_def::system::ApiCallError::new(
                make_err_msg!("{}", e)
            ))
        }?;
        read_dir.collect()
    };

    let mut v = Vec::with_capacity(5);
    
    for file in read_dir_vec.iter() {
        if file.is_err() {
            return Err(err_def::system::ApiCallError::new(make_err_msg!("{}", file.as_ref().unwrap_err())));
        }

        let entry = file.as_ref().unwrap();

        let meta = match entry.metadata() {
            Ok(ok) => Ok(ok),
            Err(_) => Err(err_def::system::ApiCallError::new(make_err_msg!("failed, read file metadata")))
        }?;

        if meta.is_dir() {continue;}

        let file_path = entry.path();

        let ext = file_path.extension();
        if ext.is_none() { continue; }
        else if ext.unwrap() != "toml" { continue; }

        let plan_data = match plan_load(file_path) {
                Ok(ok) => Ok(ok),
            Err(_) => Err(err_def::system::ApiCallError::new(make_err_msg!("failed, read file metadata")))
        }?;

        v.push(plan_data);
        
    }

    common::logger::info!("load_plans - file : {}, plans : {}", read_dir_vec.len(), v.len());
    Ok(v)
}

fn main() {
    let args = Args::parse();

    let log_init_ret = init::logger::init_once(&args.log_level, 
        if args.log_file == "" {None} else {Some(&args.log_file)});

    if log_init_ret.is_err() {
        eprintln!("{}", log_init_ret.as_ref().unwrap_err());
        process::exit(2);
    }

    let plans_ret = load_plans(args.plan_dir);

    if plans_ret.is_err() {
        common::logger::error!("{}", plans_ret.unwrap_err());    
        process::exit(2);
    }

    let plans = plans_ret.unwrap();
    
    println!("Hello, world!");
}
