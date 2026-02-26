use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(long)]
    pub base_dir : String,
    #[clap(long)]
    pub identifier : String,
    #[clap(long, default_value_t = true)]
    pub once_conf_load  : bool,
    #[clap(long, default_value = "file")]
    pub loader_type     : String
}

pub fn parsing() -> Args {
    Args::parse()
}