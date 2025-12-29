use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(long)]
    pub base_dir : String,
    #[clap(long)]
    pub identifier : String,
}

pub fn parsing() -> Args {
    Args::parse()
}