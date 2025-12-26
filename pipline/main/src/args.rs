use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    pub config: PathBuf,
    #[arg(short, long)]
    pub log_file : Option<String>,
    #[arg(short, long, default_value="info")]
    pub log_level : String,
}

pub fn parsing() -> Args {
    Args::parse()
}