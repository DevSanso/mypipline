use std::fs;
use clap::Parser;
use std::path::PathBuf;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use mypip_types::config::app::AppConfig;

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

pub(crate) fn load_app_config(base_dir : &'_ str) -> Result<AppConfig, CommonError> {
    let conf_path = PathBuf::from(base_dir).join("config").join("app.toml");
    let data = fs::read_to_string(conf_path).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
    })?;

    let convert = toml::from_str::<AppConfig>(&data).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::ParsingFail, e.to_string())
    })?;

    Ok(convert)
}