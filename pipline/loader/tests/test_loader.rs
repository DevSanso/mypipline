use common_rs::c_err::CommonError;
use common_rs::init::InitConfig;
use common_rs::init::LoggerConf;
use mypip_loader::toml_file_loader;
use mypip_types::interface::ConfLoader;


const TOML_FILE_DIR : &'static str = concat!(env!("CARGO_MANIFEST_DIR"),"/tests/assets");

#[test]
fn load_conn_toml_file_loader() -> Result<(), CommonError> {
    common_rs::init::init_common(InitConfig {
        logger_conf: LoggerConf::Console,
    })?;

    let conf_loader = toml_file_loader
    ::TomlFileConfLoader::new(TOML_FILE_DIR.to_string(), TOML_FILE_DIR.to_string(), "test".to_string(), true);

    let loader : Box<dyn ConfLoader> = Box::new(conf_loader);

    loader.load_connection()?;
    loader.load_plan()?;

    Ok(())
}