use std::path::Path;

use std::fs;
use std::io::Read;

use common::parser::serde::toml;
use common::err::define as err_define;
use common::err::make_err_msg;

use plan::template::PlanTemplate;

pub fn load<P: AsRef<Path>>(filepath : P) -> Result<PlanTemplate, Box<dyn std::error::Error>> {
    let mut f = match fs::File::open(filepath) {
        Ok(ok) => ok,
        Err(e) => return Err(err_define::system::ApiCallError::new(make_err_msg!(
            "{}", e
        )))
    };
    let conf = {
        let mut buf = String::new();
        let read_ret = f.read_to_string(&mut buf);
        if read_ret.is_err() {
            return Err(err_define::system::ApiCallError::new(make_err_msg!("{}", read_ret.unwrap_err().to_string())));
        }
        buf
    };

    match toml::from_str::<PlanTemplate>(conf.as_str()) {
        Ok(ok) => Ok(ok),
        Err(e) => Err(err_define::system::ParsingError::new(make_err_msg!(
            "{}", e
        )))
    }
}

pub fn load_str(conf : &'_ str) -> Result<PlanTemplate, Box<dyn std::error::Error>> {
    match toml::from_str::<PlanTemplate>(conf) {
        Ok(ok) => Ok(ok),
        Err(e) => Err(err_define::system::ParsingError::new(make_err_msg!(
            "{}", e
        )))
    }
}
