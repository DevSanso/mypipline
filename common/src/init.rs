pub mod logger {
    use std::error::Error;
    use std::sync::Once;

    use ftail::Ftail;
    use log::LevelFilter;

    fn convert_str_to_log_level(log_level : &'_ str) -> LevelFilter {
        match log_level {
            "debug" => LevelFilter::Debug,
            "warn" => LevelFilter::Warn,
            "trace" => LevelFilter::Trace,
            "info" => LevelFilter::Info,
            _ => LevelFilter::Error
        }
    }

    static LOGGER_INIT_ONCE : Once = Once::new();
    static mut LOGGER_INIT_RET : Result<(), Box<dyn Error>> = Ok(());
    pub(crate) static mut LOGGER_FILE_LEVEL_IS_TRACE : bool = false;

    pub fn init_once(log_level : &'_ str, log_file : Option<&'_ str>) -> &'static Result<(), Box<dyn Error>> {
        LOGGER_INIT_ONCE.call_once(|| {
            let level = convert_str_to_log_level(log_level);
            let mut ftail = Ftail::new()
            .console(LevelFilter::Debug);
    
            if log_file.is_some() {
                let file = log_file.unwrap();

                {
                    let chk_write = std::fs::OpenOptions::new().write(true).open(file);
                
                    if chk_write.is_err() {
                        unsafe {
                            LOGGER_INIT_RET = Err(crate::err::define::system::ApiCallError::new(
                                crate::err::make_err_msg_crate!("{}", chk_write.unwrap_err())
                            ));
                        }
                        return;
                    }
                }

                ftail = ftail.single_file(file, true, level);
            }
            
            unsafe {
                if log_file.is_some() {
                    if level == LevelFilter::Trace {
                        LOGGER_FILE_LEVEL_IS_TRACE = true;
                    }
                };
                LOGGER_INIT_RET = match ftail.init() {
                    Ok(_) => Ok(()),
                    Err(e) => Err(crate::err::define::system::ApiCallError::new(
                        crate::err::make_err_msg_crate!("{}", e)
                    ))
                };
            }
        });
        
        unsafe {
            &LOGGER_INIT_RET
        }
    }
}