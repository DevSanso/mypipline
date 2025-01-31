pub mod collection;
pub mod err;
pub mod init;

pub mod logger {
    pub use log::debug;
    pub use log::error;
    pub use log::info;
    pub use log::trace;

    use crate::init;

    pub fn get_is_trace_level() -> bool {
        unsafe {
            return init::logger::LOGGER_FILE_LEVEL_IS_TRACE;
        }
    }
}

pub mod parser {
    pub mod serde {
        pub mod toml {
            pub use toml::*;
        }
        
        pub mod serde_json {
            pub use serde_json::*;
        }
    }
}
