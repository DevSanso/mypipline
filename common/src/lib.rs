pub mod collection;
pub mod err;

pub mod logger {
    pub use log::debug;
    pub use log::error;
    pub use log::info;
}

pub mod parser {
    pub mod serde {
        pub mod serde {
            pub use serde::*;
        }

        pub mod toml {
            pub use toml::*;
        }
        
        pub mod serde_json {
            pub use serde_json::*;
        }
    }
}
