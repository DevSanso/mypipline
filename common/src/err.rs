pub mod define;

use std::error::Error;

#[derive(Debug)]
pub(super) struct InternalChain(pub(crate) Box<dyn Error>,pub(crate) Option<Box<InternalChain>>);

macro_rules! impl_error {
    ($category:ident ,$name : ident, $message:expr, $descr : expr) => {
        #[derive(Debug)]
        pub struct $name(&'static str /* message(description) */,String /* sub message*/, Option<Box<InternalChain>> /* output list*/);

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let _ = write!(f, "cause {} : {}\n", self.0, self.1);

                let mut ptr = &self.2;
                let mut i = 0;
                while ptr.is_some() {
                    let d = ptr.as_ref().unwrap();
                    let _ = write!(f, "    {} at {}\n", i, d.0.to_string());
                    ptr = &d.1;
                    i += 1;
                }

                std::fmt::Result::Ok(())
            }
        }

        impl Error for $name  {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                None
            }
        
            fn description(&self) -> &str {
                self.0
            }
        
            fn cause(&self) -> Option<&dyn Error> {
                self.source()
            }
        }

        impl $name {
            pub fn new(sub_msg : String) -> Box<dyn Error> {
                let ret = $name($message, sub_msg, None);
                Box::new(ret)
            }

            pub fn chain(sub_msg : String, right : Box<dyn Error>) -> Box<dyn Error> {
                let ret = $name($message, sub_msg, Some(
                    Box::new(InternalChain(right, None))
                ));

                Box::new(ret)
            }
        }

    };
}

macro_rules! impl_err_mod {
    ($name:ident, [$((
        $err_name:ident, $message:expr, $descr:expr)),*
    ]) => {
        pub mod $name {
            use std::error::Error;
            use std::fmt::Display;
            use std::fmt::Debug;

            use crate::err::impl_error;
            use crate::err::InternalChain;

            $(impl_error!($name, $err_name, $message, $descr);)*
        }
    }
}

pub(crate) use impl_error;
pub(crate) use impl_err_mod;

#[macro_export]
macro_rules! func {
    () => {
        {
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            let name = type_name_of(f);
            &name[..name.len() - 3]
        }
    };
}
pub use func;

#[macro_export]
macro_rules! make_err_msg {
    ($($arg:tt)+) => {{
        use common::err::func;
        format!("{} [{}:{}] : {}", func!(), file!(), line!(), format!($($arg)+))
    }};
}
pub use make_err_msg;

