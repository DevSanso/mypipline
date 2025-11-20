mod lua;

use common_rs::c_err::CommonError;

pub trait Interpreter {
    fn load_script_file<S : AsRef<str>>(&self, name : S, filename : S) -> Result<(),CommonError>;
    fn load_script_code<S : AsRef<str>>(&self, name : S, script : S) -> Result<(),CommonError>;
    fn drop_script<S : AsRef<str>>(&self, name : S) -> Result<(),CommonError>;
    fn gc(&self)  -> Result<(),CommonError>;

    fn run<S : AsRef<str>>(&self, name : S) -> Result<(),CommonError>;
}