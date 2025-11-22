pub(self) mod impl_vm;
pub mod pool;

use common_rs::c_err::CommonError;

pub trait Interpreter {
    fn load_script_file(&self, name : String, filename : &'_ str) -> Result<(),CommonError>;
    fn load_script_code(&self, name : String, script : &'_ str) -> Result<(),CommonError>;
    fn drop_script(&self, name : &'_ str) -> Result<(),CommonError>;
    fn gc(&self)  -> Result<(),CommonError>;

    fn run(&self, name : &'_ str) -> Result<(),CommonError>;
}