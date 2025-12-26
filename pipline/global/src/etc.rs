use std::ops::Deref;
use common_rs::c_core::collection::pool::get_thread_safe_pool;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use mypip_types::interface::Interpreter;
use mypip_types::typealias::InterpreterPool;
use mypip_interpreter::interpreter;

use crate::GLOBAL;

pub fn create_lua_interpreter_pool(max : usize) -> InterpreterPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn Interpreter>, CommonError>> = (|| {
        let real_fn  = move |_ : ()| {
            let interpreter = interpreter::lua::LuaInterpreter::new(GLOBAL.deref());

            let inter = interpreter.map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::Etc, "get failed lua vm", e)
            })?;

            Ok(Box::new(inter) as Box<dyn Interpreter>)

        };
        Box::new(real_fn)
    })();

    get_thread_safe_pool("lua interpreter".to_string(), gen_fn, max)
}