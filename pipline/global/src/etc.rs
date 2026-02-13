use std::ops::Deref;
use common_rs::c_core::collection::pool::get_thread_safe_pool;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use mypip_types::interface::Interpreter;
use mypip_types::typealias::InterpreterPool;
use mypip_interpreter::interpreter;

use crate::GLOBAL;

#[derive(Clone)]
pub(crate) enum InterpreterType {
    PYTHON,
    LUA
}

impl ToString for InterpreterType {
    fn to_string(&self) -> String {
        match self {
            InterpreterType::PYTHON => String::from("python"),
            InterpreterType::LUA => String::from("lua")
        }
    }
}

pub fn create_interpreter_pool(typ : InterpreterType, max : usize) -> InterpreterPool {
    let name_type = typ.clone();
    
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn Interpreter>, CommonError>> = (|| {
        let real_fn  = move |_ : ()| {
            let interpreter = match typ {
                InterpreterType::LUA => interpreter::lua::LuaInterpreter::new().map(|i| {
                    Box::new(i) as Box<dyn Interpreter>
                }),
                InterpreterType::PYTHON => interpreter::py::PyInterpreter::new().map(|i| {
                    Box::new(i) as Box<dyn Interpreter>
                })
            };

            let inter = interpreter.map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::Etc, "get failed lua vm", e)
            })?;

            Ok(inter)

        };
        Box::new(real_fn)
    })();

    get_thread_safe_pool(format!("{} interpreter", name_type.to_string()), gen_fn, max)
}