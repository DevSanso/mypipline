use std::path::PathBuf;
use std::sync::Arc;
use mlua::prelude::{Lua, LuaResult, LuaTable};

use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;
use mlua::{AnyUserData, Error, Table, UserData, Value};
use mypip_types::interface::GlobalLayout;

macro_rules! make_lua_error_message {
    ($e:expr) => {
        {
            use common_rs::c_core::utils::macros::func;
            format!("{}:{} - {:.256}", func!(), line!(), $e)
        }

    };
}

struct LuaInterpreterGlobalInject {
    global_ref : &'static dyn GlobalLayout,
}
impl UserData for LuaInterpreterGlobalInject {}

struct LuaScriptConverter<'a> {
    vm : &'a Lua
}

impl<'a> crate::utils::ConvertInterpreterParam<Value> for LuaScriptConverter<'a> {
    fn convert(&self, param: &'_ PairValueEnum) -> Result<Value, CommonError> {
        let d = match param {
            PairValueEnum::Double(d) => {Value::Number(*d)}
            PairValueEnum::Int(i) => {Value::Number(*i as f64)}
            PairValueEnum::BigInt(bi) => {Value::Number(*bi as f64)}
            PairValueEnum::String(s) => {
                let ls = self.vm.create_string(s.as_bytes()).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::Etc, format!("convert failed :{}", e))
                })?;
                Value::String(ls)
            }
            PairValueEnum::Bin(bin) => {
                let ls = self.vm.create_string(bin.as_slice()).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::Etc, format!("convert failed :{}", e))
                })?;
                Value::String(ls)
            }
            PairValueEnum::Bool(b) => {Value::Boolean(*b)}
            PairValueEnum::Float(f) => {Value::Number(*f as f64)}
            PairValueEnum::Array(a) => {
                let table = self.vm.create_table().map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::Etc, format!("convert failed :{}", e))
                })?;
                for e in a {
                    table.push(self.convert(e).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::Etc, "nested failed", e)
                    })?).map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::Etc, format!("table push failed :{}", e))
                    })?;
                }
                Value::Table(table)
            }
            PairValueEnum::Map(m) => {
                let table = self.vm.create_table().map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("create table data failed :{}", e))
                })?;
                for (k, v) in m {
                    let conv = self.convert(v).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::Etc, "nested failed", e)
                    })?;
                    table.set(k.clone(), conv).map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::Etc, format!("table push failed :{}", e))
                    })?;
                }
                Value::Table(table)
            }
            PairValueEnum::Null => {Value::NULL}
        };
        Ok(d)
    }
}

struct LuaPairConverter;

impl crate::utils::ConvertPairValue<Table> for LuaPairConverter {
    fn convert(&self, param: &'_ Table) -> Result<PairValueEnum, CommonError> {
        let mut real_args = Vec::with_capacity(5);
        for pair in param.sequence_values::<Value>() {
            let data = match pair.as_ref().map_err(|_|  {
                CommonError::new(&CommonDefaultErrorKind::Etc, "convert lua -> pair match failed")
            })? {
                Value::Nil => PairValueEnum::Null,
                Value::Boolean(b) => PairValueEnum::Bool(*b),
                Value::Integer(i) => PairValueEnum::BigInt(*i),
                Value::Number(n) => PairValueEnum::Double(*n),
                Value::String(s) => PairValueEnum::String(s.to_string_lossy().to_string()),
                _ => return CommonError::new(&CommonDefaultErrorKind::Etc, "convert lua -> pair failed").to_result()
            };
            real_args.push(data);
        }

        Ok(PairValueEnum::Array(real_args))
    }
}
fn lua_exec_pair_conn_wrapper(vm : &Lua, (conn_name, cmd, args) : (String, String, Table)) -> LuaResult<Value> {
    let inject: AnyUserData = vm.globals().get(crate::constant::INJECT_GLOBAL_NAME)?;

    let global = inject.borrow::<LuaInterpreterGlobalInject>()?
        .global_ref;

    let script_convert = LuaScriptConverter {vm};
    let pair_convert = LuaPairConverter;

    let data = crate::utils::exec_pair_conn(global,
                                            conn_name.as_str(),
                                            cmd.as_str(),
                                            args, script_convert, pair_convert);

    if data.is_err() {
        Err(Error::BadArgument {
            to: None,
            pos: 0,
            name: None,
            cause: Arc::new(Error::RuntimeError(make_lua_error_message!(data.err().unwrap()))),
        })
    } else {
        Ok(data.unwrap())
    }
}

pub(crate) struct LuaInterpreterInitialization;
impl LuaInterpreterInitialization {
    pub(crate) fn init() -> Result<(), CommonError> {
        Ok(())
    }
    pub(crate) fn shutdown() -> Result<(), CommonError> {
        Ok(())
    }

}
pub struct LuaInterpreter {
    lua : Lua,
    global_ref : &'static dyn GlobalLayout,
}

impl LuaInterpreter {
    fn get_script<S : AsRef<str>>(&self, plan_name : S) -> Result<String,CommonError> {
        let ret = self.global_ref.get_script_data(plan_name.as_ref()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get script data failed", e)
        })?;
        
        Ok(ret)
    }

    pub fn new() -> Result<Self,CommonError> {
        let lua_vm = Lua::new();

        let g_ref = *crate::global::GLOBAL_REFER.get().expect("broken global refer");

        let script_lib = g_ref.get_script_lib_path().map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::NoData, "", e)
        })?;

        let inject_global = lua_vm.create_userdata(LuaInterpreterGlobalInject {
            global_ref : g_ref
        }).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper global init failed")
        })?;
        let inject_pair_fn = lua_vm.create_function(lua_exec_pair_conn_wrapper).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper init failed")
        })?;
        lua_vm.globals().set(crate::constant::INJECT_GLOBAL_NAME, inject_global).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "data_conn_get global set failed")
        })?;
        lua_vm.globals().set(crate::constant::PAIR_CONN_EXEC_FN_NAME, inject_pair_fn).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "data_conn_get set failed")
        })?;
        let global_package : LuaTable = lua_vm.globals().get("package").map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
        })?;

        let current_path: String = global_package.get("path").map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
        })?;

        let mut new_path_buf = PathBuf::with_capacity(current_path.len() + 10);
        new_path_buf.push(script_lib);
        new_path_buf.push("lua");
        new_path_buf.push("?.lua");

        global_package.set("path", new_path_buf.to_string_lossy().to_string() + ";" + &current_path).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
        })?;

        Ok(LuaInterpreter {
            lua: lua_vm,
            global_ref : g_ref
        })
    }
}

impl crate::Interpreter for LuaInterpreter {
    fn gc(&self) -> Result<(), CommonError> {
        self.lua.gc_collect().map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "gc call failed")
        })
    }

    fn run(&self, name: &'_ str) -> Result<(), CommonError> {
        let script = self.get_script(name)?;
        let chunk = self.lua.load(script);
 
        chunk.exec().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ExecuteFail, format!("execute failed {}, {}", name, e.to_string()))
        })
    }
}