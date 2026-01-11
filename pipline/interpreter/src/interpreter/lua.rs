use std::sync::Arc;
use mlua::prelude::{Lua, LuaResult};

use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;
use mlua::{Error, Table, UserData, Value};
use mypip_types::interface::GlobalLayout;

const INJECT_GLOBAL_NAME : &'static str = "__inject_global_ptr";
const CONN_GET_FN_NAME : &'static str = "mypip_data_conn_get";
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

impl UserData for LuaInterpreterGlobalInject {

}


struct LuaScriptConverter<'a> {
    vm : &'a Lua
}

impl<'a> crate::utils::ConvertInterpreterParam<mlua::Value> for LuaScriptConverter<'a> {
    fn convert(&self, param: &'_ PairValueEnum) -> Result<Value, CommonError> {
        let d = match param {
            PairValueEnum::Double(d) => {mlua::Value::Number(*d)}
            PairValueEnum::Int(i) => {mlua::Value::Number(*i as f64)}
            PairValueEnum::BigInt(bi) => {mlua::Value::Number(*bi as f64)}
            PairValueEnum::String(s) => {
                let ls = self.vm.create_string(s.as_bytes()).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::Etc, format!("convert failed :{}", e))
                })?;
                mlua::Value::String(ls)
            }
            PairValueEnum::Bin(bin) => {
                let ls = self.vm.create_string(bin.as_slice()).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::Etc, format!("convert failed :{}", e))
                })?;
                mlua::Value::String(ls)
            }
            PairValueEnum::Bool(b) => {mlua::Value::Boolean(*b)}
            PairValueEnum::Float(f) => {mlua::Value::Number(*f as f64)}
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
                mlua::Value::Table(table)
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
                mlua::Value::Table(table)
            }
            PairValueEnum::Null => {mlua::Value::NULL}
        };
        Ok(d)
    }
}

struct LuaPairConverter;

impl crate::utils::ConvertPairValue<Table> for LuaPairConverter {
    fn convert(&self, param: &'_ Table) -> Result<PairValueEnum, CommonError> {
        let mut real_args = Vec::with_capacity(5);
        for pair in param.sequence_values::<mlua::Value>() {
            let data = match pair.as_ref().map_err(|e|  {
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

pub struct LuaInterpreter {
    lua : Lua,
    global_ref : &'static dyn GlobalLayout,
}

impl LuaInterpreter {

    fn convert_pair_to_lua_type(vm : &Lua, data : PairValueEnum) -> LuaResult<mlua::Value> {
        let d = match data {
            PairValueEnum::Double(d) => {mlua::Value::Number(d)}
            PairValueEnum::Int(i) => {mlua::Value::Number(i as f64)}
            PairValueEnum::BigInt(bi) => {mlua::Value::Number(bi as f64)}
            PairValueEnum::String(s) => {
                let ls = vm.create_string(s.as_bytes()).map_err(|e| {
                    Error::BadArgument {
                        to: None,
                        pos: 0,
                        name: None,
                        cause: Arc::new(Error::RuntimeError(make_lua_error_message!(e.to_string()))),
                    }
                })?;
                mlua::Value::String(ls)
            }
            PairValueEnum::Bin(bin) => {
                let ls = vm.create_string(bin.as_slice()).map_err(|e| {
                    Error::BadArgument {
                        to: None,
                        pos: 0,
                        name: None,
                        cause: Arc::new(Error::RuntimeError(make_lua_error_message!(e.to_string()))),
                    }
                })?;
                mlua::Value::String(ls)
            }
            PairValueEnum::Bool(b) => {mlua::Value::Boolean(b)}
            PairValueEnum::Float(f) => {mlua::Value::Number(f as f64)}
            PairValueEnum::Array(a) => {
                let table = vm.create_table().map_err(|e| {
                    Error::RuntimeError(make_lua_error_message!(e.to_string()))
                })?;;
                for e in a {
                    table.push(Self::convert_pair_to_lua_type(vm, e).map_err(|e| {
                        Error::BadArgument {
                            to: None,
                            pos: 0,
                            name: None,
                            cause: Arc::new(Error::RuntimeError(make_lua_error_message!(e.to_string()))),
                        }
                    })?)?;
                }
                mlua::Value::Table(table)
            }
            PairValueEnum::Map(m) => {
                let table = vm.create_table()?;
                for (k, v) in m {
                    let conv = Self::convert_pair_to_lua_type(vm, v).map_err(|e| {
                        Error::BadArgument {
                            to: None,
                            pos: 0,
                            name: None,
                            cause: Arc::new(Error::RuntimeError(make_lua_error_message!(e.to_string()))),
                        }
                    })?;
                    table.set(k, conv)?;
                }
                mlua::Value::Table(table)
            }
            PairValueEnum::Null => {mlua::Value::NULL}
        };
        Ok(d)
    }
    
    fn get_script<S : AsRef<str>>(&self, plan_name : S) -> Result<String,CommonError> {
        let ret = self.global_ref.get_script_data(plan_name.as_ref()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get script data failed", e)
        })?;
        
        Ok(ret)
    }

    fn lua_exec_conn_wrapper(vm : &Lua, (conn_name, cmd, args) : (String, String, Table)) -> LuaResult<mlua::Value> {
        let inject: mlua::AnyUserData = vm.globals().get(INJECT_GLOBAL_NAME)?;

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

    pub fn new(global : &'static dyn GlobalLayout) -> Result<Self,CommonError> {
        let lua_vm = Lua::new();
        let inject_global = lua_vm.create_userdata(LuaInterpreterGlobalInject {
            global_ref : global
        }).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper global init failed")
        })?;
        let inject_fn = lua_vm.create_function(Self::lua_exec_conn_wrapper).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper init failed")
        })?;
        lua_vm.globals().set(INJECT_GLOBAL_NAME, inject_global).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "data_conn_get global set failed")
        })?;
        lua_vm.globals().set(CONN_GET_FN_NAME, inject_fn).map_err(|_| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "data_conn_get set failed")
        })?;

        Ok(LuaInterpreter {
            lua: lua_vm,
            global_ref : global
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