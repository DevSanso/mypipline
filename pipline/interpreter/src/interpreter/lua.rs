use std::collections::HashMap;
use std::ffi::c_double;
use std::sync::RwLock;
use std::sync::Arc;
use mlua::prelude::{Lua, LuaResult};

use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;
use mlua::{Error, Table, UserData};
use mypip_types::interface::GlobalLayout;

const INJECT_GLOBAL_NAME : &'static str = "__inject_global_ptr";
const CONN_GET_FN_NAME : &'static str = "mypip_data_conn_get";

struct LuaInterpreterGlobalInject {
    global_ref : &'static dyn GlobalLayout,
}

impl UserData for LuaInterpreterGlobalInject {

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
                let ls = vm.create_string(s.as_bytes())?;
                mlua::Value::String(ls)
            }
            PairValueEnum::Bin(bin) => {
                let ls = vm.create_string(bin.as_slice())?;
                mlua::Value::String(ls)
            }
            PairValueEnum::Bool(b) => {mlua::Value::Boolean(b)}
            PairValueEnum::Float(f) => {mlua::Value::Number(f as f64)}
            PairValueEnum::Array(a) => {
                let table = vm.create_table()?;
                for e in a {
                    table.push(Self::convert_pair_to_lua_type(vm, e)?)?;
                }
                mlua::Value::Table(table)
            }
            PairValueEnum::Map(m) => {
                let table = vm.create_table()?;
                for (k, v) in m {
                    let conv = Self::convert_pair_to_lua_type(vm, v)?;
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

        let lua_args_len = args.len().map_err(|e| e)?;
        let mut real_args = Vec::with_capacity(lua_args_len.cast_unsigned() as usize);
        for idx in 0..lua_args_len {
            let data : String = args.get(idx).map_err(|e| {e})?;
            real_args.push(PairValueEnum::String(data));
        }

        let pool_get_ret = unsafe {
            global.get_exec_pool(conn_name.into())
        }.map_err(|e| {
            match e.func_ref()[0].3.name() {
                "CommonDefaultErrorKind::NoData" => Error::BadArgument {
                    to: Some("get_exec_pool".to_string()),
                    pos: 0,
                    name: Some("conn_name".to_string()),
                    cause: Arc::new(Error::RuntimeError("LuaInterpreter".to_string())),
                },
                _ => Error::RuntimeError(e.get_cause()),
            }
        })?;

        let mut item = pool_get_ret.get_owned(()).map_err(|e| {
            Error::RuntimeError(e.get_cause())
        })?;

        let conn =item.get_value();
        let conn_ret = conn.execute_pair(cmd.as_ref(), &PairValueEnum::Array(real_args)).map_err(|e| {
            Error::RuntimeError(e.get_cause())
        });

        let conn_data = if conn_ret.is_err() {
            item.dispose();
            Err(conn_ret.err().unwrap())
        } else {
            Ok(conn_ret.unwrap())
        }?;

        Self::convert_pair_to_lua_type(vm, conn_data)
    }

    pub fn new(global : &'static dyn GlobalLayout) -> Result<Self,CommonError> {
        let lua_vm = Lua::new();
        let inject_global = lua_vm.create_userdata(LuaInterpreterGlobalInject {
            global_ref : global
        }).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper global init failed")
        })?;
        let inject_fn = lua_vm.create_function(Self::lua_exec_conn_wrapper).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper init failed")
        })?;
        lua_vm.globals().set(INJECT_GLOBAL_NAME, inject_global).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "data_conn_get global set failed")
        })?;
        lua_vm.globals().set(CONN_GET_FN_NAME, inject_fn).map_err(|e| {
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
        self.lua.gc_collect().map_err(|e| {
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