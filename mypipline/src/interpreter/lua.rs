use std::collections::HashMap;
use std::ffi::c_double;
use std::sync::RwLock;
use std::sync::Arc;
use mlua::prelude::{Lua, LuaResult};

use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::relational::RelationalValue;
use mlua::{Error, Table};
use crate::global::GLOBAL;
use crate::interpreter::Interpreter;

#[derive(Default)]
struct LuaInterpreterState {
    scripts : HashMap<String, String>,
}
pub struct LuaInterpreter {
    lua : Lua,
    state : RwLock<LuaInterpreterState>,
}

impl LuaInterpreter {
    fn set_script<S : AsRef<str>>(&self, name : S, script : String) -> Result<(),CommonError> {
        let mut writer = self.state.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, "failed get writer lock")
        })?;

        writer.scripts.insert(String::from(name.as_ref()), script);
        Ok(())
    }

    fn delete_script<S : AsRef<str>>(&self, name : S) -> Result<(),CommonError> {
        let mut writer = self.state.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, "failed get writer lock")
        })?;

        writer.scripts.remove(name.as_ref());
        Ok(())
    }

    fn get_script<S : AsRef<str>>(&self, name : S) -> Result<String,CommonError> {
        let reader = self.state.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, "failed get read lock")
        })?;

        reader.scripts.get(name.as_ref()).map_or_else(
            || Err(CommonError::new(&CommonDefaultErrorKind::NoData, "script not found")),
            |s| Ok(s.clone())
        )
    }

    fn lua_exec_conn_wrapper(vm : &Lua, (conn_name, cmd, args) : (String, String, Table)) -> LuaResult<Table> {
        use crate::global::GLOBAL;

        let lua_args_len = args.len().map_err(|e| e)?;
        let mut real_args = Vec::with_capacity(lua_args_len.cast_unsigned() as usize);
        for idx in 0..lua_args_len {
            let data : String = args.get(idx).map_err(|e| {e})?;
            real_args.push(RelationalValue::String(data));
        }

        let pool_get_ret = unsafe {
            GLOBAL.get_exec_pool(conn_name.as_str())
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
        let conn_ret = conn.execute(cmd.as_ref(), real_args.as_slice()).map_err(|e| {
            Error::RuntimeError(e.get_cause())
        });

        let conn_data = if conn_ret.is_err() {
            item.dispose();
            Err(conn_ret.err().unwrap())
        } else {
          Ok(conn_ret.unwrap())
        }?;
        let mut array = vm.create_table()?;

        for row in conn_data.cols_data.iter() {
            let mut convert_row = Vec::with_capacity(row.len());
            for row_data in row.iter() {
                let convert_value = match row_data {
                    RelationalValue::Double(d) => mlua::Value::Number(c_double::from(*d)),
                    RelationalValue::Int(i) => mlua::Value::Number(c_double::from(*i)),
                    RelationalValue::BigInt(i) => mlua::Value::Integer(*i),
                    RelationalValue::String(s) => {
                        let s = vm.create_string(s.as_str().as_bytes())?;
                        mlua::Value::String(s)
                    }
                    RelationalValue::Bin(b) => {
                        let s = vm.create_string(String::from_utf8_lossy(b.as_slice()).to_string().as_str().as_bytes())?;
                        mlua::Value::String(s)
                    },
                    RelationalValue::Bool(b) => mlua::Value::Boolean(*b),
                    RelationalValue::Float(f) => mlua::Value::Number(c_double::from(*f)),
                    RelationalValue::Null => mlua::Value::Nil,
                };

                convert_row.push(convert_value);
            }
            array.push(convert_row)?;
        }
        Ok(array)
    }

    pub fn new() -> Result<Arc<Self>,CommonError> {
        let mut lua_vm = Lua::new();
        let inject_fn = lua_vm.create_function(Self::lua_exec_conn_wrapper).map_err(|e| {
           CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "lua_exec_conn_wrapper init failed")
        })?;

        lua_vm.globals().set("data_conn_get", inject_fn).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "data_conn_get set failed")
        })?;

        Ok(Arc::new(LuaInterpreter {
            lua: lua_vm,
            state: RwLock::new(LuaInterpreterState::default()),
        }))
    }
}

impl Interpreter for LuaInterpreter {
    fn load_script_file<S: AsRef<str>>(&self, name: S, filename: S) -> Result<(), CommonError> {
        let script = std::fs::read_to_string(name.as_ref()).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("read failed {} file", filename.as_ref()))
        })?;

        self.set_script(name, script)
    }

    fn load_script_code<S: AsRef<str>>(&self, name: S, script: S) -> Result<(), CommonError> {
        self.set_script(name, script.as_ref().to_string())
    }

    fn drop_script<S: AsRef<str>>(&self, name: S) -> Result<(), CommonError> {
        self.delete_script(name)
    }

    fn gc(&self) -> Result<(), CommonError> {
        self.lua.gc_collect().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "gc call failed")
        })
    }

    fn run<S: AsRef<str>>(&self, name: S) -> Result<(), CommonError> {
        let script = self.get_script(name.as_ref())?;
        let chunk = self.lua.load(script);

        chunk.exec().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ExecuteFail, format!("execute failed {}, {}", name.as_ref(), e.to_string()))
        })
    }
}