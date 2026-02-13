
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use mypip_types::interface::GlobalLayout;

pub fn interpreter_exit()  -> Result<(),CommonError> {
    crate::interpreter::py::PyInterpreterInitialization::shutdown().map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Critical, "", e)
    })?;
    crate::interpreter::lua::LuaInterpreterInitialization::shutdown().map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Critical, "", e)
    })?;
    Ok(())
}
pub fn interpreter_init(global : &'static dyn GlobalLayout) -> Result<(),CommonError> {
    crate::global::GLOBAL_REFER.get_or_init(|| global);
    
    crate::interpreter::py::PyInterpreterInitialization::init().map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Critical, "", e)
    })?;
    crate::interpreter::lua::LuaInterpreterInitialization::init().map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Critical, "", e)
    })?;
    Ok(())
}