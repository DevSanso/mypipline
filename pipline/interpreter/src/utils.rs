use std::io::Read;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;
use mypip_types::interface::GlobalLayout;

pub(crate) trait ConvertInterpreterParam<T> {
    fn convert(&self, param : &'_ PairValueEnum) -> Result<T, CommonError>;
}

pub(crate) trait ConvertPairValue<T> {
    fn convert(&self, param : &'_ T) -> Result<PairValueEnum, CommonError>;
}

pub(crate) fn exec_pair_conn<T, R>(global : &'static dyn GlobalLayout,
                                                      conn_name : &'_ str,
                                                      query : &'_ str,
                                                      param : T,
                                                      script_converter : impl ConvertInterpreterParam<R>,
                                                      pair_converter : impl ConvertPairValue<T>) -> Result<R, CommonError> {
    let real_args = pair_converter.convert(&param).map_err(|e| {
       CommonError::extend(&CommonDefaultErrorKind::Etc, "pair converter failed", e)
    })?;

    let pool_get_ret = unsafe {
        global.get_exec_pool(conn_name.into())
    }.map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "get exec pool failed", e)
    })?;

    let mut item = pool_get_ret.get_owned(()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "get exec pool item failed", e)
    })?;

    let conn =item.get_value();

    let conn_ret = conn.execute_pair(query, &real_args);

    let conn_data = if conn_ret.is_err() {
        item.dispose();
        return CommonError::extend(&CommonDefaultErrorKind::ExecuteFail,
                                   "execute failed", conn_ret.err().unwrap()).to_result();
    } else {
        item.restoration();
        Ok(conn_ret.unwrap())
    }?;

    script_converter.convert(&conn_data).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "script converter failed", e)
    })
}