use common_rs::c_err::CommonError;
use common_rs::exec::interfaces::pair::PairValueEnum;
use mlua::Table;
use pyo3::{Py, PyAny};

pub(super) struct PyPariConverter;

impl crate::utils::ConvertPairValue<Py<PyAny>> for PyPariConverter {
    fn convert(&self, param: &'_ Py<PyAny>) -> Result<PairValueEnum, CommonError> {
        todo!()
    }
}

pub(super) struct PyScriptConverter;

impl crate::utils::ConvertInterpreterParam<Py<PyAny>> for PyScriptConverter {
    fn convert(&self, param: &'_ PairValueEnum) -> Result<Py<PyAny>, CommonError> {
        todo!()
    }
}