use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;

use pyo3::{Bound, BoundObject, Py, PyAny, Python};
use pyo3::prelude::{PyAnyMethods, PyListMethods, PyTypeMethods};
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PyString, PyType};

pub(super) struct PyPariConverter<'py> {
    pub py: Python<'py>,
}

impl<'py> PyPariConverter<'py> {
    fn get_element_type(b : &'_ Bound<PyAny>) -> Result<String, CommonError> {
        let t = b.get_type();
        let type_str = t.name().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
        })?;

        Ok(type_str.as_borrowed().to_string())
    }
}

impl<'a> crate::utils::ConvertPairValue<Bound<'a, PyAny>> for PyPariConverter<'a> {


    fn convert(&self, param: &'_ Bound<'a, PyAny>) -> Result<PairValueEnum, CommonError> {
        let list : &Bound<PyList> = param.cast().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
        })?;

        let mut v = Vec::with_capacity(list.len());

        for ele in list.iter() {
            let type_name = Self::get_element_type(&ele).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::Etc, "get_element_type failed", e)
            })?;

            let data = match type_name.as_str() {
                "str" => {
                    let s : String = ele.extract::<String>().map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                    })?;
                    PairValueEnum::String(s)
                },
                "float" => {
                    let f : f64 = ele.extract::<f64>().map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                    })?;
                    PairValueEnum::Double(f)
                },
                "int" => {
                    let i : i64 = ele.extract::<i64>().map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                    })?;
                    PairValueEnum::BigInt(i)
                },
                "bool" => {
                    let b : bool = ele.extract::<bool>().map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                    })?;
                    PairValueEnum::Bool(b)
                },
                _ => {
                    return CommonError::new(&CommonDefaultErrorKind::NoSupport,format!("not convert type {}", type_name.as_str())).to_result();
                }
            };
            v.push(data);
        }

        Ok(PairValueEnum::Array(v))
    }
}

pub(super) struct PyScriptConverter<'a> {
    pub py : Python<'a>
}

impl<'a> crate::utils::ConvertInterpreterParam<Bound<'a, PyAny>> for PyScriptConverter<'a> {
    fn convert(&self, param: &'_ PairValueEnum) -> Result<Bound<'a, PyAny>, CommonError> {
        let d: Bound<'a, PyAny> = match param {
            PairValueEnum::Double(d) => {
                PyFloat::new(self.py, *d).into_any()
            }
            PairValueEnum::Int(i) => {PyInt::new(self.py, *i).into_any()}
            PairValueEnum::BigInt(bi) => {PyInt::new(self.py, *bi).into_any()}
            PairValueEnum::String(s) => {
                PyString::new(self.py, s.as_str()).into_any()
            }
            PairValueEnum::Bin(bin) => {
                PyBytes::new(self.py, bin.as_slice()).into_any()
            }
            PairValueEnum::Bool(b) => {PyBool::new(self.py, *b).into_bound().into_any()}
            PairValueEnum::Float(f) => {PyFloat::new(self.py, *f as f64).into_any()}
            PairValueEnum::Array(a) => {
                let temp : Vec<Bound<'a, PyAny>> = Vec::new();
                let list = PyList::new(self.py,temp).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                })?;
                for e in a {
                    list.append(self.convert(e).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::Etc, "nested failed", e)
                    })?).map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::Etc, format!("table push failed :{}", e))
                    })?;
                }
                list.into_any()
            }
            PairValueEnum::Map(m) => {
                let dict = PyDict::new(self.py);

                for (k, v) in m {
                    let conv = self.convert(v).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::Etc, "nested failed", e)
                    })?;
                    dict.set_item(k.clone(), conv).map_err(|e| {
                        CommonError::new(&CommonDefaultErrorKind::Etc, format!("table push failed :{}", e))
                    })?;
                }
                dict.into_any()
            }
            PairValueEnum::Null => {
                let none: Bound<'a, PyNone> = PyNone::get(self.py).into_bound();
                none.into_any()
            }
        };
        Ok(d)
    }
}