use std::collections::HashMap;
use std::ffi::CString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use pyo3::ffi::c_str;
use pyo3::prelude::{PyAnyMethods, PyBoolMethods, PyDictMethods, PyListMethods, PyModule, PyModuleMethods};
use pyo3::{pyfunction, pymodule, wrap_pyfunction, Bound, CastError, Py, PyAny, PyErr, PyResult, Python};
use pyo3::exceptions::PyRuntimeError;
use pyo3::impl_::pyfunction::WrapPyFunctionArg;
use pyo3::types::{PyBool, PyCode, PyDict, PyList, PyString};
use mypip_types::interface::GlobalLayout;

static PY_INIT_CODE : &'static str = r#"
from concurrent.futures import ThreadPoolExecutor
import uuid
import traceback
import sys

sys.stdout.reconfigure(line_buffering=True)
te_map = {}
te = ThreadPoolExecutor(max_workers=100)
global_map = {}

def __internal_run_eval(code):
    private_run_eval = lambda x : exec(x, global_map, {})
    temp = uuid.uuid4()
    random_uuid = str(temp)
    compile_code = compile(code,'<string>','exec')
    future = te.submit(private_run_eval, compile_code)
    te_map[random_uuid] = future
    return random_uuid

def __internal_await_done(uuid):
    return te_map[uuid].done()

def __internal_get_error_code(uuid):
    error_code = ""
    try:
        te_map[uuid].result()
    except Exception as e:
        error_code = traceback.format_exc()
    finally:
        del te_map[uuid]
    return error_code"#;

#[pyfunction]
#[pyo3(name = "mypip_pair_conn_exec")]
fn py_exec_pair_conn_wrapper(py: Python, conn_name : String, cmd : String, args : Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let script_convert = super::convert::PyScriptConverter {py};
    let pair_convert = super::convert::PyPariConverter{py};

    let data =  crate::utils::exec_pair_conn(*crate::global::GLOBAL_REFER.get().expect("global refer is broken"),
                                             conn_name.as_str(),
                                             cmd.as_str(),
                                             args, script_convert, pair_convert).map_err(|e| {
        PyErr::new::<PyRuntimeError, _>(format!("{:?}", e))
    })?;

    Ok(data.unbind())
}
pub(crate) struct PyInterpreterInitialization;

impl PyInterpreterInitialization {
    pub(crate) fn init() -> Result<(), CommonError> {
        Python::initialize();
        Python::attach(|py| {
            let g = crate::global::GLOBAL_REFER.get().expect("global refer get is broken");

            if let Ok(py_add_path) = g.get_script_lib_path() {
                let sys = py.import("sys");
                if sys.is_err() {
                    let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, sys.err().unwrap().to_string());
                    panic!("{}", panic_err);
                }
                let path_package_ret = sys.unwrap().getattr("path");
                if path_package_ret.is_err() {
                    let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, path_package_ret.err().unwrap().to_string());
                    panic!("{}", panic_err);
                }
                let path_package = path_package_ret.unwrap();
                let path_list_ret :  Result<&Bound<'_, PyList>, CastError<'_, '_>> = path_package.cast::<PyList>();
                if path_list_ret.is_err() {
                    let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, path_list_ret.err().unwrap().to_string());
                    panic!("{}", panic_err);
                }

                let mut py_add_path_buf = PathBuf::new();
                py_add_path_buf.push(py_add_path);
                py_add_path_buf.push("python");

                let set_package_path_ret = path_list_ret.unwrap().insert(0, py_add_path_buf.to_string_lossy().to_string());
                if set_package_path_ret.is_err() {
                    let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, set_package_path_ret.err().unwrap().to_string());
                    panic!("{}", panic_err);
                }
            }

            let cstr = CString::new(PY_INIT_CODE);

            if cstr.is_err() {
                let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, cstr.err().unwrap().to_string());
                panic!("{}", panic_err);
            }

            let run_ret = py.run(cstr.unwrap().as_c_str(), None, None);

            let main_package_ret = py.import("__main__");
            if main_package_ret.is_err() {
                let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, main_package_ret.err().unwrap().to_string());
                panic!("{}", panic_err);
            }
            let main_package = main_package_ret.unwrap();
            let wrap_ret = wrap_pyfunction!(py_exec_pair_conn_wrapper, main_package.clone());

            if wrap_ret.is_err() {
                let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, wrap_ret.err().unwrap().to_string());
                panic!("{}", panic_err);
            }

            let set_fn_ret = main_package.add_function(wrap_ret.unwrap());

            if set_fn_ret.is_err() {
                let panic_err = CommonError::new(&CommonDefaultErrorKind::Critical, set_fn_ret.err().unwrap().to_string());
                panic!("{}", panic_err);
            }

            if run_ret.is_err() {
                let ce = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, run_ret.err().unwrap().to_string());
                panic!("python init run code panic: {}", ce.to_string());
            }
        });

        Ok(())
    }

    pub(crate) fn shutdown() -> Result<(), CommonError> {
        Python::attach(|py| {
            py.run(c_str!(r#"
te.close()
te = None
te_map.clear()
            "#), None, None).unwrap();
        });
        Ok(())
    }
}

pub struct PyInterpreter {
    global_ref : &'static dyn GlobalLayout
}
impl PyInterpreter {
    pub fn new() -> Result<Self, CommonError> {
        Ok(PyInterpreter { global_ref: *crate::global::GLOBAL_REFER.get().expect("global refer is broken") })
    }

    fn get_script<S : AsRef<str>>(&self, plan_name : S) -> Result<String,CommonError> {
        let ret = self.global_ref.get_script_data(plan_name.as_ref()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get script data failed", e)
        })?;

        Ok(ret)
    }

    fn run_script(&self, script : &'_ str) -> Result<String, CommonError> {
        let mut attach_ret :  Result<(), CommonError> = Ok(());
        let mut uuid = String::from("");
        let all_script = format!(r#"__internal_run_eval(r"""{}""")"#, script);

        let cstr = CString::new(all_script).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Python::attach(|py| {
            let locals = PyDict::new(py);

            let eval_ret = py.eval(cstr.as_c_str(), None, None).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
            });

            if eval_ret.is_err() {
                attach_ret = eval_ret.err().unwrap().to_result();
                return
            }

            let var = eval_ret.unwrap();
            let uuid_ret : Result<&Bound<PyString>,  CastError<'_, '_>> = var.cast();
            if uuid_ret.is_err() {
                attach_ret = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, uuid_ret.err().unwrap().to_string()).to_result();
                return
            }
            let cast = uuid_ret.unwrap();
            uuid = cast.to_string();
        });

        attach_ret?;
        Ok(uuid)
    }

    fn force_stop_thread_execute(&self, uuid : String) -> Result<(), CommonError> {
        let mut attach_ret :  Result<(), CommonError> = Ok(());
        let all_script = format!(r#"te_map['{}'].cancel()"#, uuid);

        let cstr = CString::new(all_script).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Python::attach(|py| {
            let ret_ret = py.run(cstr.as_c_str(), None, None).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
            });

            if attach_ret.is_err() {
                attach_ret = Err(ret_ret.err().unwrap());
            }
        });

        attach_ret
    }

    fn is_thread_execute_error(&self, uuid : String) -> Result<(), CommonError> {
        let mut attach_ret :  Result<(), CommonError> = Ok(());
        let all_script = format!(r#"__internal_get_error_code('{}')"#, uuid);

        let cstr = CString::new(all_script).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Python::attach(|py| {
            let eval_ret = py.eval(cstr.as_c_str(), None, None).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
            });

            if eval_ret.is_err() {
                attach_ret = Err(eval_ret.err().unwrap());
                return
            }
            let eval_data = eval_ret.unwrap();
            let is_done_ret : Result<&Bound<PyString>,  CastError<'_, '_>> = eval_data.cast();

            if is_done_ret.is_err() {
                attach_ret = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, is_done_ret.err().unwrap().to_string()).to_result();
                return
            }

            let msg = is_done_ret.unwrap().to_string();

            if msg != "" {
                attach_ret = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, msg).to_result();
            }
        });

        attach_ret
    }

    fn await_done_script(&self, uuid : String) -> Result<(), CommonError> {
        let mut attach_ret :  Result<(), CommonError> = Ok(());
        let all_script = format!(r#"__internal_await_done('{}')"#, uuid);

        let cstr = CString::new(all_script).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        loop {
            let mut is_done = true;
            Python::attach(|py| {
                let eval_ret = py.eval(cstr.as_c_str(), None, None).map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                });

                if eval_ret.is_err() {
                    attach_ret = Err(eval_ret.err().unwrap());
                    return
                }
                let eval_data = eval_ret.unwrap();
                let is_done_ret : Result<&Bound<PyBool>,  CastError<'_, '_>> = eval_data.cast();

                if is_done_ret.is_err() {
                    attach_ret = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, is_done_ret.err().unwrap().to_string()).to_result();
                    return
                }

                is_done = is_done_ret.unwrap().is_true();
            });

            if is_done {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        attach_ret
    }
}

impl crate::Interpreter for PyInterpreter {
    fn gc(&self) -> Result<(), CommonError> {
        let mut attach_ret :  Result<(), CommonError> = Ok(());

        Python::attach(|py| {
            let gc = py.import("gc").map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
            });

            if gc.is_err() {
                attach_ret = Err(gc.unwrap_err());
                return;
            }

            let method_ret = gc.unwrap().call_method0("collect").map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
            });

            if method_ret.is_err() {
                attach_ret = Err(method_ret.unwrap_err());
            }
        });

        attach_ret
    }

    fn run(&self, name: &'_ str) -> Result<(), CommonError> {
        let script = self.get_script(name)?;

        let key = self.run_script(script.as_str()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "run_script failed", e)
        })?;

        let await_ret = self.await_done_script(key.clone());

        if await_ret.is_err() {
            self.force_stop_thread_execute(key.clone()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "run_script force stop failed", e)
            })?;

            await_ret?;
        }

        self.is_thread_execute_error(key.clone()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "python error", e)
        })?;

        Ok(())
    }
}