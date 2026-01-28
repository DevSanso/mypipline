use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock};
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use pyo3::ffi::c_str;
use pyo3::prelude::{PyBoolMethods, PyDictMethods};
use pyo3::{pyfunction, Bound, CastError, Py, PyAny, PyErr, PyResult, Python};
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyBool, PyDict, PyString};
use mypip_types::interface::GlobalLayout;

static PY_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static PY_INIT_MUTEX : Mutex<()> = Mutex::new(());

static GLOBAL_REFER : OnceLock<&'static dyn GlobalLayout> = OnceLock::new();

#[pyfunction]
fn py_exec_pair_conn_wrapper(py: Python, conn_name : String, cmd : String, args : Py<PyAny>) -> PyResult<Py<PyAny>> {
    let script_convert = super::utils::PyScriptConverter;
    let pair_convert = super::utils::PyPariConverter;

    let data =  crate::utils::exec_pair_conn(*GLOBAL_REFER.get().expect("global refer is broken"),
                                             conn_name.as_str(),
                                             cmd.as_str(),
                                             args, script_convert, pair_convert).map_err(|e| {
        PyErr::new::<PyRuntimeError, _>(format!("{:?}", e))
    })?;

    Ok(data)
}
pub struct PyThreadInterpreter {
    global_ref : &'static dyn GlobalLayout
}

impl PyThreadInterpreter {
    pub fn new(global : &'static dyn GlobalLayout) -> PyThreadInterpreter {
        if PY_INIT_COUNT.fetch_add(1, Ordering::SeqCst) == 0 {
            GLOBAL_REFER.get_or_init(|| global);

            let lock = PY_INIT_MUTEX.lock().unwrap();
            Python::attach(|py| {
                py.run(c_str!(r#"
                global te_map = {}
                global te = ThreadPoolExecutor(max_workers=100)
                def run_eval(code):
                    return eval(code)
            "#), None, None).unwrap();
            });
            drop(lock);
        }

        PyThreadInterpreter { global_ref: global }
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
        let all_script = format!(
            r#"import uuid
                random_uuid = uuid.uuid4()
                future = te.submit(run_eval, """{}""")
                te_map[random_uuid] = future"#, script
        );

        let cstr = CString::new(all_script).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        Python::attach(|py| {
            let locals = PyDict::new(py);

            attach_ret = py.run(cstr.as_c_str(), None, Some(&locals)).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
            });


            if attach_ret.is_err() {
                return
            }

            let ret = locals.get_item("random_uuid");

            if ret.is_err() {
                attach_ret = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, ret.err().unwrap().to_string()).to_result();
                return
            }

            if let Some(var) = ret.unwrap() {
                let uuid_ret : Result<&Bound<PyString>,  CastError<'_, '_>> = var.cast();
                if uuid_ret.is_err() {
                    attach_ret = CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, uuid_ret.err().unwrap().to_string()).to_result();
                    return
                }

                uuid = uuid_ret.unwrap().to_string();
            } else {
                attach_ret = CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, "var is not string type").to_result();
            }
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

    fn await_done_script(&self, uuid : String) -> Result<(), CommonError> {
        let mut attach_ret :  Result<(), CommonError> = Ok(());
        let all_script = format!(r#"te_map['{}'].done()"#, uuid);

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

impl Drop for PyThreadInterpreter {
    fn drop(&mut self) {
        if PY_INIT_COUNT.fetch_sub(1, Ordering::SeqCst) == 1 {
            let lock = PY_INIT_MUTEX.lock().unwrap();
            Python::attach(|py| {
                py.run(c_str!(r#"
                te.close()
                te = None
                te_map.clear()
            "#), None, None).unwrap();
            });
            drop(lock);
        }
    }
}

impl crate::Interpreter for PyThreadInterpreter {
    fn gc(&self) -> Result<(), CommonError> {
        todo!()
    }

    fn run(&self, name: &'_ str) -> Result<(), CommonError> {
        let script = self.get_script(name)?;

        let key = self.run_script(script.as_str()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "run_script failed", e)
        })?;

        let await_ret = self.await_done_script(key.clone());

        if await_ret.is_err() {
            self.force_stop_thread_execute(key).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "run_script force stop failed", e)
            })?;

            await_ret?;
        }
        Ok(())
    }
}