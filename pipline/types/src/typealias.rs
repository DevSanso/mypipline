use std::sync::Arc;
use common_rs::c_core::collection::pool::ThreadSafePool;
use crate::interface::Interpreter;

pub type InterpreterPool = Arc<dyn ThreadSafePool<Box<dyn Interpreter>, ()>>;