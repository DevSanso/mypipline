use std::sync::Arc;
use common_rs::exec::c_exec_shell::ShellSplit;
use common_rs::exec::c_relational_exec::{RelationalExecutorPool, RelationalValue};
use crate::executor::exec_sync;
use crate::executor::types::PlanState;
use crate::types::config::Plan;
use crate::executor::types::PlanThreadEntryArgs;



pub(super) fn plan_thread_entry(args : PlanThreadEntryArgs) {

}