use std::sync::Arc;
use common_rs::exec::c_exec_shell::ShellSplit;
use common_rs::exec::c_relational_exec::{RelationalExecutorPool, RelationalValue};
use crate::executor::exec_sync;
use crate::types::config::Plan;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(super) enum PlanState {
    RUNNING,
    STOP
}

pub(super) struct PlanThreadEntryArgs {
    pub state : Arc<exec_sync::ExecutorStateMap<PlanState>>,
    pub db_conn : Arc<exec_sync::ExecutorStateMap<RelationalExecutorPool<RelationalValue>>>,
    pub shell_conn : Arc<exec_sync::ExecutorStateMap<RelationalExecutorPool<ShellSplit>>>,
    pub plan : Plan,
    pub name : String,
}