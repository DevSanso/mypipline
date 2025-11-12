use std::sync::Arc;
use common_rs::c_err::{CommonError, CommonErrors};
use common_rs::c_err::gen::CommonErrorList;
use common_rs::exec::c_exec_shell::ShellParam;
use common_rs::exec::c_relational_exec::{RelationalExecuteResultSet, RelationalExecutorPool, RelationalValue};
use common_rs::logger;


use crate::types::config::{Plan, PlanElement};
use crate::executor::types::{ExecutorStateMap, PlanState, PlanThreadEntryArgs};
use crate::constant;
use crate::executor::types as exec_types;
use crate::executor::utils;

fn copy_execute_result_set(buf : &mut RelationalExecuteResultSet, exec : &RelationalExecuteResultSet) -> Result<(), CommonError> {
    if buf.cols_data.len() > 0 {
        if exec.cols_data.len() < 0 || buf.cols_data[0].len() != exec.cols_data[0].len() {
            return CommonError::new(&CommonErrorList::NotMatchArgs, "not matching row count".to_string()).to_result()
        }
    } else {
        buf.cols_name.extend_from_slice(exec.cols_name.as_slice());
    }
    buf.cols_data.extend_from_slice(&exec.cols_data.as_slice());

    Ok(())
}

fn convert_real_query(origin : &'_ str, hard_args : &'_ [String], is_num : bool) -> (String, Vec<(usize, usize)>) {
    let hard = utils::convert_string_use_prefix_hard(origin, constant::CONVERT_HARD_BIND_PARAM_PREFIX, hard_args);

    if is_num {
        utils::convert_string_use_prefix_soft_num(hard.as_str(), constant::CONVERT_SQL_BIND_PARAM_PREFIX, 5)
    }
    else {
        utils::convert_string_use_prefix_soft_question(hard.as_str(), constant::CONVERT_SQL_BIND_PARAM_PREFIX, 5)
    }
}

fn run_command_from_shell(e : &PlanElement, entry_args : &PlanThreadEntryArgs, args : Option<RelationalExecuteResultSet>) -> Result<RelationalExecuteResultSet, CommonError> {
    let (real, _) = convert_real_query(e.conn_name.as_str(), e.args.as_slice(), true);

    let p = entry_args.state.get_shell_conn_pool(e.conn_name.as_str())?;
    let mut ret = RelationalExecuteResultSet::default();
    let mut conn_item = p.get_owned(()).map_err(|err| {
        CommonError::new(&CommonErrorList::Etc, format!("{} : {}", e.conn_name, err.to_string()))
    })?;

    let conn = conn_item.get_value();

    if args.is_none() {
        let shell_param = ShellParam {
            sep: e.args[0],
            next: e.args[1],
            args: vec![],
        };

        let res_set = conn.execute(real.as_str(), &[shell_param])?;
        copy_execute_result_set(&mut ret, &res_set)?;
        return Ok(ret);
    }

    for param in args.unwrap().cols_data {
        let shell_param = ShellParam {
            sep: e.args[0],
            next: e.args[1],
            args: param.into(),
        };

        let res_set = conn.execute(real.as_str(), &[shell_param])?;
        copy_execute_result_set(&mut ret, &res_set)?;
    }

    Ok(ret)
}

fn run_command_from_rdb(e : &PlanElement, entry_args : &PlanThreadEntryArgs, args : Option<RelationalExecuteResultSet>) -> Result<RelationalExecuteResultSet, CommonError> {
    let (real, off) = convert_real_query(e.conn_name.as_str(), e.args.as_slice(), true);

    let p = entry_args.state.get_db_conn_pool(e.conn_name.as_str())?;
    let mut ret = RelationalExecuteResultSet::default();
    let mut conn_item = p.get_owned(()).map_err(|err| {
        CommonError::new(&CommonErrorList::Etc, format!("{} : {}", e.conn_name, err.to_string()))
    })?;

    let conn = conn_item.get_value();

    if args.is_none() {
        let res_set = conn.execute(real.as_str(), &[])?;
        copy_execute_result_set(&mut ret, &res_set)?;
        return Ok(ret);
    }

    let prev_plan_param = args.unwrap();
    let mut exec_param = vec![RelationalValue::Null; prev_plan_param.cols_name.len()];
    let mut all_row_off = Vec::with_capacity(5);
    for i in 0..off.len() {
        let offset = off[i];

        if offset.0 != usize::MAX {
            if offset.0 >= prev_plan_param.cols_data.len() {
                return CommonError::new(&CommonErrorList::OverFlowMemory,
                                        format!("{} - {} > {}", e.conn_name, offset.0, prev_plan_param.cols_data.len())).to_result();
            }

            if offset.1 >= prev_plan_param.cols_data[offset.0].len() {
                return CommonError::new(&CommonErrorList::OverFlowMemory,
                                        format!("{} - row - {} > {}", e.conn_name, offset.1, prev_plan_param.cols_data[offset.0].len())).to_result();
            }

            exec_param[i] = prev_plan_param.cols_data[offset.0][offset.1];
        }
        else {
            if offset.1 >= prev_plan_param.cols_name.len() {
                return CommonError::new(&CommonErrorList::OverFlowMemory,
                                        format!("{} - row - {} > {}", e.conn_name, offset.1, prev_plan_param.cols_data[offset.0].len())).to_result();
            }

            all_row_off.push(offset.1);
        }
    }

    for i in 0..args.unwrap().cols_data.len() {
        for off in all_row_off.iter() {
            exec_param[off] = prev_plan_param.cols_data[i][off];
        }

        let res_set = conn.execute(real.as_str(), exec_param.as_slice())?;
        copy_execute_result_set(&mut ret, &res_set)?;
    }

    Ok(ret)
}
pub(super) fn plan_thread_entry(args : PlanThreadEntryArgs) {
    let mut use_result_ret : Option<RelationalExecuteResultSet> = None;

    for ele in &args.plan.elements {
        let is_shell = args.state.is_shell_conn(ele.conn_name.as_str());
        if is_shell.is_err() {
            logger::log_error!("{}", is_shell.err().unwrap());
            break;
        }
        let data = if is_shell.unwrap() {
            run_command_from_shell(ele, &args, use_result_ret)
        } else {
            run_command_from_rdb(ele, &args, use_result_ret)
        };
        if data.is_err() {
            logger::log_error!("{}", data.err().unwrap());
            break;
        }
        use_result_ret = Some(data.unwrap());
    }

    args.state.set_plan_state(&args.plan.plan_name, PlanState::STOP)
        .expect(format!("plan_thread_entry - set failed - {}", args.plan.plan_name).as_str());
}