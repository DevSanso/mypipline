use std::collections::{HashMap, HashSet};
use common_rs::c_core::func;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::*;
use common_rs::logger::log_debug;
use mypip_global::GLOBAL;
use mypip_types::config::plan::{PlanChain, PlanChainArgs};
use mypip_types::interface::GlobalLayout;

pub(crate) struct QueryEntry<'a> {
    plan_name : &'a str,
    chain : &'a [PlanChain]
}


fn create_query_bind_from_args_array(v: &mut Vec<PairValueEnum>, p : &'_ PlanChain)  -> Result<(), CommonError> {
    if let Some(args) = p.args.as_ref() {
        if args.len() > v.len() {
            v.resize(args.len(), PairValueEnum::Null);
        }
        for arg in args.iter() {
            if arg.idx <= 0 {
                return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("{} Index out of bounds: {}", p.id, arg.idx)).to_result();
            }

            v[arg.idx - 1] = PairValueEnum::String(arg.data.clone());
        }
    }
    Ok(())
}

fn create_query_bind_from_bind_array(v: &mut Vec<PairValueEnum>, p : &'_ PlanChain, m : &HashMap::<String, PairValueEnum>) -> Result<(), CommonError> {
    if let Some(binds) = p.bind.as_ref() {
        if binds.len() > v.len() {
            v.resize(v.len() + binds.len(), PairValueEnum::Null);
        }
        for bind in binds {
            if bind.idx <= 0 {
                return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("{} Index out of bounds: {}", p.id, bind.idx)).to_result();
            }

            if bind.idx > v.len() {
                v.resize(bind.idx + 2, PairValueEnum::Null);
            }

            if let Some(PairValueEnum::Map(ele)) = m.get(bind.id.as_str()) {

            }
        }
    }
    Ok(())
}

fn create_query_bind_array(p : &'_ PlanChain, m : &HashMap::<String, PairValueEnum>) -> Result<PairValueEnum, CommonError> {
    let mut v = Vec::new();
    create_query_bind_from_args_array(&mut v, p).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "args array use failed", e)
    })?;
    create_query_bind_from_bind_array(&mut v, p, m).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "bind array use failed", e)
    })?;
    Ok(PairValueEnum::Array(v))
}

fn chk_conflict_bind_param(p : &'_[PlanChain]) -> bool {
    let mut exists_set = HashSet::<usize>::new();

    for plan in p {
        if let Some(args) = plan.args.as_ref() {
            for arg in args {
                if !exists_set.insert(arg.idx) {
                    return false;
                }
            }
        }

        if let Some(binds) = plan.bind.as_ref() {
            for bind in binds {
                if !exists_set.insert(bind.idx) {
                    return false;
                }
            }
        }
    }

    true
}

fn run_first_query(item : &PlanChain, plan_name : &'_ str, ) -> Result<PairValueEnum, CommonError> {
    let pool_name = item.connection.as_str();
    let p = GLOBAL.get_exec_pool(pool_name.into()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("get pool failed {}:{}",plan_name, item.id), e)
    })?;

    let mut p_item = p.get_owned(()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("get pool item failed {}:{}",plan_name, item.id), e)
    })?;

    let conn = p_item.get_value();
    let mut bind_data = Vec::new();
    create_query_bind_from_args_array(&mut bind_data, item).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("bind data create failed {}:{}",plan_name, item.id), e)
    })?;

    let ret = conn.execute_pair(item.query.as_str(), &PairValueEnum::Array(bind_data)).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, format!("query run failed {}:{}",plan_name, item.id), e)
    })?;

    Ok(ret)
}

impl<'a> QueryEntry<'a> {
    pub fn run(&self) -> Result<(), CommonError> {
        let mut data_map = HashMap::<String, PairValueEnum>::new();
        
        if self.chain.len() <= 0 {
            return Ok(());
        }

        if !chk_conflict_bind_param(self.chain) {
            return CommonError::new(&CommonDefaultErrorKind::Etc, "conflict idx bind or args").to_result();
        }

        let first_data = run_first_query(&self.chain[0], self.plan_name).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "first query run failed", e)
        })?;

        data_map.insert(self.chain[0].id.clone(), first_data);

        log_debug!("{} - try running, name={} first success", func!(), self.chain[0].id.as_str());

        for item in self.chain.iter().skip(1) {
            let pool_name = item.connection.as_str();
            let p = GLOBAL.get_exec_pool(pool_name.into()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("get pool failed {}:{}",self.plan_name, item.id), e)
            })?;

            let mut p_item = p.get_owned(()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("get pool item failed {}:{}",self.plan_name, item.id), e)
            })?;

            let conn = p_item.get_value();

            log_debug!("{} - try running, query={}", func!(), item.query.as_str());

            let bind_data = create_query_bind_array(item, &data_map).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("bind data create failed {}:{}",self.plan_name, item.id), e)
            })?;
            
            let ret = conn.execute_pair(item.query.as_str(), &bind_data).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, format!("query run failed {}:{}",self.plan_name, item.id), e)
            })?;
            
            data_map.insert(item.id.clone(), ret);
        }
        
        Ok(())
    }
    
    pub fn new(plan_name : &'a str, chain : &'a [PlanChain]) -> Self {
        Self { plan_name, chain }
    }
}