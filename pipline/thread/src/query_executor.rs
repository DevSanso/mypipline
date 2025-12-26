use std::collections::HashMap;
use common_rs::c_core::func;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::*;
use common_rs::logger::log_debug;
use mypip_global::GLOBAL;
use mypip_types::config::plan::{PlanChain, PlanChainArgs};
use mypip_types::interface::GlobalLayout;

pub(super) struct QueryExecutor<'a> {
    chain : &'a [PlanChain]
}

struct QueryExecutorBindBuilder {
    m : HashMap<String, PairValueEnum>,
    v : Vec<PairValueEnum>
}

fn create_query_bind_array(p : &'_ PlanChain, m : &HashMap::<String, PairValueEnum>) -> Result<PairValueEnum, CommonError> {
    let mut v = Vec::with_capacity(3);
    for arg in p.args.iter() {
        if arg.idx - 1 <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("{} Index out of bounds: {}", p.id, arg.idx - 1)).to_result();
        }
        
        if arg.idx > v.len() {
            v.resize(arg.idx + 2, PairValueEnum::Null);
        }
        
        v[arg.idx - 1] = PairValueEnum::String(arg.data.clone());
    }
    
    for bind in p.bind.iter() {
        if bind.idx - 1 <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("{} Index out of bounds: {}", p.id, bind.idx - 1)).to_result();
        }
        
        if bind.idx > v.len() {
            v.resize(bind.idx + 2, PairValueEnum::Null);
        }
        
        if let Some(ele) = m.get(bind.id.as_str()) {
            if let PairValueEnum::Array(arr) = ele {
                v[bind.idx - 1] = arr.get(bind.col - 1).map_or_else(|| PairValueEnum::Null, Clone::clone);
            }
        }
    }

    Ok(PairValueEnum::Array(v))
}

impl<'a> QueryExecutor<'a> {
    pub fn run(&self) -> Result<(), CommonError> {
        let mut data_map = HashMap::<String, PairValueEnum>::new();

        for item in self.chain {
            let pool_name = item.conn_name.as_str();
            let p = GLOBAL.get_exec_pool(pool_name.into()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get pool failed", e)
            })?;

            let mut p_item = p.get_owned(()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get pool item failed", e)
            })?;

            let conn = p_item.get_value();

            log_debug!("{} - try running, query={}", func!(), item.cmd.as_str());
            
            let bind_data = create_query_bind_array(item, &data_map).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "bind data create failed", e)
            })?;

            let ret = conn.execute_pair(item.cmd.as_str(), &bind_data).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "query run failed", e)
            })?;
            
            data_map.insert(item.id.clone(), ret);
        }
        
        Ok(())
    }
    
    pub fn new(chain : &'a [PlanChain]) -> Self {
        Self { chain }
    }
}