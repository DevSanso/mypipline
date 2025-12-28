use std::cell::{RefCell};
use std::collections::{HashMap, HashSet};
use common_rs::c_core::func;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::*;
use common_rs::logger::log_debug;
use mypip_global::GLOBAL;
use mypip_types::config::plan::{PlanChain, PlanChainArgs};
use mypip_types::interface::GlobalLayout;

#[derive(Default)]
struct QueryEntryCache<'a> {
    bind_data_map: HashMap<&'a str, usize>,
}

impl<'a> QueryEntryCache<'a> {
    pub fn put(&mut self, key: &'a str, value : usize) {
        self.bind_data_map.insert(key, value);
    }
    pub fn get_max_cnt(&self) -> usize {
        let m = self.bind_data_map.iter().max_by(|x, x1| {
            x.1.cmp(x1.1)
        });

        m.expect(format!("{} - get max is broken", common_rs::c_core::utils::macros::func!()).as_str()).1.clone()
    }
}

pub(crate) struct QueryEntry<'a> {
    plan_name : &'a str,
    chain : &'a [PlanChain],

    cache : RefCell<QueryEntryCache<'a>>
}

fn create_query_bind_from_args_array(v: &mut Vec<PairValueEnum>, p : &'_ PlanChain)  -> Result<(), CommonError> {
    if let Some(args) = p.args.as_ref() {
        if args.len() <= 0 {
            return Ok(());
        }

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

fn create_query_bind_from_bind_array(v: &mut Vec<PairValueEnum>, p : &'_ PlanChain, m : &HashMap::<String, PairValueEnum>, use_idx : usize) -> Result<(), CommonError> {
    if let Some(binds) = p.bind.as_ref() {
        if binds.len() <= 0 {
            return Ok(());
        }

        let bind_id_ks = m.keys();

        let max_idx_chain = binds.iter().max_by(|x, x1| {
            x.idx.cmp(&x1.idx)
        }).expect("bins max idx function is panic");

        if v.len() < max_idx_chain.idx {
            v.resize(max_idx_chain.idx, PairValueEnum::Null);
        }

        for bind_id in bind_id_ks {
            if let Some(pair_data) = m.get(bind_id) {
                if let PairValueEnum::Map(data_map) = pair_data {
                    for bind in binds.iter().filter(|x2| {x2.id.as_str() == bind_id.as_str()}) {
                        data_map.get(bind.key.as_str());
                    }
                }
            } else {
                return CommonError::new(&CommonDefaultErrorKind::NoData, format!("Bind {} not found", bind_id)).to_result();
            }
        }

        for bind in binds {
            if bind.idx <= 0 {
                return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("{} Index out of bounds: {}", p.id, bind.idx)).to_result();
            }

            if bind.idx > v.len() {
                v.resize(bind.idx + 2, PairValueEnum::Null);
            }

            if let Some(PairValueEnum::Map(ele)) = m.get(bind.id.as_str()) {
                if let Some(PairValueEnum::Array(cols)) = ele.get(bind.key.as_str()) {
                    if let Some(row_idx) = bind.row {
                        let data = cols.get(row_idx).map_or(PairValueEnum::Null, |col| {col.clone()});
                        v[bind.idx-1] = data;
                    } else {
                        let data = cols.get(use_idx).map_or(PairValueEnum::Null, |col| {col.clone()});
                        v[bind.idx-1] = data;
                    }
                }
            } else {
                return CommonError::new(&CommonDefaultErrorKind::NoData, format!("{} not exists bind id data {}", p.id, bind.idx)).to_result();
            }
        }
    }
    Ok(())
}

fn create_query_bind_array(p : &'_ PlanChain, m : &HashMap::<String, PairValueEnum>, use_bind_idx : usize) -> Result<Vec<PairValueEnum>, CommonError> {
    let mut v = Vec::new();
    create_query_bind_from_args_array(&mut v, p).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "args array use failed", e)
    })?;
    create_query_bind_from_bind_array(&mut v, p, m, use_bind_idx).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "bind array use failed", e)
    })?;
    Ok(v)
}

fn chk_conflict_bind_param(p : &'_[PlanChain]) -> bool {
    let mut exists_set = HashSet::<usize>::new();

    for plan in p {
        exists_set.clear();
        
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

fn get_ret_bind_param_cnt(ret : &PairValueEnum) -> usize {
    let mut max = 0;

    if let PairValueEnum::Map(map) = ret {
        if map.len() <= 0 {
            return 0;
        } else {
            let (_, v) = map.iter().next()
                .expect(format!("{} - map.len check cond is break", common_rs::c_core::utils::macros::func!()).as_str());

            if let PairValueEnum::Array(arr) = v {
                max = arr.len();
            }
        }
    }

    max
}
fn run_one_query(item : &PlanChain, plan_name : &'_ str, bind_data : Vec<PairValueEnum>) -> Result<PairValueEnum, CommonError> {
    let pool_name = item.connection.as_str();
    let p = GLOBAL.get_exec_pool(pool_name.into()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("get pool failed {}:{}",plan_name, item.id), e)
    })?;

    let mut p_item = p.get_owned(()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("get pool item failed {}:{}",plan_name, item.id), e)
    })?;

    let conn = p_item.get_value();

    let ret = conn.execute_pair(item.query.as_str(), &PairValueEnum::Array(bind_data)).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, format!("query run failed {}:{}",plan_name, item.id), e)
    })?;

    p_item.dispose();

    Ok(ret)
}

impl<'a> QueryEntry<'a> {
    pub fn run(&self) -> Result<(), CommonError> {
        let mut data_map = HashMap::<String, PairValueEnum>::new();
        let mut loop_query_cnt = 0;
        
        if self.chain.len() <= 0 {
            return Ok(());
        }

        if !chk_conflict_bind_param(self.chain) {
            return CommonError::new(&CommonDefaultErrorKind::Etc, "conflict idx bind or args").to_result();
        }

        let mut bind_data = Vec::new();
        create_query_bind_from_args_array(&mut bind_data, &self.chain[0]).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("bind data create failed {}",self.plan_name), e)
        })?;

        let first_data = run_one_query(&self.chain[0], self.plan_name, bind_data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "first query run failed", e)
        })?;

        self.cache.borrow_mut().put(&self.chain[0].id, get_ret_bind_param_cnt(&first_data));

        data_map.insert(self.chain[0].id.clone(), first_data);

        log_debug!("{} - try running, name={} first success", func!(), self.chain[0].id.as_str());

        for item in self.chain.iter().skip(1) {
            log_debug!("{} - try running, query={}", func!(), item.query.as_str());

            let ret : PairValueEnum = if let Some(bind_param) = &item.bind {
                let mut bind_ret = PairValueEnum::Null;
                for bind_idx in 0..self.cache.borrow_mut().get_max_cnt() {
                    let bind_data = create_query_bind_array(item, &data_map, bind_idx).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("query bind param set failed {}", self.plan_name), e)
                    })?;
                    bind_ret = run_one_query(item, &self.plan_name, bind_data).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("query bind param run {}", self.plan_name), e)
                    })?;
                }
                Ok(bind_ret)
            } else {
                let mut bind_data = Vec::new();
                create_query_bind_from_args_array(&mut bind_data, item).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, format!("bind data create failed {}:{}",self.plan_name, item.id), e)
                })?;

                run_one_query(item, self.plan_name, bind_data).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "first query run failed", e)
                })
            }?;

            data_map.insert(item.id.clone(), ret);
        }
        
        Ok(())
    }
    
    pub fn new(plan_name : &'a str, chain : &'a [PlanChain]) -> Self {
        Self { plan_name, chain, cache : RefCell::new(QueryEntryCache::default()) }
    }
}