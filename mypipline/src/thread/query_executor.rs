use std::collections::HashMap;
use common_rs::c_core::func;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::relational::{RelationalExecuteResultSet, RelationalValue};
use common_rs::logger::log_debug;
use crate::global::GLOBAL;
use crate::types::config::plan::{PlanChain, PlanChainArgs};

pub(super) struct QueryExecutor<'a> {
    chain : &'a [PlanChain]
}

struct QueryExecutorBindBuilder {
    v : Vec<RelationalValue>
}

impl QueryExecutorBindBuilder {
    pub fn new(cap : usize) -> QueryExecutorBindBuilder {
        QueryExecutorBindBuilder { v : Vec::with_capacity(cap) }
    }

    pub fn as_bind_slice(&self) -> &'_[RelationalValue] {self.v.as_slice()}
    pub fn set_args(mut self, p : &PlanChain) -> Self {
        for a in p.args.as_slice() {
            if a.idx >= self.v.capacity() {
                self.v.resize(a.idx + 1, RelationalValue::Null);
            }
            self.v[a.idx] = (RelationalValue::String(a.data.clone()));
        }

        self
    }

    pub fn set_bind(mut self, p : &PlanChain, m : &HashMap::<usize, RelationalExecuteResultSet>) -> Self {
        for b in p.bind.as_slice() {
            let set = &m[&b.chain_idx];
            let row = &set.cols_data[b.row];
            let data = row[b.col].clone();

            if b.idx >= self.v.capacity() {
                self.v.resize(b.idx + 1, RelationalValue::Null);
            }

            self.v[b.idx] = data;
        }
        self
    }
}

impl<'a> QueryExecutor<'a> {
    pub fn run(&self) -> Result<(), CommonError> {
        let data_map = HashMap::<usize, RelationalExecuteResultSet>::new();

        for item in self.chain {
            let p = GLOBAL.get_exec_pool(item.conn_name.as_str()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get pool failed", e)
            })?;

            let builder = QueryExecutorBindBuilder::new(10)
                .set_args(item)
                .set_bind(item, &data_map);

            let mut p_item = p.get_owned(()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "get pool item failed", e)
            })?;

            let conn = p_item.get_value();

            log_debug!("{} - try running, query={}", func!(), item.cmd.as_str());

            conn.execute(item.cmd.as_str(), builder.as_bind_slice()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "query run failed", e)
            })?;
        }
        
        Ok(())
    }
    
    pub fn new(chain : &'a [PlanChain]) -> Self {
        Self { chain }
    }
}