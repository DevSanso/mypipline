mod db_conn;
pub(crate) mod types;

use std::sync::Arc;
use common::logger;

use common::collection::pool::OwnedPool;
use conn::{CommonConnection, CommonConnectionInfo};
use db_conn::ScyllaCommonConnection;

pub type ScyllaPool = Arc<OwnedPool<Box<dyn CommonConnection>,()>>;

pub fn create_scylla_conn_pool(info : Vec<CommonConnectionInfo>, alloc_size : usize) -> ScyllaPool {
    let gen_fn : Box<dyn Fn(()) -> Option<Box<dyn CommonConnection>>> = (|info : Vec<CommonConnectionInfo>| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = ScyllaCommonConnection::new(global_info.clone());
            
            match conn {
                Ok(ok) => Some(Box::new(ok) as Box<dyn CommonConnection>),
                Err(err) => {
                    logger::error!("{}", err.to_string());
                    None
                }
            }
        };

        Box::new(real_fn)
    })(info);

    OwnedPool::new("scylla".to_string(), gen_fn, alloc_size)
}