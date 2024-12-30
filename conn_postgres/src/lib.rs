mod db_conn;

use std::sync::Arc;
use common::logger;
use db_conn::PostgresConnection;

use common::collection::pool::OwnedPool;
use conn::{CommonConnection, CommonConnectionInfo};

pub type PgPool = Arc<OwnedPool<Box<dyn CommonConnection>,()>>;

pub fn create_scylla_conn_pool(info : CommonConnectionInfo, alloc_size : usize) -> PgPool {
    let gen_fn : Box<dyn Fn(()) -> Option<Box<dyn CommonConnection>>> = (|info : CommonConnectionInfo| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = PostgresConnection::new(global_info.clone());
            
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