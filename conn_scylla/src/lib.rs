mod db_conn;
pub(crate) mod types;

use common::logger;

use common::collection::pool::get_thread_safe_pool;
use conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonSqlConnectionPool};
use db_conn::ScyllaCommonSqlConnection;

pub fn create_scylla_conn_pool(name : String, info : Vec<CommonSqlConnectionInfo>, alloc_size : usize) -> CommonSqlConnectionPool {
    let gen_fn : Box<dyn Fn(()) -> Option<Box<dyn CommonSqlConnection>>> = (|info : Vec<CommonSqlConnectionInfo>| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = ScyllaCommonSqlConnection::new(global_info.clone());
            
            match conn {
                Ok(ok) => Some(Box::new(ok) as Box<dyn CommonSqlConnection>),
                Err(err) => {
                    logger::error!("{}", err.to_string());
                    None
                }
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}