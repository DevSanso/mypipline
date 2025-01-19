mod db_conn;

use common::logger;
use db_conn::PostgresConnection;

use common::collection::pool::get_thread_safe_pool;
use conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonSqlConnectionPool};

pub fn create_pg_conn_pool(name : String, info : CommonSqlConnectionInfo, alloc_size : usize) -> CommonSqlConnectionPool {
    let gen_fn : Box<dyn Fn(()) -> Option<Box<dyn CommonSqlConnection>>> = (|info : CommonSqlConnectionInfo| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = PostgresConnection::new(global_info.clone());
            
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