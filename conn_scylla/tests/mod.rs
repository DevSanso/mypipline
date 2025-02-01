use std::error::Error;
use common::init::logger;
use conn::CommonSqlConnectionInfo;
use conn_scylla::create_scylla_conn_pool;

#[test]
pub fn test_scylla_connect() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();



    let info = CommonSqlConnectionInfo {
        addr : "127.0.0.1:9042".to_string(),
        db_name : "system".to_string(),
        user : "cassandra".to_string(),
        password : "cassandra".to_string(),
        timeout_sec : 30

    };
    let pool = create_scylla_conn_pool("testing".to_string(), vec![info],3);

    let mut conn = pool.get_owned(())?;
    let c = conn.get_value();
    Ok(())
}

#[test]
pub fn test_scylla_get_unix_time() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();
    let info = CommonSqlConnectionInfo {
        addr : "127.0.0.1:9042".to_string(),
        db_name : "system".to_string(),
        user : "cassandra".to_string(),
        password : "cassandra".to_string(),
        timeout_sec : 30

    };
    let pool = create_scylla_conn_pool("testing".to_string(), vec![info],3);

    let mut conn = pool.get_owned(())?;
    let c = conn.get_value();

    let ti = c.get_current_time()?;
    println!("{}", ti.as_secs());
    Ok(())
}