use std::error::Error;
use common::{collection::pool::PoolItem, init::logger};
use conn::{CommonSqlConnection, CommonSqlConnectionInfo};
use conn_scylla::create_scylla_conn_pool;
use conn::CommonValue;


const CREATE_KEY_SPACE_CQL : &'static str = "CREATE KEYSPACE test_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";

const DROP_KEY_SPACE_CQL : &'static str = "DROP KEYSPACE IF EXISTS test_keyspace";

const CREATE_TABLE_CQL : &'static str = "CREATE TABLE test_keyspace.simple_table (
    id int PRIMARY KEY,
    value_int int,
    value_float float,
    value_text text
)";

const DROP_TABLE_CQL : &'static str = "DROP TABLE IF EXISTS test_keyspace.simple_table";


fn create_schema(mut item : Box<dyn PoolItem<Box<dyn CommonSqlConnection>>>) -> Result<(), Box<dyn Error>> {
    let conn = item.get_value();

    conn.execute(CREATE_KEY_SPACE_CQL, &[])?;
    conn.execute(CREATE_TABLE_CQL, &[])?;
    Ok(())
}

fn drop_schema(mut item : Box<dyn PoolItem<Box<dyn CommonSqlConnection>>>) -> Result<(), Box<dyn Error>> {
    let conn = item.get_value();

    conn.execute(DROP_TABLE_CQL, &[])?;
    conn.execute(DROP_KEY_SPACE_CQL, &[])?;

    Ok(())
}

#[test]
pub fn test_scylla_connect() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None);

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

#[test]
pub fn test_scylla_get_dummy_select() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None).as_ref().expect("log init failed");

    let info = CommonSqlConnectionInfo {
        addr : "127.0.0.1:9042".to_string(),
        db_name : "system".to_string(),
        user : "cassandra".to_string(),
        password : "cassandra".to_string(),
        timeout_sec : 30

    };
    let pool = create_scylla_conn_pool("testing".to_string(), vec![info],3);

    
    drop_schema(pool.get_owned(())?)?;
    create_schema(pool.get_owned(())?)?;

    let mut conn: Box<dyn PoolItem<Box<dyn CommonSqlConnection>>> = pool.get_owned(())?;
    let c = conn.get_value();

    let id = CommonValue::Int(123);
    let val_int = CommonValue::Int(1111);
    let value_float = CommonValue::Float(0.1234);
    let value_text = CommonValue::String("hello world".to_string());

    let param = [id, val_int, value_float, value_text];

    c.execute("INSERT INTO test_keyspace.simple_table (id, value_int, value_float, value_text) values (?, ?, ?, ?)", param.as_ref())?;

    let ret = c.execute("SELECT id, value_int, value_float , value_text from test_keyspace.simple_table", &[])?;

    let row = &ret.cols_data[0];

    assert_eq!(row[0], param[0], "id check failed");
    assert_eq!(row[1], param[1], "value int check failed");
    assert_eq!(row[2], param[2], "value float check failed");
    assert_eq!(row[3], param[3], "value text check failed");

    Ok(())
}