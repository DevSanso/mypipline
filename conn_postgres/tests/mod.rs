use std::error::Error;
use common::{collection::pool::PoolItem, init::logger};
use conn::{CommonSqlConnection, CommonSqlConnectionInfo};
use conn_postgres::create_pg_conn_pool;
use conn::CommonValue;

const CREATE_SCHEMA_SQL : &'static str = "CREATE SCHEMA IF NOT EXISTS test_schema;";

const DROP_SCHEMA_SQL : &'static str = "DROP SCHEMA IF EXISTS test_schema";

const CREATE_TABLE_SQL : &'static str = "CREATE TABLE test_schema.simple_table (
    id int8,
    value_int int8,
    value_float float8,
    value_text varchar(24)
)";

const DROP_TABLE_SQL : &'static str = "DROP TABLE IF EXISTS test_schema.simple_table";

fn create_schema(mut item : Box<dyn PoolItem<Box<dyn CommonSqlConnection>>>) -> Result<(), Box<dyn Error>> {
    let conn = item.get_value();

    conn.execute(CREATE_SCHEMA_SQL, &[])?;
    conn.execute(CREATE_TABLE_SQL, &[])?;
    Ok(())
}

fn drop_schema(mut item : Box<dyn PoolItem<Box<dyn CommonSqlConnection>>>) -> Result<(), Box<dyn Error>> {
    let conn = item.get_value();

    conn.execute(DROP_TABLE_SQL, &[])?;
    conn.execute(DROP_SCHEMA_SQL, &[])?;

    Ok(())
}

#[test]
pub fn test_pg_connect() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None);

    let info = CommonSqlConnectionInfo {
        addr : "127.0.0.1:5432".to_string(),
        db_name : "postgres".to_string(),
        user : "postgres".to_string(),
        password : "postgres".to_string(),
        timeout_sec : 30

    };
    let pool = create_pg_conn_pool("testing".to_string(), info,3);

    let mut conn = pool.get_owned(())?;
    let c = conn.get_value();
    Ok(())
}

#[test]
pub fn test_pg_get_unix_time() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None);

    let info = CommonSqlConnectionInfo {
        addr : "127.0.0.1:5432".to_string(),
        db_name : "postgres".to_string(),
        user : "postgres".to_string(),
        password : "postgres".to_string(),
        timeout_sec : 30

    };
    let pool = create_pg_conn_pool("testing".to_string(), info,3);

    let mut conn = pool.get_owned(())?;
    let c = conn.get_value();

    let ti = c.get_current_time()?;
    println!("{}", ti.as_secs());
    Ok(())
}

#[test]
pub fn test_pg_get_dummy_select() -> Result<(), Box<dyn Error>> {
    logger::init_once("trace", None);

    let info = CommonSqlConnectionInfo {
        addr : "127.0.0.1:5432".to_string(),
        db_name : "postgres".to_string(),
        user : "postgres".to_string(),
        password : "postgres".to_string(),
        timeout_sec : 30

    };
    let pool = create_pg_conn_pool("testing".to_string(), info,3);

    
    drop_schema(pool.get_owned(())?)?;
    create_schema(pool.get_owned(())?)?;

    let mut conn: Box<dyn PoolItem<Box<dyn CommonSqlConnection>>> = pool.get_owned(())?;
    let c = conn.get_value();

    let id = CommonValue::BigInt(123);
    let val_int = CommonValue::BigInt(1111);
    let value_float = CommonValue::Double(0.1234);
    let value_text = CommonValue::String("hello world".to_string());

    let param = [id, val_int, value_float, value_text];

    c.execute("INSERT INTO test_schema.simple_table (id, value_int, value_float, value_text) values ($1, $2, $3, $4)", param.as_ref())?;

    let ret = c.execute("SELECT id, value_int, value_float , value_text from test_schema.simple_table", &[])?;

    let row = &ret.cols_data[0];

    assert_eq!(row[0], param[0], "id check failed");
    assert_eq!(row[1], param[1], "value int check failed");
    assert_eq!(row[2], param[2], "value float check failed");
    assert_eq!(row[3], param[3], "value text check failed");

    Ok(())
}


