use plan_toml;

const TOML_SAMPLE_STR : &'static str = "
[collect]
type = \"sql\"
interval = 5
interval_is_system = true

[collect.sql]
dbtype = 'postgres'
connection = [
     {ip = '127.0.0.1', port=5432, user='postgres', password='postgres', dbname='postgres'}
]

query = 'select 1 as val'
fetch = {'val' = {val_type = 'int'}}

[send]
type = \"sql\"

[send.sql]
dbtype = 'postgres'
connection = [
     {ip = '127.0.0.1', port=5432, user='postgres', password='postgres', dbname='postgres'}
]
query = 'select #{val}'
";


#[test]
pub fn test_load_toml_plan() -> Result<(), Box<dyn std::error::Error>> {
    let result = plan_toml::load_str(TOML_SAMPLE_STR)?;
    let mut collect = result.collect;
    let sql = collect.sql.take().unwrap();
    let mut send = result.send;
    let send_sql = send.sql.take().unwrap();

    assert_eq!(collect.collect_type, "sql", "collect.collect_type check failed");
    assert_eq!(collect.interval, 5, "collect.interval_type check failed");
    assert_eq!(collect.interval_is_system, true, "collect.interval_is_system check failed");
    assert_eq!(sql.dbtype ,"postgres", "collect.sql.dbtype check failed");
    assert_ne!(sql.query, "", "collect.sql.query check failed");

    println!(" collect query : {}", sql.query);

    assert_eq!(send.send_type, "sql", "send.send_type check failed");
    assert_eq!(send_sql.dbtype, "postgres", "send.sql.dbtype check failed");
    assert_ne!(send_sql.query, "", "send.sql.query check failed");

    println!(" send query : {}", send_sql.query);

    Ok(())
}

#[test]
pub fn test_load_toml_plan_file() -> Result<(), Box<dyn std::error::Error>> {
    use std::path::Path;

    let assets_file = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/assets/sample.toml");
    let result = plan_toml::load(assets_file)?;
    let mut collect = result.collect;
    let sql = collect.sql.take().unwrap();
    let mut send = result.send;
    let send_sql = send.sql.take().unwrap();

    assert_eq!(collect.collect_type, "sql", "collect.collect_type check failed");
    assert_eq!(collect.interval, 5, "collect.interval_type check failed");
    assert_eq!(collect.interval_is_system, true, "collect.interval_is_system check failed");
    assert_eq!(sql.dbtype ,"postgres", "collect.sql.dbtype check failed");
    assert_ne!(sql.query, "", "collect.sql.query check failed");

    assert_eq!(send.send_type, "sql", "send.send_type check failed");
    assert_eq!(send_sql.dbtype, "postgres", "send.sql.dbtype check failed");
    assert_ne!(send_sql.query, "", "send.sql.query check failed");

    Ok(())
}