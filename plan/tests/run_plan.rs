use conn::CommonValue;
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

query = 'select cast(1 as int8) as val'
fetch = {'val' = {val_type = 'int'}}

[send]
type = \"sql\"

[send.sql]
dbtype = 'postgres'
connection = [
     {ip = '127.0.0.1', port=5432, user='postgres', password='postgres', dbname='postgres'}
]
query = 'select #{val} + cast(0 as int8)'
";


#[test]
pub fn test_run_toml_plan() -> Result<(), Box<dyn std::error::Error>> {
    let result = plan_toml::load_str(TOML_SAMPLE_STR)?;
    let mut maked = plan::make_plans(result)?;


    let collect_ret = maked.0.do_collect()?;
    assert_eq!(collect_ret["val"][0], CommonValue::BigInt(1), "failed eq result data");
    maked.1.do_send(collect_ret)?;
    Ok(())
}

