

pub const CONN_TYPE_SHELL : &'static str = "cmd";
pub const CONN_TYPE_PG : &'static str = "postgres";
pub const CONN_TYPE_SCYLLA : &'static str = "scylla";
pub const CONN_TYPE_DUCKDB : &'static str = "duckdb";
pub const PLAN_TYPE_SCRIPT : &'static str = "script";
pub const PLAN_TYPE_QUERY : &'static str = "query";

pub const CONVERT_HARD_BIND_PARAM_PREFIX : &'static str = "$$CONV_BIND_PARAM:";
pub const CONVERT_SQL_BIND_PARAM_PREFIX : &'static str = "$$BIND_PARAM:";