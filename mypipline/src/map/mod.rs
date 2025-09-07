pub mod impl_map;

use std::sync::Arc;

use common_rs::db::core::CommonSqlConnectionPool;

use crate::config::ConnectionConfig;
use crate::config::PlanConfig;
use crate::plan::Plan;

pub type DbConnPool = impl_map::CallMap<String, CommonSqlConnectionPool, ConnectionConfig>;
pub type PlanPool = impl_map::CallMap<String, Arc<Plan>, PlanConfig>;