mod collect;
mod send;
pub mod template;

use common::{err::define as err_def, make_err_msg};

use template::PlanTemplate;
use crate::{collect::CollectPlan, send::SendPlan};



pub(crate) mod utils;

pub enum PlanType {
    RestApi,
    SQL(&'static str)
}

pub trait Plan {
    fn plan_type(&self) -> PlanType;
}

pub(crate) fn new_collect_plan(data : &'_ crate::template::CollectPlanTemplate) -> Result<Box<dyn CollectPlan>, Box<dyn std::error::Error>> {
    todo!()
}

pub fn make_plans(plan_template : PlanTemplate) -> Result<(Box<dyn SendPlan>, Box<dyn CollectPlan>), Box<dyn std::error::Error>> {
    let collect_plan = new_collect_plan(&plan_template.collect);

    todo!()
}