mod collect;
mod send;
pub mod template;

use template::PlanTemplate;
pub use crate::{collect::CollectPlan, send::SendPlan};

pub(crate) mod utils;

pub enum PlanType {
    RestApi,
    SQL(&'static str)
}

pub trait Plan {
    fn plan_type(&self) -> PlanType;
}

pub fn make_plans(plan_template : PlanTemplate) -> Result<(Box<dyn CollectPlan>, Box<dyn SendPlan>), Box<dyn std::error::Error>> {
    let collect_plan = crate::collect::new_collect_plan(
        plan_template.name.clone(), &plan_template.collect)?;
    
    let send_plan = crate::send::new_send_plan(
        plan_template.name.clone(), &plan_template.send)?;
    

    Ok((collect_plan, send_plan))
}