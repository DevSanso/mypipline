mod collect;
mod send;
pub(crate) mod utils;

pub enum PlanType {
    RestApi,
    SQL(&'static str)
}

pub trait Plan {
    fn plan_type(&self) -> PlanType;
}