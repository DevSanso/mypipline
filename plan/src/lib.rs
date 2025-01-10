mod collect;
mod send;

pub enum PlanType {
    RestApi,
    SQL(&'static str)
}

pub trait Plan {
    fn plan_type(&self) -> PlanType;
}