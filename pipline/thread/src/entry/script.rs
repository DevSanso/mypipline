use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use mypip_global::GLOBAL;
use mypip_types::config::plan::PlanScript;
use mypip_types::interface::GlobalLayout;

pub struct ScriptEntry {
    plan_name : String,
    plan_script : PlanScript
}

impl ScriptEntry {
    pub fn new(plan_name : String, plan_script : PlanScript) -> Self {
        Self { plan_name, plan_script }
    }
    pub fn run(&self) -> Result<(), CommonError> {
        const SUPPORT : [&'static str;2] = ["lua","python"];
        if !SUPPORT.contains(&self.plan_script.lang.as_str()) {
            return CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("{} - only support lua", self.plan_name)).to_result();
        }

        let p = GLOBAL.get_interpreter_pool(self.plan_script.lang.as_str().into()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::Etc, format!("{} - failed get pool", self.plan_name), e)
        })?;
        
        let mut item = p.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::Etc, format!("{} - failed get item", self.plan_name), e)
        })?;

        let vm = item.get_value();
        vm.run(self.plan_script.file.as_str()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, format!("{} - failed run script", self.plan_name), e)
        })?;

        Ok(())
    }
}