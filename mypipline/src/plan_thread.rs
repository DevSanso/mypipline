use std::error::Error;
use std::time;

use plan::{make_plans, template::PlanTemplate};
use plan::{SendPlan, CollectPlan};
use common::err::define as err_def;
use common::err::make_err_msg;

fn plan_entry(plans : (String, Box<dyn CollectPlan>, Box<dyn SendPlan>)) {
    let name = plans.0;
    let mut collect = plans.1;
    let mut send = plans.2;

    loop {
        std::thread::sleep(time::Duration::from_secs(1));

        let is_interval = match collect.is_interval() {
            Ok(ok) => ok,
            Err(e) => {
                common::logger::error!("plan_entry({}) - failed interval get\n{}", name, e);
                false
            }
        };

        if !is_interval {
            common::logger::trace!("plan_entry({}) - not interval", name);
            continue;
        }

        let collect_data = match collect.do_collect() {
            Ok(ok) => ok,
            Err(e) => {
                common::logger::error!("plan_entry({}) - collect failed\n{}", name, e);
                continue;
            }
        };

        common::logger::debug!("plan_entry({}) - get success data ks: {}", name, collect_data.keys().count());

        if common::logger::get_is_trace_level() {
            for (k, val) in &collect_data {
                let mut buffer = String::with_capacity(256);
                buffer.push_str(format!("plan_entry({}) - print data key: {}", name, k).as_str());

                for v in val {
                    buffer.push_str(format!("\n{:?}", v).as_str());
                }
                common::logger::trace!("{}", buffer);
            }
        }

        let send_ret = send.do_send(collect_data);

        if send_ret.is_err() {
            common::logger::error!("plan_entry({}) - send failed\n{}", name, send_ret.unwrap_err());
        }
        else {
            common::logger::debug!("plan_entry({}) - collect and send done", name);
        }
    }
}

fn get_plans(vp : Vec<PlanTemplate>) -> Result<Vec<(String, Box<dyn CollectPlan>, Box<dyn SendPlan>)>, Box<dyn std::error::Error>> {
    let mut v = Vec::new();

    for p in vp {
        let name = p.name.clone();
        let real_plan = match make_plans(p) {
            Ok(ok) => Ok(ok),
            Err(e) => Err(err_def::system::ParsingError::new(make_err_msg!("{}", e), None))
        }?;

        v.push((name, real_plan.0, real_plan.1));
    }

    Ok(v)
}

pub(crate) fn start_plan_threads(vp : Vec<PlanTemplate>) -> Result<(), Box<dyn Error>> {
    let plans = match get_plans(vp) {
        Ok(ok) => Ok(ok),
        Err(e) => Err(err_def::system::ParsingError::new(make_err_msg!("{}", e), None))
    }?;

    std::thread::scope(|s| {
        for p in plans {
            s.spawn(|| {
                plan_entry(p);
            });
        }

        loop {
            //do etc logic
            std::thread::sleep(time::Duration::from_secs(30));
        }
    });

    Ok(())
}