use std::sync::OnceLock;
use mypip_types::interface::GlobalLayout;

pub(crate) static GLOBAL_REFER : OnceLock<&'static dyn GlobalLayout> = OnceLock::new();