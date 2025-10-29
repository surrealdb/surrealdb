use js::prelude::Async;

use super::{fut, run};
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"set",
	"add" => run,
	"all" => fut Async,
	"any" => fut Async,
	"complement" => run,
	"contains" => run,
	"difference" => run,
	"intersect" => run,
	"is_empty" => run,
	"len" => run,
	"remove" => run,
	"union" => run
);
