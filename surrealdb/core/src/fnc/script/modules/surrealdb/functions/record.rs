use js::prelude::Async;

use super::{fut, run};
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"record",
	"exists" => fut Async,
	"id" => run,
	"table" => run,
	"tb" => run,
	"is_edge" => fut Async
);
