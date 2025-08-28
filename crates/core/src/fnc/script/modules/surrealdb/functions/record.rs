use js::prelude::Async;

use super::{fut, run};
use crate::fnc::script::modules::impl_module_def;

mod is;

pub struct Package;

impl_module_def!(
	Package,
	"record",
	"exists" => fut Async,
	"id" => run,
	"is" => (is::Package),
	"table" => run,
	"tb" => run
);
