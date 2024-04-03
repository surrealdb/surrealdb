use super::fut;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

mod is;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"type",
	"bool" => run,
	"datetime" => run,
	"decimal" => run,
	"duration" => run,
	"field" => fut Async,
	"fields" => fut Async,
	"float" => run,
	"int" => run,
	"is" => (is::Package),
	"number" => run,
	"point" => run,
	"regex" => run,
	"string" => run,
	"table" => run,
	"thing" => run,
	"range" => run
);
