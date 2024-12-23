use super::fut;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"record",
	"exists" => fut Async,
	"id" => run,
	"table" => run,
	"tb" => run,
	"refs" => fut Async
);
