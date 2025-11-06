use super::fut;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

pub struct Package;

impl_module_def!(
	Package,
	"migration",
	"diagnose" => fut Async
);
