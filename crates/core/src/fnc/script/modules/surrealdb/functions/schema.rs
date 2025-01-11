use super::fut;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"schema",
	"fields" => fut Async,
	"indexes" => fut Async,
	"tables" => fut Async
);
