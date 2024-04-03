use super::fut;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"http",
	"head" => fut Async,
	"get" => fut Async,
	"put" => fut Async,
	"post" => fut Async,
	"patch" => fut Async,
	"delete" => fut Async
);
