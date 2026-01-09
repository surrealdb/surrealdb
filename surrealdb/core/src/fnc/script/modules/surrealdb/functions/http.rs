use js::prelude::Async;

use super::fut;
use crate::fnc::script::modules::impl_module_def;

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
