use js::prelude::Async;

use super::fut;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"value",
	"chain" => fut Async,
	"diff" => fut Async,
	"expect" => fut Async,
	"patch" => fut Async
);
