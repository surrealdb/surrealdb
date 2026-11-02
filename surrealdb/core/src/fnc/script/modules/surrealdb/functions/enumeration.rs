use js::prelude::Async;

use super::fut;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"enum",
	"cast" => fut Async,
	"max" => fut Async,
	"min" => fut Async,
	"rank" => fut Async,
	"values" => fut Async
);
