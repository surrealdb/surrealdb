use js::prelude::Async;

use super::fut;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"search",
	"analyze" => fut Async,
	"highlight" => fut Async,
	"offsets" => fut Async,
	"score" => fut Async
);
