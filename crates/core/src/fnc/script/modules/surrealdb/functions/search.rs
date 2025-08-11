use super::fut;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

pub struct Package;

impl_module_def!(
	Package,
	"search",
	"analyze" => fut Async,
	"highlight" => fut Async,
	"linear" => fut Async,
	"offsets" => fut Async,
	"rrf" => fut Async,
	"score" => fut Async
);
