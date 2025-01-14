use super::fut;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"schema",
	"event" => fut Async,
	"events" => fut Async,
	"field" => fut Async,
	"fields" => fut Async,
	"function" => fut Async,
	"functions" => fut Async,
	"index" => fut Async,
	"indexes" => fut Async,
	"table" => fut Async,
	"tables" => fut Async
);
