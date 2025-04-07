use super::fut;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"file",
	//
	"bucket" => run,
	"key" => run,
	//
	"put" => fut Async,
	"put_if_not_exists" => fut Async,
	"get" => fut Async,
	"head" => fut Async,
	"delete" => fut Async,
	"copy" => fut Async,
	"copy_if_not_exists" => fut Async,
	"rename" => fut Async,
	"rename_if_not_exists" => fut Async,
	"exists" => fut Async,
	"list" => fut Async
);
