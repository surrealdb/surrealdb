use super::super::fut;
use crate::fnc::script::modules::impl_module_def;
use js::prelude::Async;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"crypto::scrypt",
	"compare" => fut Async,
	"generate" => fut Async
);
