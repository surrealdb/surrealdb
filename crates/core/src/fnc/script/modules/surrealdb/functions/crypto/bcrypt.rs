use js::prelude::Async;

use super::super::fut;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"crypto::bcrypt",
	"compare" => fut Async,
	"generate" => fut Async
);
