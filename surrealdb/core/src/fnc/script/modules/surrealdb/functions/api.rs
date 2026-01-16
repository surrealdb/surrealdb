use js::prelude::Async;

use super::fut;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

mod req;
mod res;

impl_module_def!(
	Package,
	"api",
	"invoke" => fut Async,
	"timeout" => fut Async,
	"res" => (res::Package),
	"req" => (req::Package),
);
