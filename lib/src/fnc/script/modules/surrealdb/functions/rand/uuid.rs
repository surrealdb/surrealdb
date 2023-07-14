use super::super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"rand::uuid",
	"v4" => run,
	"v7" => run
);
