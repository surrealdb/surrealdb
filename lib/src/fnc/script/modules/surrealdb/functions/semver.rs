use super::run;
use crate::fnc::script::modules::impl_module_def;

mod increment;
mod set;
pub struct Package;

impl_module_def!(
	Package,
	"semver",
	"compare" => run,
	"major" => run,
	"minor" => run,
	"patch" => run,
	"increment" => (increment::Package),
	"set" => (set::Package)
);
