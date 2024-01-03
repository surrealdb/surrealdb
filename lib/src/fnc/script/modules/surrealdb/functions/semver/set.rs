use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"semver::set",
	"major" => run,
	"minor" => run,
	"patch" => run
);
