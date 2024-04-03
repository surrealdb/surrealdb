use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"string::is",
	"alphanum" => run,
	"alpha" => run,
	"ascii" => run,
	"datetime" => run,
	"domain" => run,
	"email" => run,
	"email" => run,
	"hexadecimal" => run,
	"latitude" => run,
	"longitude" => run,
	"numeric" => run,
	"semver" => run,
	"url" => run,
	"uuid" => run
);
