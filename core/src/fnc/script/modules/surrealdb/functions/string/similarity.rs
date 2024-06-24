use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"string::similarity",
	"fuzzy" => run,
	"jaro" => run,
	"smithwaterman" => run
);
