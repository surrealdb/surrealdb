use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"string::similarity",
	"fuzzy" => run,
	"jaro" => run,
	"jaro_winkler" => run,
	"sorensen_dice" => run,
	"smithwaterman" => run
);
