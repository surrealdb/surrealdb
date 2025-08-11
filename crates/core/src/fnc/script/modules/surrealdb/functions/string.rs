use super::run;
use crate::fnc::script::modules::impl_module_def;

mod distance;
mod html;
mod is;
mod semver;
mod similarity;

pub struct Package;

impl_module_def!(
	Package,
	"string",
	"concat" => run,
	"contains" => run,
	"distance" => (distance::Package),
	"ends_with" => run,
	"html" => (html::Package),
	"is" => (is::Package),
	"join" => run,
	"len" => run,
	"lowercase" => run,
	"matches" => run,
	"repeat" => run,
	"replace" => run,
	"reverse" => run,
	"similarity" => (similarity::Package),
	"slice" => run,
	"slug" => run,
	"split" => run,
	"starts_with" => run,
	"trim" => run,
	"uppercase" => run,
	"words" => run,
	"semver" => (semver::Package)
);
