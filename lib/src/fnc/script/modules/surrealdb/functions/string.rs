use super::run;
use crate::fnc::script::modules::impl_module_def;

mod distance;
mod similarity;
pub struct Package;

impl_module_def!(
	Package,
	"string",
	"concat" => run,
	"contains" => run,
	"distance" => (distance::Package),
	"endsWith" => run,
	"join" => run,
	"len" => run,
	"lowercase" => run,
	"repeat" => run,
	"replace" => run,
	"reverse" => run,
	"similarity" => (similarity::Package),
	"slice" => run,
	"slug" => run,
	"split" => run,
	"startsWith" => run,
	"trim" => run,
	"uppercase" => run,
	"words" => run
);
