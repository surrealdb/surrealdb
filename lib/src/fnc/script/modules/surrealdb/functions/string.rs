use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"string",
	"concat" => run,
	"contains" => run,
	"endsWith" => run,
	"join" => run,
	"len" => run,
	"lowercase" => run,
	"repeat" => run,
	"replace" => run,
	"reverse" => run,
	"slice" => run,
	"slug" => run,
	"split" => run,
	"startsWith" => run,
	"trim" => run,
	"uppercase" => run,
	"words" => run
);
