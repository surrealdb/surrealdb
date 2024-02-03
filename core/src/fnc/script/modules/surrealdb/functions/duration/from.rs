use super::super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"duration::from",
	"days" => run,
	"hours" => run,
	"micros" => run,
	"millis" => run,
	"mins" => run,
	"nanos" => run,
	"secs" => run,
	"weeks" => run,
	"years" => run
);
