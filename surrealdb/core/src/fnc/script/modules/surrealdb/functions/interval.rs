use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"interval",
	"days" => run,
	"hours" => run,
	"justify_days" => run,
	"justify_hours" => run,
	"justify_interval" => run,
	"make" => run,
	"micros" => run,
	"mins" => run,
	"months" => run,
	"secs" => run,
	"years" => run
);
