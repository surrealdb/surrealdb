use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"duration",
	"days" => run,
	"hours" => run,
	"micros" => run,
	"millis" => run,
	"mins" => run,
	"nanos" => run,
	"secs" => run,
	"weeks" => run,
	"years" => run,
	"from_days" => run,
	"from_hours" => run,
	"from_micros" => run,
	"from_millis" => run,
	"from_mins" => run,
	"from_nanos" => run,
	"from_secs" => run,
	"from_weeks" => run,
	"from_years" => run
);
