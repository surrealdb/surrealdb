use super::run;
use crate::fnc::script::modules::impl_module_def;

mod from;

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
	"from" => (from::Package)
);
