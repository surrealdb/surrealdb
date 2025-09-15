use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"time",
	"ceil" => run,
	"day" => run,
	"floor" => run,
	"format" => run,
	"group" => run,
	"hour" => run,
	"max" => run,
	"min" => run,
	"mins" => run,
	"minute" => run,
	"month" => run,
	"nano" => run,
	"micros" => run,
	"millis" => run,
	"now" => run,
	"round" => run,
	"second" => run,
	"secs" => run,
	"timezone" => run,
	"unix" => run,
	"wday" => run,
	"week" => run,
	"yday" => run,
	"year" => run,
	"from_nanos" => run,
	"from_micros" => run,
	"from_millis" => run,
	"from_secs" => run,
	"from_ulid" => run,
	"from_unix" => run,
	"from_uuid" => run,
	"is_leap_year" => run
);
