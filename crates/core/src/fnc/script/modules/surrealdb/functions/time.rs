use super::run;
use crate::fnc::script::modules::impl_module_def;

mod from;
mod is;

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
	"from" => (from::Package),
	"is" => (is::Package)
);
