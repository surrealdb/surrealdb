use super::{pkg, run};
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

mod from;

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"time",
	"day" => run,
	"floor" => run,
	"format" => run,
	"group" => run,
	"hour" => run,
	"mins" => run,
	"minute" => run,
	"month" => run,
	"nano" => run,
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
	"from" => (from::Package)
);
