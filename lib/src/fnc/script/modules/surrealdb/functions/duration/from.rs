use super::super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

pub struct Package;

type Any = Rest<Value>;

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
