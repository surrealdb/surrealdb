use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

pub struct Package;

type Any = Rest<Value>;

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
