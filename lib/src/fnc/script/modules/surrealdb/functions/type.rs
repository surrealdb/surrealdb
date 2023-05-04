use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"type",
	"bool" => run,
	"datetime" => run,
	"decimal" => run,
	"duration" => run,
	"float" => run,
	"int" => run,
	"number" => run,
	"point" => run,
	"regex" => run,
	"string" => run,
	"table" => run,
	"thing" => run
);
