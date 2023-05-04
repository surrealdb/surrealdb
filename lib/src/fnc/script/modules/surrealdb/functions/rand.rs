use super::super::pkg;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

mod uuid;

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"rand",
	"bool" => run,
	"enum" => run,
	"float" => run,
	"guid" => run,
	"int" => run,
	"string" => run,
	"time" => run,
	"ulid" => run,
	"uuid" => (uuid::Package)
);
