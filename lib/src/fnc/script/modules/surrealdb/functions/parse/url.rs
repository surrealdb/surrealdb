use super::super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"parse::url",
	"domain" => run,
	"fragment" => run,
	"host" => run,
	"path" => run,
	"port" => run,
	"query" => run,
	"scheme" => run
);
