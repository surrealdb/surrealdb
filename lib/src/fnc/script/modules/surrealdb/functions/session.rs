use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"session",
	"db" => run,
	"id" => run,
	"ip" => run,
	"ns" => run,
	"origin" => run,
	"sc" => run,
	"sd" => run,
	"token" => run
);
