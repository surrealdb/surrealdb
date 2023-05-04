use super::fut;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"http",
	"head" => fut Async,
	"get" => fut Async,
	"put" => fut Async,
	"post" => fut Async,
	"patch" => fut Async,
	"delete" => fut Async
);
