use super::super::pkg;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

mod hash;

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"geo",
	"area" => run,
	"bearing" => run,
	"centroid" => run,
	"distance" => run,
	"hash" => (hash::Package)
);
