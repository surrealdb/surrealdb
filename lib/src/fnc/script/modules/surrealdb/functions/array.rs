use super::pkg;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

mod sort;
pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"array",
	"add" => run,
	"all" => run,
	"any" => run,
	"append" => run,
	"combine" => run,
	"complement" => run,
	"concat" => run,
	"difference" => run,
	"distinct" => run,
	"flatten" => run,
	"group" => run,
	"insert" => run,
	"intersect" => run,
	"len" => run,
	"max" => run,
	"min" => run,
	"pop" => run,
	"push" => run,
	"prepend" => run,
	"remove" => run,
	"reverse" => run,
	"slice" => run,
	"sort" => (sort::Package),
	"union" => run
);
