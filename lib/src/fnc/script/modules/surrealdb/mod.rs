use crate::fnc::script::modules::impl_module_def;
use js::{module::ModuleDef, Ctx, Module, Result, Value};

mod functions;

pub struct Package;

impl_module_def!(
	Package,
	"surrealdb",
	"functions" => (functions::Package),
	"version" => (env!("CARGO_PKG_VERSION"))
);

fn pkg<'js, D>(ctx: Ctx<'js>, name: &str) -> Result<Value<'js>>
where
	D: ModuleDef,
{
	Module::evaluate_def::<D, _>(ctx, name)?.get::<_, js::Value>("default")
}
