use crate::fnc::script::modules::impl_module_def;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

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
	Module::new_def::<D, _>(ctx, name)?.eval()?.get::<_, js::Value>("default")
}
