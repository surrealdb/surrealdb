use crate::fnc::script::modules::impl_module_def;
use js::Created;
use js::Ctx;
use js::Loaded;
use js::Module;
use js::ModuleDef;
use js::Native;
use js::Object;
use js::Result;
use js::Value;

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
