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

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("functions")?;
		module.add("version")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("functions", pkg::<functions::Package>(ctx, "functions"))?;
		module.set("version", env!("CARGO_PKG_VERSION"))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("functions", pkg::<functions::Package>(ctx, "functions"))?;
		default.set("version", env!("CARGO_PKG_VERSION"))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}

fn pkg<'js, D>(ctx: Ctx<'js>, name: &str) -> Result<Value<'js>>
where
	D: ModuleDef,
{
	Module::new_def::<D, _>(ctx, name)?.eval()?.get::<_, js::Value>("default")
}
