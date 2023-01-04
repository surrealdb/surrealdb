use super::super::pkg;
use js::Created;
use js::Ctx;
use js::Loaded;
use js::Module;
use js::ModuleDef;
use js::Native;
use js::Object;
use js::Result;

mod email;
mod url;

pub struct Package;

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("email")?;
		module.add("url")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("email", pkg::<email::Package>(ctx, "email"))?;
		module.set("url", pkg::<url::Package>(ctx, "url"))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("email", pkg::<email::Package>(ctx, "email"))?;
		default.set("url", pkg::<url::Package>(ctx, "url"))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
