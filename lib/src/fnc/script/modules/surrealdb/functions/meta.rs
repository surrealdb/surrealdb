use super::run;
use crate::sql::value::Value;
use js::Created;
use js::Ctx;
use js::Func;
use js::Loaded;
use js::Module;
use js::ModuleDef;
use js::Native;
use js::Object;
use js::Rest;
use js::Result;

pub struct Package;

type Any = Rest<Value>;

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("id")?;
		module.add("table")?;
		module.add("tb")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("id", Func::from(|v: Any| run("meta::id", v.0)))?;
		module.set("table", Func::from(|v: Any| run("meta::table", v.0)))?;
		module.set("tb", Func::from(|v: Any| run("meta::tb", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("id", Func::from(|v: Any| run("meta::id", v.0)))?;
		default.set("table", Func::from(|v: Any| run("meta::table", v.0)))?;
		default.set("tb", Func::from(|v: Any| run("meta::tb", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
