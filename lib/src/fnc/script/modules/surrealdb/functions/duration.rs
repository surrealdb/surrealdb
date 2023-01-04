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
		module.add("days")?;
		module.add("hours")?;
		module.add("mins")?;
		module.add("secs")?;
		module.add("weeks")?;
		module.add("years")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("days", Func::from(|v: Any| run("duration::days", v.0)))?;
		module.set("hours", Func::from(|v: Any| run("duration::hours", v.0)))?;
		module.set("mins", Func::from(|v: Any| run("duration::mins", v.0)))?;
		module.set("secs", Func::from(|v: Any| run("duration::secs", v.0)))?;
		module.set("weeks", Func::from(|v: Any| run("duration::weeks", v.0)))?;
		module.set("years", Func::from(|v: Any| run("duration::years", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("days", Func::from(|v: Any| run("duration::days", v.0)))?;
		default.set("hours", Func::from(|v: Any| run("duration::hours", v.0)))?;
		default.set("mins", Func::from(|v: Any| run("duration::mins", v.0)))?;
		default.set("secs", Func::from(|v: Any| run("duration::secs", v.0)))?;
		default.set("weeks", Func::from(|v: Any| run("duration::weeks", v.0)))?;
		default.set("years", Func::from(|v: Any| run("duration::years", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
