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
		module.add("db")?;
		module.add("id")?;
		module.add("ip")?;
		module.add("ns")?;
		module.add("origin")?;
		module.add("sc")?;
		module.add("sd")?;
		module.add("token")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("db", Func::from(|v: Any| run("session::db", v.0)))?;
		module.set("id", Func::from(|v: Any| run("session::id", v.0)))?;
		module.set("ip", Func::from(|v: Any| run("session::ip", v.0)))?;
		module.set("ns", Func::from(|v: Any| run("session::ns", v.0)))?;
		module.set("origin", Func::from(|v: Any| run("session::origin", v.0)))?;
		module.set("sc", Func::from(|v: Any| run("session::sc", v.0)))?;
		module.set("sd", Func::from(|v: Any| run("session::sd", v.0)))?;
		module.set("token", Func::from(|v: Any| run("session::token", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("db", Func::from(|v: Any| run("session::db", v.0)))?;
		default.set("id", Func::from(|v: Any| run("session::id", v.0)))?;
		default.set("ip", Func::from(|v: Any| run("session::ip", v.0)))?;
		default.set("ns", Func::from(|v: Any| run("session::ns", v.0)))?;
		default.set("origin", Func::from(|v: Any| run("session::origin", v.0)))?;
		default.set("sc", Func::from(|v: Any| run("session::sc", v.0)))?;
		default.set("sd", Func::from(|v: Any| run("session::sd", v.0)))?;
		default.set("token", Func::from(|v: Any| run("session::token", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
