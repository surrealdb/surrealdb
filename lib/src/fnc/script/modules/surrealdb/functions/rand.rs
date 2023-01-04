use super::super::pkg;
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

mod uuid;

pub struct Package;

type Any = Rest<Value>;

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("bool")?;
		module.add("enum")?;
		module.add("float")?;
		module.add("guid")?;
		module.add("int")?;
		module.add("string")?;
		module.add("time")?;
		module.add("uuid")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("bool", Func::from(|v: Any| run("rand::bool", v.0)))?;
		module.set("enum", Func::from(|v: Any| run("rand::enum", v.0)))?;
		module.set("float", Func::from(|v: Any| run("rand::float", v.0)))?;
		module.set("guid", Func::from(|v: Any| run("rand::guid", v.0)))?;
		module.set("int", Func::from(|v: Any| run("rand::int", v.0)))?;
		module.set("string", Func::from(|v: Any| run("rand::string", v.0)))?;
		module.set("time", Func::from(|v: Any| run("rand::time", v.0)))?;
		module.set("uuid", pkg::<uuid::Package>(ctx, "uuid"))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("bool", Func::from(|v: Any| run("rand::bool", v.0)))?;
		default.set("enum", Func::from(|v: Any| run("rand::enum", v.0)))?;
		default.set("float", Func::from(|v: Any| run("rand::float", v.0)))?;
		default.set("guid", Func::from(|v: Any| run("rand::guid", v.0)))?;
		default.set("int", Func::from(|v: Any| run("rand::int", v.0)))?;
		default.set("string", Func::from(|v: Any| run("rand::string", v.0)))?;
		default.set("time", Func::from(|v: Any| run("rand::time", v.0)))?;
		default.set("uuid", pkg::<uuid::Package>(ctx, "uuid"))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
