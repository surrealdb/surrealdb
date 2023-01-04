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
		module.add("bool")?;
		module.add("datetime")?;
		module.add("decimal")?;
		module.add("duration")?;
		module.add("float")?;
		module.add("int")?;
		module.add("number")?;
		module.add("point")?;
		module.add("regex")?;
		module.add("string")?;
		module.add("table")?;
		module.add("thing")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("bool", Func::from(|v: Any| run("type::bool", v.0)))?;
		module.set("datetime", Func::from(|v: Any| run("type::datetime", v.0)))?;
		module.set("decimal", Func::from(|v: Any| run("type::decimal", v.0)))?;
		module.set("duration", Func::from(|v: Any| run("type::duration", v.0)))?;
		module.set("float", Func::from(|v: Any| run("type::float", v.0)))?;
		module.set("int", Func::from(|v: Any| run("type::int", v.0)))?;
		module.set("number", Func::from(|v: Any| run("type::number", v.0)))?;
		module.set("point", Func::from(|v: Any| run("type::point", v.0)))?;
		module.set("regex", Func::from(|v: Any| run("type::regex", v.0)))?;
		module.set("string", Func::from(|v: Any| run("type::string", v.0)))?;
		module.set("table", Func::from(|v: Any| run("type::table", v.0)))?;
		module.set("thing", Func::from(|v: Any| run("type::thing", v.0)))?;
		// Set default export
		let default = Object::new(ctx)?;
		default.set("bool", Func::from(|v: Any| run("type::bool", v.0)))?;
		default.set("datetime", Func::from(|v: Any| run("type::datetime", v.0)))?;
		default.set("decimal", Func::from(|v: Any| run("type::decimal", v.0)))?;
		default.set("duration", Func::from(|v: Any| run("type::duration", v.0)))?;
		default.set("float", Func::from(|v: Any| run("type::float", v.0)))?;
		default.set("int", Func::from(|v: Any| run("type::int", v.0)))?;
		default.set("number", Func::from(|v: Any| run("type::number", v.0)))?;
		default.set("point", Func::from(|v: Any| run("type::point", v.0)))?;
		default.set("regex", Func::from(|v: Any| run("type::regex", v.0)))?;
		default.set("string", Func::from(|v: Any| run("type::string", v.0)))?;
		default.set("table", Func::from(|v: Any| run("type::table", v.0)))?;
		default.set("thing", Func::from(|v: Any| run("type::thing", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
