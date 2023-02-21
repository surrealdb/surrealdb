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
		module.add("concat")?;
		module.add("endsWith")?;
		module.add("join")?;
		module.add("len")?;
		module.add("lowercase")?;
		module.add("repeat")?;
		module.add("replace")?;
		module.add("reverse")?;
		module.add("slice")?;
		module.add("slug")?;
		module.add("split")?;
		module.add("startsWith")?;
		module.add("trim")?;
		module.add("uppercase")?;
		module.add("words")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("concat", Func::from(|v: Any| run("string::concat", v.0)))?;
		module.set("endsWith", Func::from(|v: Any| run("string::endsWith", v.0)))?;
		module.set("join", Func::from(|v: Any| run("string::join", v.0)))?;
		module.set("len", Func::from(|v: Any| run("string::len", v.0)))?;
		module.set("lowercase", Func::from(|v: Any| run("string::lowercase", v.0)))?;
		module.set("repeat", Func::from(|v: Any| run("string::repeat", v.0)))?;
		module.set("replace", Func::from(|v: Any| run("string::replace", v.0)))?;
		module.set("reverse", Func::from(|v: Any| run("string::reverse", v.0)))?;
		module.set("slice", Func::from(|v: Any| run("string::slice", v.0)))?;
		module.set("slug", Func::from(|v: Any| run("string::slug", v.0)))?;
		module.set("split", Func::from(|v: Any| run("string::split", v.0)))?;
		module.set("startsWith", Func::from(|v: Any| run("string::startsWith", v.0)))?;
		module.set("trim", Func::from(|v: Any| run("string::trim", v.0)))?;
		module.set("uppercase", Func::from(|v: Any| run("string::uppercase", v.0)))?;
		module.set("words", Func::from(|v: Any| run("string::words", v.0)))?;
		// Set default export
		let default = Object::new(ctx)?;
		default.set("concat", Func::from(|v: Any| run("string::concat", v.0)))?;
		default.set("endsWith", Func::from(|v: Any| run("string::endsWith", v.0)))?;
		default.set("join", Func::from(|v: Any| run("string::join", v.0)))?;
		default.set("len", Func::from(|v: Any| run("string::len", v.0)))?;
		default.set("lowercase", Func::from(|v: Any| run("string::lowercase", v.0)))?;
		default.set("repeat", Func::from(|v: Any| run("string::repeat", v.0)))?;
		default.set("replace", Func::from(|v: Any| run("string::replace", v.0)))?;
		default.set("reverse", Func::from(|v: Any| run("string::reverse", v.0)))?;
		default.set("slice", Func::from(|v: Any| run("string::slice", v.0)))?;
		default.set("slug", Func::from(|v: Any| run("string::slug", v.0)))?;
		default.set("split", Func::from(|v: Any| run("string::split", v.0)))?;
		default.set("startsWith", Func::from(|v: Any| run("string::startsWith", v.0)))?;
		default.set("trim", Func::from(|v: Any| run("string::trim", v.0)))?;
		default.set("uppercase", Func::from(|v: Any| run("string::uppercase", v.0)))?;
		default.set("words", Func::from(|v: Any| run("string::words", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
