use super::fut;
use crate::sql::value::Value;
use js::Async;
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
		module.add("head")?;
		module.add("get")?;
		module.add("put")?;
		module.add("post")?;
		module.add("patch")?;
		module.add("delete")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("head", Func::from(Async(|v: Any| fut("http::head", v.0))))?;
		module.set("get", Func::from(Async(|v: Any| fut("http::get", v.0))))?;
		module.set("put", Func::from(Async(|v: Any| fut("http::put", v.0))))?;
		module.set("post", Func::from(Async(|v: Any| fut("http::post", v.0))))?;
		module.set("patch", Func::from(Async(|v: Any| fut("http::patch", v.0))))?;
		module.set("delete", Func::from(Async(|v: Any| fut("http::delete", v.0))))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("head", Func::from(Async(|v: Any| fut("http::head", v.0))))?;
		default.set("get", Func::from(Async(|v: Any| fut("http::get", v.0))))?;
		default.set("put", Func::from(Async(|v: Any| fut("http::put", v.0))))?;
		default.set("post", Func::from(Async(|v: Any| fut("http::post", v.0))))?;
		default.set("patch", Func::from(Async(|v: Any| fut("http::patch", v.0))))?;
		default.set("delete", Func::from(Async(|v: Any| fut("http::delete", v.0))))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
