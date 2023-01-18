use super::super::run;
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
		module.add("domain")?;
		module.add("fragment")?;
		module.add("host")?;
		module.add("path")?;
		module.add("port")?;
		module.add("query")?;
		module.add("scheme")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("domain", Func::from(|v: Any| run("parse::url::domain", v.0)))?;
		module.set("fragment", Func::from(|v: Any| run("parse::url::fragment", v.0)))?;
		module.set("host", Func::from(|v: Any| run("parse::url::host", v.0)))?;
		module.set("path", Func::from(|v: Any| run("parse::url::path", v.0)))?;
		module.set("port", Func::from(|v: Any| run("parse::url::port", v.0)))?;
		module.set("query", Func::from(|v: Any| run("parse::url::query", v.0)))?;
		module.set("scheme", Func::from(|v: Any| run("parse::url::scheme", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("domain", Func::from(|v: Any| run("parse::url::domain", v.0)))?;
		default.set("fragment", Func::from(|v: Any| run("parse::url::fragment", v.0)))?;
		default.set("host", Func::from(|v: Any| run("parse::url::host", v.0)))?;
		default.set("path", Func::from(|v: Any| run("parse::url::path", v.0)))?;
		default.set("port", Func::from(|v: Any| run("parse::url::port", v.0)))?;
		default.set("query", Func::from(|v: Any| run("parse::url::query", v.0)))?;
		default.set("scheme", Func::from(|v: Any| run("parse::url::scheme", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
