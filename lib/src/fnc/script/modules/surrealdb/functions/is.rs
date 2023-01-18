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
		module.add("alphanum")?;
		module.add("alpha")?;
		module.add("ascii")?;
		module.add("datetime")?;
		module.add("domain")?;
		module.add("email")?;
		module.add("hexadecimal")?;
		module.add("latitude")?;
		module.add("longitude")?;
		module.add("numeric")?;
		module.add("semver")?;
		module.add("url")?;
		module.add("uuid")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("alphanum", Func::from(|v: Any| run("is::alphanum", v.0)))?;
		module.set("alpha", Func::from(|v: Any| run("is::alpha", v.0)))?;
		module.set("ascii", Func::from(|v: Any| run("is::ascii", v.0)))?;
		module.set("datetime", Func::from(|v: Any| run("is::datetime", v.0)))?;
		module.set("domain", Func::from(|v: Any| run("is::domain", v.0)))?;
		module.set("email", Func::from(|v: Any| run("is::email", v.0)))?;
		module.set("hexadecimal", Func::from(|v: Any| run("is::hexadecimal", v.0)))?;
		module.set("latitude", Func::from(|v: Any| run("is::latitude", v.0)))?;
		module.set("longitude", Func::from(|v: Any| run("is::longitude", v.0)))?;
		module.set("numeric", Func::from(|v: Any| run("is::numeric", v.0)))?;
		module.set("semver", Func::from(|v: Any| run("is::semver", v.0)))?;
		module.set("url", Func::from(|v: Any| run("is::url", v.0)))?;
		module.set("uuid", Func::from(|v: Any| run("is::uuid", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("alphanum", Func::from(|v: Any| run("is::alphanum", v.0)))?;
		default.set("alpha", Func::from(|v: Any| run("is::alpha", v.0)))?;
		default.set("ascii", Func::from(|v: Any| run("is::ascii", v.0)))?;
		default.set("datetime", Func::from(|v: Any| run("is::datetime", v.0)))?;
		default.set("domain", Func::from(|v: Any| run("is::domain", v.0)))?;
		default.set("email", Func::from(|v: Any| run("is::email", v.0)))?;
		default.set("hexadecimal", Func::from(|v: Any| run("is::hexadecimal", v.0)))?;
		default.set("latitude", Func::from(|v: Any| run("is::latitude", v.0)))?;
		default.set("longitude", Func::from(|v: Any| run("is::longitude", v.0)))?;
		default.set("numeric", Func::from(|v: Any| run("is::numeric", v.0)))?;
		default.set("semver", Func::from(|v: Any| run("is::semver", v.0)))?;
		default.set("url", Func::from(|v: Any| run("is::url", v.0)))?;
		default.set("uuid", Func::from(|v: Any| run("is::uuid", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
