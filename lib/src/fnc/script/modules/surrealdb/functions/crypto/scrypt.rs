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
		module.add("compare")?;
		module.add("generate")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("compare", Func::from(|v: Any| run("scrypt::argon2::compare", v.0)))?;
		module.set("generate", Func::from(|v: Any| run("scrypt::argon2::generate", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("compare", Func::from(|v: Any| run("scrypt::argon2::compare", v.0)))?;
		default.set("generate", Func::from(|v: Any| run("scrypt::argon2::generate", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
