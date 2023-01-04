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

mod argon2;
mod bcrypt;
mod pbkdf2;
mod scrypt;

pub struct Package;

type Any = Rest<Value>;

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("md5")?;
		module.add("sha1")?;
		module.add("sha256")?;
		module.add("sha512")?;
		module.add("argon2")?;
		module.add("bcrypt")?;
		module.add("pbkdf2")?;
		module.add("scrypt")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("md5", Func::from(|v: Any| run("crypto::md5", v.0)))?;
		module.set("sha1", Func::from(|v: Any| run("crypto::sha1", v.0)))?;
		module.set("sha256", Func::from(|v: Any| run("crypto::sha256", v.0)))?;
		module.set("sha512", Func::from(|v: Any| run("crypto::sha512", v.0)))?;
		module.set("argon2", pkg::<argon2::Package>(ctx, "argon2"))?;
		module.set("bcrypt", pkg::<bcrypt::Package>(ctx, "bcrypt"))?;
		module.set("pbkdf2", pkg::<pbkdf2::Package>(ctx, "pbkdf2"))?;
		module.set("scrypt", pkg::<scrypt::Package>(ctx, "scrypt"))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("md5", Func::from(|v: Any| run("crypto::md5", v.0)))?;
		default.set("sha1", Func::from(|v: Any| run("crypto::sha1", v.0)))?;
		default.set("sha256", Func::from(|v: Any| run("crypto::sha256", v.0)))?;
		default.set("sha512", Func::from(|v: Any| run("crypto::sha512", v.0)))?;
		default.set("argon2", pkg::<argon2::Package>(ctx, "argon2"))?;
		default.set("bcrypt", pkg::<bcrypt::Package>(ctx, "bcrypt"))?;
		default.set("pbkdf2", pkg::<pbkdf2::Package>(ctx, "pbkdf2"))?;
		default.set("scrypt", pkg::<scrypt::Package>(ctx, "scrypt"))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
