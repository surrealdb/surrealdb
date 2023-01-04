use super::pkg;
use crate::ctx::Context;
use crate::fnc;
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

mod array;
mod crypto;
mod duration;
mod geo;
mod http;
mod is;
mod math;
mod meta;
mod parse;
mod rand;
mod session;
mod string;
mod time;
mod r#type;

pub struct Package;

type Any = Rest<Value>;

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("array")?;
		module.add("count")?;
		module.add("crypto")?;
		module.add("duration")?;
		module.add("geo")?;
		module.add("http")?;
		module.add("is")?;
		module.add("math")?;
		module.add("meta")?;
		module.add("not")?;
		module.add("parse")?;
		module.add("rand")?;
		module.add("string")?;
		module.add("time")?;
		module.add("type")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("array", pkg::<array::Package>(ctx, "array"))?;
		module.set("count", Func::from(|v: Any| run("count", v.0)))?;
		module.set("crypto", pkg::<crypto::Package>(ctx, "crypto"))?;
		module.set("duration", pkg::<duration::Package>(ctx, "duration"))?;
		module.set("geo", pkg::<geo::Package>(ctx, "geo"))?;
		module.set("http", pkg::<http::Package>(ctx, "http"))?;
		module.set("is", pkg::<is::Package>(ctx, "is"))?;
		module.set("math", pkg::<math::Package>(ctx, "math"))?;
		module.set("meta", pkg::<meta::Package>(ctx, "meta"))?;
		module.set("not", Func::from(|v: Any| run("not", v.0)))?;
		module.set("parse", pkg::<parse::Package>(ctx, "parse"))?;
		module.set("rand", pkg::<rand::Package>(ctx, "rand"))?;
		module.set("string", pkg::<string::Package>(ctx, "string"))?;
		module.set("time", pkg::<time::Package>(ctx, "time"))?;
		module.set("type", pkg::<r#type::Package>(ctx, "type"))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("array", pkg::<array::Package>(ctx, "array"))?;
		default.set("count", Func::from(|v: Any| run("count", v.0)))?;
		default.set("crypto", pkg::<crypto::Package>(ctx, "crypto"))?;
		default.set("duration", pkg::<duration::Package>(ctx, "duration"))?;
		default.set("geo", pkg::<geo::Package>(ctx, "geo"))?;
		default.set("http", pkg::<http::Package>(ctx, "http"))?;
		default.set("is", pkg::<is::Package>(ctx, "is"))?;
		default.set("math", pkg::<math::Package>(ctx, "math"))?;
		default.set("meta", pkg::<meta::Package>(ctx, "meta"))?;
		default.set("not", Func::from(|v: Any| run("not", v.0)))?;
		default.set("parse", pkg::<parse::Package>(ctx, "parse"))?;
		default.set("rand", pkg::<rand::Package>(ctx, "rand"))?;
		default.set("string", pkg::<string::Package>(ctx, "string"))?;
		default.set("time", pkg::<time::Package>(ctx, "time"))?;
		default.set("type", pkg::<r#type::Package>(ctx, "type"))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}

fn run(name: &str, args: Vec<Value>) -> Result<Value> {
	// Create a default context
	let ctx = Context::background();
	// Process the called function
	let res = fnc::synchronous(&ctx, name, args);
	// Convert any response error
	res.map_err(|err| js::Error::Exception {
		message: err.to_string(),
		file: String::from(""),
		line: -1,
		stack: String::from(""),
	})
}

async fn fut(name: &str, args: Vec<Value>) -> Result<Value> {
	// Create a default context
	let ctx = Context::background();
	// Process the called function
	let res = fnc::asynchronous(&ctx, name, args).await;
	// Convert any response error
	res.map_err(|err| js::Error::Exception {
		message: err.to_string(),
		file: String::from(""),
		line: -1,
		stack: String::from(""),
	})
}
