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

mod hash;

pub struct Package;

type Any = Rest<Value>;

impl ModuleDef for Package {
	fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
		module.add("default")?;
		module.add("area")?;
		module.add("bearing")?;
		module.add("centroid")?;
		module.add("distance")?;
		module.add("hash")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("area", Func::from(|v: Any| run("geo::area", v.0)))?;
		module.set("bearing", Func::from(|v: Any| run("geo::bearing", v.0)))?;
		module.set("centroid", Func::from(|v: Any| run("geo::centroid", v.0)))?;
		module.set("distance", Func::from(|v: Any| run("geo::distance", v.0)))?;
		module.set("hash", pkg::<hash::Package>(ctx, "hash"))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("area", Func::from(|v: Any| run("geo::area", v.0)))?;
		default.set("bearing", Func::from(|v: Any| run("geo::bearing", v.0)))?;
		default.set("centroid", Func::from(|v: Any| run("geo::centroid", v.0)))?;
		default.set("distance", Func::from(|v: Any| run("geo::distance", v.0)))?;
		default.set("hash", pkg::<hash::Package>(ctx, "hash"))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
