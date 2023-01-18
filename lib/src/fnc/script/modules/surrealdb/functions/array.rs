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
		module.add("combine")?;
		module.add("complement")?;
		module.add("concat")?;
		module.add("difference")?;
		module.add("distinct")?;
		module.add("flatten")?;
		module.add("group")?;
		module.add("insert")?;
		module.add("intersect")?;
		module.add("len")?;
		module.add("sort")?;
		module.add("union")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("combine", Func::from(|v: Any| run("array::combine", v.0)))?;
		module.set("complement", Func::from(|v: Any| run("array::complement", v.0)))?;
		module.set("concat", Func::from(|v: Any| run("array::concat", v.0)))?;
		module.set("difference", Func::from(|v: Any| run("array::difference", v.0)))?;
		module.set("distinct", Func::from(|v: Any| run("array::distinct", v.0)))?;
		module.set("flatten", Func::from(|v: Any| run("array::flatten", v.0)))?;
		module.set("group", Func::from(|v: Any| run("array::group", v.0)))?;
		module.set("insert", Func::from(|v: Any| run("array::insert", v.0)))?;
		module.set("intersect", Func::from(|v: Any| run("array::intersect", v.0)))?;
		module.set("len", Func::from(|v: Any| run("array::len", v.0)))?;
		module.set("sort", Func::from(|v: Any| run("array::sort", v.0)))?;
		module.set("union", Func::from(|v: Any| run("array::union", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("combine", Func::from(|v: Any| run("array::combine", v.0)))?;
		default.set("complement", Func::from(|v: Any| run("array::complement", v.0)))?;
		default.set("concat", Func::from(|v: Any| run("array::concat", v.0)))?;
		default.set("difference", Func::from(|v: Any| run("array::difference", v.0)))?;
		default.set("distinct", Func::from(|v: Any| run("array::distinct", v.0)))?;
		default.set("flatten", Func::from(|v: Any| run("array::flatten", v.0)))?;
		default.set("group", Func::from(|v: Any| run("array::group", v.0)))?;
		default.set("insert", Func::from(|v: Any| run("array::insert", v.0)))?;
		default.set("intersect", Func::from(|v: Any| run("array::intersect", v.0)))?;
		default.set("len", Func::from(|v: Any| run("array::len", v.0)))?;
		default.set("sort", Func::from(|v: Any| run("array::sort", v.0)))?;
		default.set("union", Func::from(|v: Any| run("array::union", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
