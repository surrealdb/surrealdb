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
		module.add("day")?;
		module.add("floor")?;
		module.add("format")?;
		module.add("group")?;
		module.add("hour")?;
		module.add("mins")?;
		module.add("month")?;
		module.add("nano")?;
		module.add("now")?;
		module.add("round")?;
		module.add("secs")?;
		module.add("timezone")?;
		module.add("unix")?;
		module.add("wday")?;
		module.add("week")?;
		module.add("yday")?;
		module.add("year")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("day", Func::from(|v: Any| run("time::day", v.0)))?;
		module.set("floor", Func::from(|v: Any| run("time::floor", v.0)))?;
		module.set("format", Func::from(|v: Any| run("time::format", v.0)))?;
		module.set("group", Func::from(|v: Any| run("time::group", v.0)))?;
		module.set("hour", Func::from(|v: Any| run("time::hour", v.0)))?;
		module.set("mins", Func::from(|v: Any| run("time::mins", v.0)))?;
		module.set("month", Func::from(|v: Any| run("time::month", v.0)))?;
		module.set("nano", Func::from(|v: Any| run("time::nano", v.0)))?;
		module.set("now", Func::from(|v: Any| run("time::now", v.0)))?;
		module.set("round", Func::from(|v: Any| run("time::round", v.0)))?;
		module.set("secs", Func::from(|v: Any| run("time::secs", v.0)))?;
		module.set("timezone", Func::from(|v: Any| run("time::timezone", v.0)))?;
		module.set("unix", Func::from(|v: Any| run("time::unix", v.0)))?;
		module.set("wday", Func::from(|v: Any| run("time::wday", v.0)))?;
		module.set("week", Func::from(|v: Any| run("time::week", v.0)))?;
		module.set("yday", Func::from(|v: Any| run("time::yday", v.0)))?;
		module.set("year", Func::from(|v: Any| run("time::year", v.0)))?;
		// Set default export
		let default = Object::new(ctx)?;
		default.set("day", Func::from(|v: Any| run("time::day", v.0)))?;
		default.set("floor", Func::from(|v: Any| run("time::floor", v.0)))?;
		default.set("format", Func::from(|v: Any| run("time::format", v.0)))?;
		default.set("group", Func::from(|v: Any| run("time::group", v.0)))?;
		default.set("hour", Func::from(|v: Any| run("time::hour", v.0)))?;
		default.set("mins", Func::from(|v: Any| run("time::mins", v.0)))?;
		default.set("month", Func::from(|v: Any| run("time::month", v.0)))?;
		default.set("nano", Func::from(|v: Any| run("time::nano", v.0)))?;
		default.set("now", Func::from(|v: Any| run("time::now", v.0)))?;
		default.set("round", Func::from(|v: Any| run("time::round", v.0)))?;
		default.set("secs", Func::from(|v: Any| run("time::secs", v.0)))?;
		default.set("timezone", Func::from(|v: Any| run("time::timezone", v.0)))?;
		default.set("unix", Func::from(|v: Any| run("time::unix", v.0)))?;
		default.set("wday", Func::from(|v: Any| run("time::wday", v.0)))?;
		default.set("week", Func::from(|v: Any| run("time::week", v.0)))?;
		default.set("yday", Func::from(|v: Any| run("time::yday", v.0)))?;
		default.set("year", Func::from(|v: Any| run("time::year", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
