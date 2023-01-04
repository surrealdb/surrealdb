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
		module.add("abs")?;
		module.add("bottom")?;
		module.add("ceil")?;
		module.add("fixed")?;
		module.add("floor")?;
		module.add("interquartile")?;
		module.add("max")?;
		module.add("mean")?;
		module.add("median")?;
		module.add("midhinge")?;
		module.add("min")?;
		module.add("mode")?;
		module.add("nearestrank")?;
		module.add("percentile")?;
		module.add("pow")?;
		module.add("product")?;
		module.add("round")?;
		module.add("spread")?;
		module.add("sqrt")?;
		module.add("stddev")?;
		module.add("sum")?;
		module.add("top")?;
		module.add("trimean")?;
		module.add("variance")?;
		Ok(())
	}

	fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
		// Set specific exports
		module.set("abs", Func::from(|v: Any| run("math::abs", v.0)))?;
		module.set("bottom", Func::from(|v: Any| run("math::bottom", v.0)))?;
		module.set("ceil", Func::from(|v: Any| run("math::ceil", v.0)))?;
		module.set("fixed", Func::from(|v: Any| run("math::fixed", v.0)))?;
		module.set("floor", Func::from(|v: Any| run("math::floor", v.0)))?;
		module.set("interquartile", Func::from(|v: Any| run("math::interquartile", v.0)))?;
		module.set("max", Func::from(|v: Any| run("math::max", v.0)))?;
		module.set("mean", Func::from(|v: Any| run("math::mean", v.0)))?;
		module.set("median", Func::from(|v: Any| run("math::median", v.0)))?;
		module.set("midhinge", Func::from(|v: Any| run("math::midhinge", v.0)))?;
		module.set("min", Func::from(|v: Any| run("math::min", v.0)))?;
		module.set("mode", Func::from(|v: Any| run("math::mode", v.0)))?;
		module.set("nearestrank", Func::from(|v: Any| run("math::nearestrank", v.0)))?;
		module.set("percentile", Func::from(|v: Any| run("math::percentile", v.0)))?;
		module.set("pow", Func::from(|v: Any| run("math::pow", v.0)))?;
		module.set("product", Func::from(|v: Any| run("math::product", v.0)))?;
		module.set("round", Func::from(|v: Any| run("math::round", v.0)))?;
		module.set("spread", Func::from(|v: Any| run("math::spread", v.0)))?;
		module.set("sqrt", Func::from(|v: Any| run("math::sqrt", v.0)))?;
		module.set("stddev", Func::from(|v: Any| run("math::stddev", v.0)))?;
		module.set("sum", Func::from(|v: Any| run("math::sum", v.0)))?;
		module.set("top", Func::from(|v: Any| run("math::top", v.0)))?;
		module.set("trimean", Func::from(|v: Any| run("math::trimean", v.0)))?;
		module.set("variance", Func::from(|v: Any| run("math::variance", v.0)))?;
		// Set default exports
		let default = Object::new(ctx)?;
		default.set("abs", Func::from(|v: Any| run("math::abs", v.0)))?;
		default.set("bottom", Func::from(|v: Any| run("math::bottom", v.0)))?;
		default.set("ceil", Func::from(|v: Any| run("math::ceil", v.0)))?;
		default.set("fixed", Func::from(|v: Any| run("math::fixed", v.0)))?;
		default.set("floor", Func::from(|v: Any| run("math::floor", v.0)))?;
		default.set("interquartile", Func::from(|v: Any| run("math::interquartile", v.0)))?;
		default.set("max", Func::from(|v: Any| run("math::max", v.0)))?;
		default.set("mean", Func::from(|v: Any| run("math::mean", v.0)))?;
		default.set("median", Func::from(|v: Any| run("math::median", v.0)))?;
		default.set("midhinge", Func::from(|v: Any| run("math::midhinge", v.0)))?;
		default.set("min", Func::from(|v: Any| run("math::min", v.0)))?;
		default.set("mode", Func::from(|v: Any| run("math::mode", v.0)))?;
		default.set("nearestrank", Func::from(|v: Any| run("math::nearestrank", v.0)))?;
		default.set("percentile", Func::from(|v: Any| run("math::percentile", v.0)))?;
		default.set("pow", Func::from(|v: Any| run("math::pow", v.0)))?;
		default.set("product", Func::from(|v: Any| run("math::product", v.0)))?;
		default.set("round", Func::from(|v: Any| run("math::round", v.0)))?;
		default.set("spread", Func::from(|v: Any| run("math::spread", v.0)))?;
		default.set("sqrt", Func::from(|v: Any| run("math::sqrt", v.0)))?;
		default.set("stddev", Func::from(|v: Any| run("math::stddev", v.0)))?;
		default.set("sum", Func::from(|v: Any| run("math::sum", v.0)))?;
		default.set("top", Func::from(|v: Any| run("math::top", v.0)))?;
		default.set("trimean", Func::from(|v: Any| run("math::trimean", v.0)))?;
		default.set("variance", Func::from(|v: Any| run("math::variance", v.0)))?;
		module.set("default", default)?;
		// Everything ok
		Ok(())
	}
}
