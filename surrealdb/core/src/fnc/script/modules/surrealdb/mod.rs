use js::module::ModuleDef;
use js::{Class, Ctx, Function, Module, Result, String as JsString, Value};

mod functions;
pub mod query;

pub struct Package;

impl ModuleDef for Package {
	fn declare(decls: &js::module::Declarations) -> js::Result<()> {
		decls.declare("default")?;
		decls.declare("functions")?;
		decls.declare("version")?;
		decls.declare("value")?;
		decls.declare("query")?;
		decls.declare("Query")?;
		Ok(())
	}

	fn evaluate<'js>(ctx: &js::Ctx<'js>, exports: &js::module::Exports<'js>) -> js::Result<()> {
		let default = js::Object::new(ctx.clone())?;
		let package = pkg::<functions::Package>(ctx, "functions")?;
		exports.export("functions", package.clone())?;
		default.set("functions", package)?;

		let version = JsString::from_str(ctx.clone(), env!("CARGO_PKG_VERSION"))?;
		exports.export("version", version.clone())?;
		default.set("version", version)?;

		let query_func = Function::new(ctx.clone(), query::js_query)?.with_name("query")?;
		exports.export("query", query_func.clone())?;
		default.set("query", query_func)?;

		let value_func = Function::new(ctx.clone(), query::js_query)?.with_name("value")?;
		exports.export("value", value_func.clone())?;
		default.set("value", value_func)?;

		let query_object = (Class::<query::Query>::create_constructor(ctx))?;
		exports.export("Query", query_object.clone())?;
		default.set("Query", query_object)?;

		exports.export("default", default)?;
		Ok(())
	}
}

fn pkg<'js, D>(ctx: &Ctx<'js>, name: &str) -> Result<Value<'js>>
where
	D: ModuleDef,
{
	let (m, promise) = Module::evaluate_def::<D, _>(ctx.clone(), name)?;
	promise.finish::<()>()?;
	m.get::<_, js::Value>("default")
}
