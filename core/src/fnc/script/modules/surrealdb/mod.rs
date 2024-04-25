use crate::sql::{value as parse_value, Value as SurValue};
use js::class::OwnedBorrow;
use js::prelude::Coerced;
use js::Exception;
use js::{module::ModuleDef, Class, Ctx, Function, Module, Result, String as JsString, Value};
use reblessive::tree::Stk;

use self::query::{QueryContext, QUERY_DATA_PROP_NAME};

mod functions;
pub mod query;

#[non_exhaustive]
pub struct Package;

#[js::function]
async fn value(ctx: Ctx<'_>, value: Coerced<String>) -> Result<SurValue> {
	let value = parse_value(&value.0).map_err(|e| Exception::throw_type(&ctx, &e.to_string()))?;
	let this = ctx.globals().get::<_, OwnedBorrow<QueryContext>>(QUERY_DATA_PROP_NAME)?;
	let value = Stk::enter_run(|stk| async {
		value
			.compute(stk, this.context, this.opt, this.txn, this.doc)
			.await
			.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))
	})
	.await?;
	Ok(value)
}

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

		let value_func = Function::new(ctx.clone(), js_value)?.with_name("value")?;
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
	promise.finish()?;
	m.get::<_, js::Value>("default")
}
