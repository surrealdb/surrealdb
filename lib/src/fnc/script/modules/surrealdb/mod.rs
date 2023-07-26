use crate::fnc::script::modules::impl_module_def;
use js::{module::ModuleDef, Class, Ctx, Function, Module, Result, Value};

mod functions;
pub mod query;

pub struct Package;

impl ModuleDef for Package {
	fn declare(decls: &mut js::module::Declarations) -> js::Result<()> {
		decls.declare("default")?;
		decls.declare("functions")?;
		decls.declare("version")?;
		decls.declare("query")?;
		decls.declare("Query")?;
		Ok(())
	}

	fn evaluate<'js>(ctx: &js::Ctx<'js>, exports: &mut js::module::Exports<'js>) -> js::Result<()> {
		let default = js::Object::new(ctx.clone())?;
		let package = impl_module_def!(ctx, "surrealdb", "functions", (functions::Package),);
		exports.export("functions", package.clone())?;
		default.set("functions", package)?;

		let version = impl_module_def!(ctx, "surrealdb", "version", (env!("CARGO_PKG_VERSION")),);
		exports.export("version", version.clone())?;
		default.set("version", version)?;

		let query_func = Function::new(ctx.clone(), query::js_query)?.with_name("query")?;
		exports.export("query", query_func.clone())?;
		default.set("query", query_func)?;

		let query_object = impl_module_def!(
			ctx,
			"surrealdb",
			"Query",
			(Class::<query::Query>::create_constructor(ctx)),
		)?;

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
	Module::evaluate_def::<D, _>(ctx.clone(), name)?.get::<_, js::Value>("default")
}
