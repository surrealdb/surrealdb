use crate::fnc::script::modules::impl_module_def;
use js::{module::ModuleDef, Class, Ctx, Function, Module, Result, Value};

mod functions;
mod query;

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
		exports.export(
			"functions",
			impl_module_def!(ctx, "surrealdb", "functions", (functions::Package),),
		)?;
		default.set(
			"functions",
			impl_module_def!(ctx, "surrealdb", "functions", (functions::Package),),
		)?;
		exports.export(
			"version",
			impl_module_def!(ctx, "surrealdb", "version", (env!("CARGO_PKG_VERSION")),),
		)?;
		default.set(
			"version",
			impl_module_def!(ctx, "surrealdb", "version", (env!("CARGO_PKG_VERSION")),),
		)?;

		let query_func = Function::new(ctx.clone(), query::js_query)?.with_name("query")?;
		exports.export("query", query_func.clone())?;
		default.set("query", query_func)?;
		exports.export(
			"Query",
			impl_module_def!(
				ctx,
				"surrealdb",
				"Query",
				(Class::<query::Query>::create_constructor(ctx)),
			),
		)?;
		default.set(
			"Query",
			impl_module_def!(
				ctx,
				"surrealdb",
				"Query",
				(Class::<query::Query>::create_constructor(ctx)),
			),
		)?;
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
