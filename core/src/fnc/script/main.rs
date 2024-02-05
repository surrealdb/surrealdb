use super::classes;
use super::fetch;
use super::globals;
use super::modules;
use super::modules::loader;
use super::modules::resolver;
use super::modules::surrealdb::query::QueryContext;
use super::modules::surrealdb::query::QUERY_DATA_PROP_NAME;
use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::value::Value;
use js::async_with;
use js::object::Property;
use js::prelude::Promise;
use js::prelude::Rest;
use js::prelude::This;
use js::CatchResultExt;
use js::Class;
use js::Ctx;
use js::Function;
use js::Module;

/// Insert query data into the context,
///
/// # Safety
/// Caller must ensure that the runtime from which `Ctx` originates cannot outlife 'a.
pub unsafe fn create_query_data<'a>(
	context: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	doc: Option<&'a CursorDoc<'a>>,
	ctx: &Ctx<'_>,
) -> Result<(), js::Error> {
	// remove the restricting lifetime.
	let ctx = Ctx::from_raw(ctx.as_raw());

	let object = Class::instance(
		ctx.clone(),
		QueryContext {
			context,
			opt,
			txn,
			doc,
		},
	)?;

	// make the query data not enumerable, writeable, or configurable.
	let prop = Property::from(object);
	ctx.globals().prop(QUERY_DATA_PROP_NAME, prop)?;

	Ok(())
}

pub async fn run(
	context: &Context<'_>,
	opt: &Options,
	txn: &Transaction,
	doc: Option<&CursorDoc<'_>>,
	src: &str,
	arg: Vec<Value>,
) -> Result<Value, Error> {
	// Check the context
	if context.is_done() {
		return Ok(Value::None);
	}
	// Create an JavaScript context
	let run = js::AsyncRuntime::new().unwrap();
	// Explicitly set max stack size to 256 KiB
	run.set_max_stack_size(262_144).await;
	// Explicitly set max memory size to 2 MB
	run.set_memory_limit(2_000_000).await;
	// Ensure scripts are cancelled with context
	let cancellation = context.cancellation();
	let handler = Box::new(move || cancellation.is_done());
	run.set_interrupt_handler(Some(handler)).await;
	// Create an execution context
	let ctx = js::AsyncContext::full(&run).await.unwrap();
	// Set the module resolver and loader
	run.set_loader(resolver(), loader()).await;
	// Create the main function structure
	let src = format!(
		"export default async function() {{ try {{ {src} }} catch(e) {{ return (e instanceof Error) ? e : new Error(e); }} }}"
	);

	// Attempt to execute the script
	async_with!(ctx => |ctx|{
		let res = async{
			// register all classes to the runtime.
			// Get the context global object
			let global = ctx.globals();

			// SAFETY: This is safe because the runtime only lives for the duration of this
			// function. For the entire duration of which context, opt, txn and doc are valid.
			unsafe{ create_query_data(context,opt,txn,doc,&ctx) }?;
			// Register the surrealdb module as a global object
			global.set(
				"surrealdb",
				Module::evaluate_def::<modules::surrealdb::Package, _>(ctx.clone(), "surrealdb")?
					.get::<_, js::Value>("default")?,
			)?;
			fetch::register(&ctx)?;
			let console = globals::console::console(&ctx)?;
			// Register the console function to the globals
			global.set("console",console)?;
			// Register the special SurrealDB types as classes
			classes::init(&ctx)?;
			// Attempt to compile the script
			let res = ctx.clone().compile("script", src)?;
			// Attempt to fetch the main export
			let fnc = res.get::<_, Function>("default")?;
			// Extract the doc if any
			let doc = doc.map(|v|v.doc.as_ref());
			// Execute the main function
			let promise: Promise<Value> = fnc.call((This(doc), Rest(arg)))?;
			promise.await
		}.await;

		res.catch(&ctx).map_err(Error::from)
	})
	.await
}
