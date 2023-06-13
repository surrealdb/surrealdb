use super::classes;
use super::globals;
use super::modules;
use super::modules::loader;
use super::modules::resolver;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::value::Value;
use js::async_with;
use js::prelude::Promise;
use js::prelude::Rest;
use js::prelude::This;
use js::CatchResultExt;
use js::Function;
use js::Module;

pub async fn run(
	ctx: &Context<'_>,
	_opt: &Options,
	src: &str,
	arg: Vec<Value>,
) -> Result<Value, Error> {
	// Check the context
	if ctx.is_done() {
		return Ok(Value::None);
	}
	// Get the optional doc
	let doc = ctx.doc();
	// Create an JavaScript context
	let run = js::AsyncRuntime::new().unwrap();
	// Explicitly set max stack size to 256 KiB
	run.set_max_stack_size(262_144).await;
	// Explicitly set max memory size to 2 MB
	run.set_memory_limit(2_000_000).await;
	// Ensure scripts are cancelled with context
	let cancellation = ctx.cancellation();
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

		let res = async move {
			// Get the context global object
			let global = ctx.globals();
			// Register the surrealdb module as a global object
			global.set(
				"surrealdb",
				Module::evaluate_def::<modules::surrealdb::Package, _>(ctx, "surrealdb")?
					.get::<_, js::Value>("default")?,
			)?;
			// Register the fetch function to the globals
			global.init_def::<globals::fetch::Fetch>()?;
			// Register the console function to the globals
			global.init_def::<globals::console::Console>()?;
			// Register the special SurrealDB types as classes
			global.init_def::<classes::duration::Duration>()?;
			global.init_def::<classes::record::Record>()?;
			global.init_def::<classes::uuid::Uuid>()?;
			// Attempt to compile the script
			let res = ctx.compile("script", src)?;
			// Attempt to fetch the main export
			let fnc = res.get::<_, Function>("default")?;
			// Execute the main function
			let promise: Promise<Value> = fnc.call((This(doc), Rest(arg)))?;
			promise.await
		}.await;

		res.catch(ctx).map_err(Error::from)
	})
	.await
}
