use std::cell::RefCell;
use std::time::{Duration, Instant};

use anyhow::Result;
use js::prelude::*;
use js::{CatchResultExt, Ctx, Function, Module, Promise, async_with};

use super::modules::surrealdb::query::QueryContext;
use super::modules::{loader, resolver};
use super::{classes, fetch, globals, modules};
use crate::cnf::{SCRIPTING_MAX_MEMORY_LIMIT, SCRIPTING_MAX_STACK_SIZE};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::val::Value;

/// Insert query data into the context,
///
/// # Safety
/// Caller must ensure that the runtime from which `Ctx` originates cannot
/// outlife 'a.
pub unsafe fn create_query_data<'a>(
	context: &'a Context,
	opt: &'a Options,
	doc: Option<&'a CursorDoc>,
	ctx: &Ctx<'_>,
) -> Result<(), js::Error> {
	unsafe {
		// remove the restricting lifetime.
		let ctx = Ctx::from_raw(ctx.as_raw());

		ctx.store_userdata(QueryContext {
			context,
			opt,
			doc,
			pending: RefCell::new(None),
		})
		.expect("userdata shouldn't be in use");

		Ok(())
	}
}

pub async fn run(
	context: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	src: &str,
	arg: Vec<Value>,
) -> Result<Value> {
	// Check the context
	if context.is_done(true).await? {
		return Ok(Value::None);
	}

	// Scripting functions are pretty heavy so make the increase pretty heavy.
	let opt = opt.dive(4)?;

	//TODO: Maybe check memory usage?

	let instant_start = Instant::now();
	let time_limit = Duration::from_millis(*crate::cnf::SCRIPTING_MAX_TIME_LIMIT as u64);

	// Create a JavaScript context
	let run = js::AsyncRuntime::new().unwrap();
	// Explicitly set max stack size to 256 KiB
	run.set_max_stack_size(*SCRIPTING_MAX_STACK_SIZE).await;
	// Explicitly set max memory size to 2 MB
	run.set_memory_limit(*SCRIPTING_MAX_MEMORY_LIMIT).await;
	// Ensure scripts are cancelled with context
	let cancellation = context.cancellation();
	let handler = Box::new(move || cancellation.is_done() || instant_start.elapsed() > time_limit);
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
	async_with!(ctx => |ctx| {
		let res = async {
			// Get the context global object
			let global = ctx.globals();
			// SAFETY: This is safe because the runtime only lives for the duration of this
			// function. For the entire duration of which context, opt, txn and doc are valid.
			unsafe{ create_query_data(context, &opt, doc, &ctx) }?;
			// Register the fetch module as a global function
			fetch::register(&ctx)?;
			// Register the surrealdb module as a global object
			let (module, promise) = Module::evaluate_def::<modules::surrealdb::Package, _>(ctx.clone(), "surrealdb")?;
			promise.finish::<()>()?;
			global.set("surrealdb",
				module.get::<_, js::Value>("default")?,
			)?;
			// Register the console module as a global object
			let console = globals::console::console(&ctx)?;
			global.set("console", console)?;
			// Register the special SurrealDB types as classes
			classes::init(&ctx)?;
			// Load the script as a module and evaluate it
			let (module, promise) = Module::declare(ctx.clone(),"script", src)?.eval()?;
			promise.into_future::<()>().await?;
			// Attempt to fetch the main export
			let fnc = module.get::<_, Function>("default")?;
			// Extract the doc if any
			let doc = doc.map(|v| v.doc.as_ref());
			// Execute the main function
			let promise = fnc.call::<_, Promise>((This(doc), Rest(arg)))?.into_future::<Value>();
			promise.await
		}.await;
		// Catch and convert any errors
		res.catch(&ctx).map_err(Error::from)
	})
	.await.map_err(anyhow::Error::new)
}
