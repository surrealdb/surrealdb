use super::classes;
use super::executor::Executor;
use super::globals;
use super::modules;
use super::modules::loader;
use super::modules::resolver;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::value::Value;
use js::Function;
use js::Module;
use js::Promise;
use js::Rest;
use js::This;

pub async fn run(
	ctx: &Context<'_>,
	_opt: &Options,
	_txn: &Transaction,
	doc: Option<&Value>,
	src: &str,
	arg: Vec<Value>,
) -> Result<Value, Error> {
	// Check the context
	if ctx.is_done() {
		return Ok(Value::None);
	}
	// Create a new agent
	let exe = Executor::default();
	// Create an JavaScript context
	let run = js::Runtime::new().unwrap();
	// Explicitly set max stack size to 256 KiB
	run.set_max_stack_size(262_144);
	// Explicitly set max memory size to 2 MB
	run.set_memory_limit(2_000_000);
	// Ensure scripts are cancelled with context
	let cancellation = ctx.cancellation();
	run.set_interrupt_handler(Some(Box::new(move || cancellation.is_done())));
	// Create an execution context
	let ctx = js::Context::full(&run).unwrap();
	// Set the module resolver and loader
	run.set_loader(resolver(), loader());
	// Enable async code in the runtime
	run.spawn_executor(&exe).detach();
	// Create the main function structure
	let src = format!(
		"export default async function() {{ try {{ {src} }} catch(e) {{ return (e instanceof Error) ? e : new Error(e); }} }}"
	);
	// Attempt to execute the script
	let res: Result<Promise<Value>, js::Error> = ctx.with(|ctx| {
		// Get the context global object
		let global = ctx.globals();
		// Register the surrealdb module as a global object
		global.set(
			"surrealdb",
			Module::new_def::<modules::surrealdb::Package, _>(ctx, "surrealdb")?
				.eval()?
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
		fnc.call((This(doc), Rest(arg)))
	});
	// Return the script result
	match res {
		// The script executed successfully
		Ok(v) => match exe.run(v).await {
			// The promise fulfilled successfully
			Ok(v) => Ok(v),
			// There was an error awaiting the promise
			Err(e) => Err(Error::from(e)),
		},
		// There was an error running the script
		Err(e) => Err(Error::from(e)),
	}
}
