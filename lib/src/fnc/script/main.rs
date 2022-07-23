use super::classes;
use super::executor::Executor;
use super::globals;
use super::modules::loader;
use super::modules::resolver;
use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;
use js::Function;
use js::Promise;
use js::Rest;
use js::This;

pub async fn run(
	ctx: &Context<'_>,
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
	// Create an execution context
	let ctx = js::Context::full(&run).unwrap();
	// Set the module resolver and loader
	run.set_loader(resolver(), loader());
	// Enable async code in the runtime
	run.spawn_executor(&exe).detach();
	// Create the main function structure
	let src = format!("export async function main() {{ {} }}", src);
	// Attempt to execute the script
	let res: Result<Promise<Value>, js::Error> = ctx.with(|ctx| {
		// Get the context global object
		let global = ctx.globals();
		// Register the surrealdb module as a global object
		global.init_def::<globals::surrealdb::Package>().unwrap();
		// Register the Duration type as a global class
		global.init_def::<classes::Duration>().unwrap();
		// Register the Record type as a global class
		global.init_def::<classes::Record>().unwrap();
		// Register the Uuid type as a global class
		global.init_def::<classes::Uuid>().unwrap();
		// Attempt to compile the script
		let res = ctx.compile("script", src)?;
		// Attempt to fetch the main export
		let fnc = res.get::<_, Function>("main")?;
		// Execute the main function
		fnc.call((This(doc), Rest(arg)))
	});
	// Return the script result
	let res = match res {
		// The script executed successfully
		Ok(v) => match exe.run(v).await {
			// The promise fulfilled successfully
			Ok(v) => Ok(v),
			// There was an error awaiting the promise
			Err(e) => Err(Error::from(e)),
		},
		// There was an error running the script
		Err(e) => Err(Error::from(e)),
	};
	// Return the result
	res
}
