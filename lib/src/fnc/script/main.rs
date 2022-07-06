use super::classes;
use super::executor::Executor;
use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;
use js::Promise;

pub async fn run(
	ctx: &Context<'_>,
	src: &str,
	arg: Vec<Value>,
	doc: Option<&Value>,
) -> Result<Value, Error> {
	// Check the context
	let _ = ctx.check()?;
	// Create a new agent
	let exe = Executor::default();
	// Create an JavaScript context
	let run = js::Runtime::new().unwrap();
	// Create an execution context
	let ctx = js::Context::full(&run).unwrap();
	// Enable async code in the runtime
	run.spawn_executor(&exe).detach();
	// Convert the arguments to JavaScript
	let args = Value::from(arg);
	// Convert the current document to JavaScript
	let this = doc.map_or(&Value::None, |v| v);
	// Create the main function structure
	let src = format!("(async function() {{ {} }}).apply(self, args)", src);
	// Attempt to execute the script
	let res: Result<Promise<Value>, js::Error> = ctx.with(|ctx| {
		// Get the context global object
		let global = ctx.globals();
		// Register the Duration type as a global class
		global.init_def::<classes::Duration>().unwrap();
		// Register the Record type as a global class
		global.init_def::<classes::Record>().unwrap();
		// Register the Uuid type as a global class
		global.init_def::<classes::Uuid>().unwrap();
		// Register the document as a global object
		global.prop("self", this).unwrap();
		// Register the args as a global object
		global.prop("args", args).unwrap();
		// Attempt to execute the script
		ctx.eval(src)
	});
	// Return the script result
	let res = match res {
		// The script executed successfully
		Ok(v) => match exe.run(v).await {
			// The promise fulfilled successfully
			Ok(v) => Ok(v),
			// There was an error awaiting the promise
			Err(e) => Err(Error::InvalidScript {
				message: e.to_string(),
			}),
		},
		// There was an error running the script
		Err(e) => Err(Error::InvalidScript {
			message: e.to_string(),
		}),
	};
	// Return the result
	res
}
