// Specify the imports
use crate::sql::value::Value;
use js::{prelude::Rest, Ctx, Object, Result};
/// Log the input values as INFO
#[js::function]
pub fn log(args: Rest<Value>) {
	info!("{}", args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" "));
}
/// Log the input values as INFO
#[js::function]
pub fn info(args: Rest<Value>) {
	info!("{}", args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" "));
}
/// Log the input values as WARN
#[js::function]
pub fn warn(args: Rest<Value>) {
	warn!("{}", args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" "));
}
/// Log the input values as ERROR
#[js::function]
pub fn error(args: Rest<Value>) {
	error!("{}", args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" "));
}
/// Log the input values as DEBUG
#[js::function]
pub fn debug(args: Rest<Value>) {
	debug!("{}", args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" "));
}
/// Log the input values as TRACE
#[js::function]
pub fn trace(args: Rest<Value>) {
	trace!("{}", args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" "));
}

pub fn console<'js>(ctx: &Ctx<'js>) -> Result<Object<'js>> {
	let console = Object::new(ctx.clone())?;
	console.set("log", js_log)?;
	console.set("info", js_info)?;
	console.set("warn", js_warn)?;
	console.set("error", js_error)?;
	console.set("debug", js_debug)?;
	console.set("trace", js_trace)?;
	Ok(console)
}
