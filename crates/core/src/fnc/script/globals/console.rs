// Specify the imports
use js::prelude::Rest;
use js::{Coerced, Ctx, Object, Result};
/// Log the input values as INFO
#[js::function]
pub fn log(args: Rest<Coerced<String>>) {
	info!("{}", args.0.join(" "));
}
/// Log the input values as INFO
#[js::function]
pub fn info(args: Rest<Coerced<String>>) {
	info!("{}", args.0.join(" "));
}
/// Log the input values as WARN
#[js::function]
pub fn warn(args: Rest<Coerced<String>>) {
	warn!("{}", args.0.join(" "));
}
/// Log the input values as ERROR
#[js::function]
pub fn error(args: Rest<Coerced<String>>) {
	error!("{}", args.0.join(" "));
}
/// Log the input values as DEBUG
#[js::function]
pub fn debug(args: Rest<Coerced<String>>) {
	debug!("{}", args.0.join(" "));
}
/// Log the input values as TRACE
#[js::function]
pub fn trace(args: Rest<Coerced<String>>) {
	trace!("{}", args.0.join(" "));
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
