// Specify the imports
use std::fmt;

use js::prelude::Rest;
use js::{Coerced, Ctx, Object, Result};

pub struct Printer<'a>(&'a [Coerced<String>]);
impl fmt::Display for Printer<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (idx, x) in self.0.iter().enumerate() {
			if idx != 0 {
				write!(f, " ")?;
			}
			write!(f, "{}", x.0)?;
		}
		Ok(())
	}
}

/// Log the input values as INFO
#[js::function]
pub fn log(args: Rest<Coerced<String>>) {
	info!("{}", Printer(&args.0));
}
/// Log the input values as INFO
#[js::function]
pub fn info(args: Rest<Coerced<String>>) {
	info!("{}", Printer(&args.0));
}
/// Log the input values as WARN
#[js::function]
pub fn warn(args: Rest<Coerced<String>>) {
	warn!("{}", Printer(&args.0));
}
/// Log the input values as ERROR
#[js::function]
pub fn error(args: Rest<Coerced<String>>) {
	error!("{}", Printer(&args.0));
}
/// Log the input values as DEBUG
#[js::function]
pub fn debug(args: Rest<Coerced<String>>) {
	debug!("{}", Printer(&args.0));
}
/// Log the input values as TRACE
#[js::function]
pub fn trace(args: Rest<Coerced<String>>) {
	trace!("{}", Printer(&args.0));
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
