use std::{error::Error, fmt, sync::Arc};

use js::{Ctx, Result};

mod body;
mod classes;
mod func;
mod stream;
#[cfg(feature = "http")]
mod util;

use classes::{Blob, FormData, Headers, Request, Response};
use func::Fetch;

// Anoyingly errors aren't clone,
// But with how we implement streams RequestError must be clone.
/// Error returned by the request.
#[derive(Debug, Clone)]
pub enum RequestError {
	Reqwest(Arc<reqwest::Error>),
}

impl fmt::Display for RequestError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {
			RequestError::Reqwest(ref e) => writeln!(f, "request error: {e}"),
		}
	}
}

impl Error for RequestError {}

/// Register the fetch types in the context.
pub fn register(ctx: Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	globals.init_def::<Fetch>()?;

	globals.init_def::<Response>()?;
	globals.init_def::<Request>()?;
	globals.init_def::<Blob>()?;
	globals.init_def::<FormData>()?;
	globals.init_def::<Headers>()?;

	Ok(())
}
