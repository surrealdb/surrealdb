use std::{error::Error, fmt, io, sync::Arc};

use js::{Ctx, Result};

mod body;
mod classes;
mod func;
mod stream;
mod util;

use body::{Body, BodyKind};
use classes::{Blob, FormData, Headers, Request, Response};
use func::Fetch;

#[derive(Debug, Clone)]
pub enum RequestError {
	// Anoyingly errors aren't clone,
	// But with how we implement streams RequestError must be clone.
	Io(Arc<io::Error>),
	Reqwest(Arc<reqwest::Error>),
}

impl fmt::Display for RequestError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {
			RequestError::Io(ref e) => writeln!(f, "io error: {e}"),
			RequestError::Reqwest(ref e) => writeln!(f, "request error: {e}"),
		}
	}
}

impl Error for RequestError {}

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
