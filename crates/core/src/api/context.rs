use http::HeaderMap;

use crate::expr::{Bytesize, Duration};
use anyhow::Result;

use super::middleware::{CollectedMiddleware, invoke::InvokeMiddleware};

#[derive(Default, Debug)]
pub struct InvocationContext {
	pub request_body_max: Option<Bytesize>,
	pub request_body_raw: bool,
	pub response_body_raw: bool,
	pub response_headers: Option<HeaderMap>,
	pub timeout: Option<Duration>,
}

impl InvocationContext {
	pub fn apply_middleware<'a>(&'a mut self, middleware: CollectedMiddleware<'a>) -> Result<()> {
		for entry in middleware {
			entry.invoke(self)?;
		}

		Ok(())
	}
}
