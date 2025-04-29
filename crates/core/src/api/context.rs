use http::HeaderMap;

use super::middleware::invoke::InvokeMiddleware;
use super::middleware::CollectedMiddleware;
use crate::err::Error;
use crate::sql::{Bytesize, Duration};

#[derive(Default, Debug)]
pub struct InvocationContext {
	pub request_body_max: Option<Bytesize>,
	pub request_body_raw: bool,
	pub response_body_raw: bool,
	pub response_headers: Option<HeaderMap>,
	pub timeout: Option<Duration>,
}

impl InvocationContext {
	pub fn apply_middleware<'a>(
		&'a mut self,
		middleware: CollectedMiddleware<'a>,
	) -> Result<(), Error> {
		for entry in middleware {
			entry.invoke(self)?;
		}

		Ok(())
	}
}
