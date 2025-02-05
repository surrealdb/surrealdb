use std::collections::BTreeMap;

use crate::{
	err::Error,
	sql::{Bytesize, Duration},
};

use super::middleware::{invoke::InvokeMiddleware, CollectedMiddleware};

#[derive(Default, Debug)]
pub struct RequestContext {
	pub max_body_size: Option<Bytesize>,
	pub timeout: Option<Duration>,
	pub headers: Option<BTreeMap<String, String>>,
}

impl RequestContext {
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
