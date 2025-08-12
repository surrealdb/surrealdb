pub mod api;
pub(super) mod invoke;

use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::val::{Array, Object, Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RequestMiddleware(pub Vec<(String, Vec<Value>)>);

pub type CollectedMiddleware<'a> = Vec<(&'a String, &'a Vec<Value>)>;

impl RequestMiddleware {
	pub fn collect<'a>(slice: &'a [&'a RequestMiddleware]) -> Result<CollectedMiddleware<'a>> {
		let mut middleware: CollectedMiddleware<'a> = Vec::new();
		for map in slice {
			for (k, v) in map.0.iter() {
				match k.split_once("::") {
					Some(("api", _)) => middleware.push((k, v)),
					Some(("fn", _)) => {
						bail!(Error::Unimplemented(
							"Custom middleware are not yet supported".into(),
						));
					}
					_ => {
						fail!("Found a middleware which is unparsable")
					}
				}
			}
		}

		Ok(middleware)
	}
}

impl InfoStructure for RequestMiddleware {
	fn structure(self) -> Value {
		Value::Object(Object(
			self.0
				.into_iter()
				.map(|(k, v)| {
					let value = v
						.iter()
						.map(|x| Value::Strand(Strand::new(x.to_string()).unwrap()))
						.collect();

					(k, Value::Array(Array(value)))
				})
				.collect(),
		))
	}
}
