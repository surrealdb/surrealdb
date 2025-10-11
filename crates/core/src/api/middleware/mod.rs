pub mod api;
pub(super) mod invoke;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::statements::info::InfoStructure;
use crate::val::{Array, Object, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub(crate) struct RequestMiddleware(pub(crate) Vec<(String, Vec<Value>)>);

impl InfoStructure for RequestMiddleware {
	fn structure(self) -> Value {
		Value::Object(Object(
			self.0
				.into_iter()
				.map(|(k, v)| {
					let value = v.iter().map(|x| Value::String(x.to_string())).collect();

					(k, Value::Array(Array(value)))
				})
				.collect(),
		))
	}
}
