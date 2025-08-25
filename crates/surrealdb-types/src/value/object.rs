use std::collections::BTreeMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::Value;
use crate::utils::escape::EscapeKey;

/// Represents an object with key-value pairs in SurrealDB
///
/// An object is a collection of key-value pairs where keys are strings and values can be of any
/// type. The underlying storage is a `BTreeMap<String, Value>` which maintains sorted keys.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Object(pub BTreeMap<String, Value>);

impl Object {
	/// Create a new empty object
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	/// Insert a key-value pair into the object
	pub fn insert<V>(&mut self, key: String, value: V) -> Option<Value>
	where
		V: Into<Value>,
	{
		self.0.insert(key, value.into())
	}
}

impl Display for Object {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"{{{}}}",
			self.0
				.iter()
				.map(|(k, v)| format!("{}: {}", EscapeKey(k), v))
				.collect::<Vec<String>>()
				.join(", ")
		)
	}
}
