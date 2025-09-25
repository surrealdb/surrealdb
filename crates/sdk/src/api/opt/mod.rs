//! The different options and types for use in API functions

use std::borrow::Cow;

use anyhow::Context;

pub mod auth;
pub mod capabilities;

mod config;
pub(crate) mod endpoint;
mod export;
pub(crate) mod query;
mod resource;
mod tls;

pub use config::*;
pub use endpoint::*;
pub use export::*;
pub use query::*;
pub use resource::*;
use surrealdb_types::{Array, Kind, Object, SurrealValue, Value};
#[cfg(any(feature = "native-tls", feature = "rustls"))]
pub use tls::*;

#[derive(Debug)]
enum InnerOp {
	Add {
		path: String,
		value: Value,
	},
	Remove {
		path: String,
	},
	Replace {
		path: String,
		value: Value,
	},
	Change {
		path: String,
		value: String,
	},
}

impl SurrealValue for InnerOp {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(_))
	}

	fn into_value(self) -> Value {
		match self {
			InnerOp::Add {
				path,
				value,
			} => {
				// { "op": "add", "path": "/biscuits/1", "value": { "name": "Ginger Nut" } }
				let mut obj = Object::new();
				obj.insert("op".to_string(), Value::String("add".to_string()));
				obj.insert("path".to_string(), Value::String(path.to_string()));
				obj.insert("value".to_string(), value);
				Value::Object(obj)
			}
			InnerOp::Remove {
				path,
			} => {
				// { "op": "remove", "path": "/biscuits/1" }
				let mut obj = Object::new();
				obj.insert("op".to_string(), Value::String("remove".to_string()));
				obj.insert("path".to_string(), Value::String(path.to_string()));
				Value::Object(obj)
			}
			InnerOp::Replace {
				path,
				value,
			} => {
				// { "op": "replace", "path": "/biscuits/1", "value": { "name": "Ginger Nut" } }
				let mut obj = Object::new();
				obj.insert("op".to_string(), Value::String("replace".to_string()));
				obj.insert("path".to_string(), Value::String(path.to_string()));
				obj.insert("value".to_string(), value);
				Value::Object(obj)
			}
			InnerOp::Change {
				path,
				value,
			} => {
				// { "op": "change", "path": "/biscuits/1", "value": "name" }
				let mut obj = Object::new();
				obj.insert("op".to_string(), Value::String("change".to_string()));
				obj.insert("path".to_string(), Value::String(path.to_string()));
				obj.insert("value".to_string(), Value::String(value.to_string()));
				Value::Object(obj)
			}
		}
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		let Value::Object(mut obj) = value else {
			return Err(anyhow::anyhow!("Expected Object, got {:?}", value.kind()));
		};
		let op = obj.remove("op").context("Key 'op' missing")?;
		let op = op.into_string()?;

		match op.as_str() {
			"add" => {
				let path = obj.remove("path").context("Key 'path' missing")?;
				let path = path.into_string()?;
				let value = obj.remove("value").context("Key 'value' missing")?;
				Ok(InnerOp::Add {
					path,
					value,
				})
			}
			"remove" => {
				let path = obj.remove("path").context("Key 'path' missing")?;
				let path = path.into_string()?;
				Ok(InnerOp::Remove {
					path,
				})
			}
			"replace" => {
				let path = obj.remove("path").context("Key 'path' missing")?;
				let path = path.into_string()?;
				let value = obj.remove("value").context("Key 'value' missing")?;
				Ok(InnerOp::Replace {
					path,
					value,
				})
			}
			"change" => {
				let path = obj.remove("path").context("Key 'path' missing")?;
				let path = path.into_string()?;
				let value = obj.remove("value").context("Key 'value' missing")?;
				let value = value.into_string()?;
				Ok(InnerOp::Change {
					path,
					value,
				})
			}
			_ => Err(anyhow::anyhow!("Invalid operation '{op}'")),
		}
	}
}

/// A [JSON Patch] operation
///
/// From the official website:
///
/// > JSON Patch is a format for describing changes to a JSON document.
/// > It can be used to avoid sending a whole document when only a part has
/// > changed.
///
/// [JSON Patch]: https://jsonpatch.com/
#[derive(Debug)]
#[must_use]
pub struct PatchOp(pub(crate) Value);

impl PatchOp {
	/// Adds a value to an object or inserts it into an array.
	///
	/// In the case of an array, the value is inserted before the given index.
	/// The `-` character can be used instead of an index to insert at the end
	/// of an array.
	///
	/// # Examples
	///
	/// ```
	/// # use serde_json::json;
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::add("/biscuits/1", json!({ "name": "Ginger Nut" }))
	/// # ;
	/// ```
	pub fn add(path: impl Into<String>, value: impl SurrealValue) -> Self {
		Self(
			InnerOp::Add {
				path: path.into(),
				value: value.into_value(),
			}
			.into_value(),
		)
	}

	/// Removes a value from an object or array.
	///
	/// # Examples
	///
	/// ```
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::remove("/biscuits")
	/// # ;
	/// ```
	///
	/// Remove the first element of the array at `biscuits`
	/// (or just removes the “0” key if `biscuits` is an object)
	///
	/// ```
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::remove("/biscuits/0")
	/// # ;
	/// ```
	pub fn remove(path: impl Into<String>) -> Self {
		Self(
			InnerOp::Remove {
				path: path.into(),
			}
			.into_value(),
		)
	}

	/// Replaces a value.
	///
	/// Equivalent to a “remove” followed by an “add”.
	///
	/// # Examples
	///
	/// ```
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::replace("/biscuits/0/name", "Chocolate Digestive")
	/// # ;
	/// ```
	pub fn replace(path: impl Into<String>, value: impl SurrealValue) -> Self {
		Self(
			InnerOp::Replace {
				path: path.into(),
				value: value.into_value(),
			}
			.into_value(),
		)
	}

	/// Changes a value
	pub fn change(path: impl Into<String>, diff: String) -> Self {
		Self(
			InnerOp::Change {
				path: path.into(),
				value: diff,
			}
			.into_value(),
		)
	}
}

/// Multiple patch operations
#[derive(Debug, Default)]
#[must_use]
pub struct PatchOps(Vec<PatchOp>);

impl From<PatchOps> for PatchOp {
	fn from(ops: PatchOps) -> Self {
		let mut merged = PatchOp(Value::Array(Array::with_capacity(ops.0.len())));
		for PatchOp(result) in ops.0 {
			if let Value::Array(value) = &mut merged.0 {
				value.push(result);
			}
		}
		merged
	}
}

impl PatchOps {
	/// Prepare for multiple patch operations
	pub const fn new() -> Self {
		Self(Vec::new())
	}

	/// Adds a value to an object or inserts it into an array.
	///
	/// In the case of an array, the value is inserted before the given index.
	/// The `-` character can be used instead of an index to insert at the end
	/// of an array.
	///
	/// # Examples
	///
	/// ```
	/// # use serde_json::json;
	/// # use surrealdb::opt::PatchOps;
	/// PatchOps::new().add("/biscuits/1", json!({ "name": "Ginger Nut" }))
	/// # ;
	/// ```
	pub fn add<T>(mut self, path: &str, value: T) -> Self
	where
		T: SurrealValue,
	{
		self.0.push(PatchOp::add(path, value));
		self
	}

	/// Removes a value from an object or array.
	///
	/// # Examples
	///
	/// ```
	/// # use surrealdb::opt::PatchOps;
	/// PatchOps::new().remove("/biscuits")
	/// # ;
	/// ```
	///
	/// Remove the first element of the array at `biscuits`
	/// (or just removes the “0” key if `biscuits` is an object)
	///
	/// ```
	/// # use surrealdb::opt::PatchOps;
	/// PatchOps::new().remove("/biscuits/0")
	/// # ;
	/// ```
	pub fn remove(mut self, path: &str) -> Self {
		self.0.push(PatchOp::remove(path));
		self
	}

	/// Replaces a value.
	///
	/// Equivalent to a “remove” followed by an “add”.
	///
	/// # Examples
	///
	/// ```
	/// # use surrealdb::opt::PatchOps;
	/// PatchOps::new().replace("/biscuits/0/name", "Chocolate Digestive")
	/// # ;
	/// ```
	pub fn replace<T>(mut self, path: &str, value: T) -> Self
	where
		T: SurrealValue,
	{
		self.0.push(PatchOp::replace(path, value));
		self
	}

	/// Changes a value
	pub fn change(mut self, path: &str, diff: String) -> Self {
		self.0.push(PatchOp::change(path, diff));
		self
	}
}

/// Makes the client wait for a certain event or call to happen before
/// continuing
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum WaitFor {
	/// Waits for the connection to succeed
	Connection,
	/// Waits for the desired database to be selected
	Database,
}

/// Forwards a raw query without trying to parse for live select statements
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct Raw(pub(crate) Cow<'static, str>);

impl From<&'static str> for Raw {
	fn from(query: &'static str) -> Self {
		Self(Cow::Borrowed(query))
	}
}

impl From<String> for Raw {
	fn from(query: String) -> Self {
		Self(Cow::Owned(query))
	}
}
