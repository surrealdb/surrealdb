//! The different options and types for use in API functions

pub mod auth;
pub mod capabilities;

mod config;
pub(crate) mod endpoint;
mod export;
pub(crate) mod query;
mod resource;
mod tls;
mod websocket;

pub use config::*;
pub use endpoint::*;
pub use export::*;
pub use query::*;
pub use resource::*;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
pub use tls::*;
pub use websocket::*;

use crate::types::{SurrealValue, Value};

#[derive(Debug, SurrealValue)]
#[surreal(untagged, lowercase)]
pub enum PatchOp {
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

impl From<PatchOp> for Value {
	fn from(op: PatchOp) -> Value {
		let mut obj = crate::types::Object::new();
		match op {
			PatchOp::Add {
				path,
				value,
			} => {
				obj.insert("op".to_string(), Value::String("add".to_string()));
				obj.insert("path".to_string(), Value::String(path));
				obj.insert("value".to_string(), value);
			}
			PatchOp::Remove {
				path,
			} => {
				obj.insert("op".to_string(), Value::String("remove".to_string()));
				obj.insert("path".to_string(), Value::String(path));
			}
			PatchOp::Replace {
				path,
				value,
			} => {
				obj.insert("op".to_string(), Value::String("replace".to_string()));
				obj.insert("path".to_string(), Value::String(path));
				obj.insert("value".to_string(), value);
			}
			PatchOp::Change {
				path,
				value,
			} => {
				obj.insert("op".to_string(), Value::String("change".to_string()));
				obj.insert("path".to_string(), Value::String(path));
				obj.insert("value".to_string(), Value::String(value));
			}
		}
		Value::Object(obj)
	}
}

// /// A [JSON Patch] operation
// ///
// /// From the official website:
// ///
// /// > JSON Patch is a format for describing changes to a JSON document.
// /// > It can be used to avoid sending a whole document when only a part has
// /// > changed.
// ///
// /// [JSON Patch]: https://jsonpatch.com/
// #[derive(Debug)]
// #[must_use]
// pub struct PatchOp(pub(crate) Value);

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
		PatchOp::Add {
			path: path.into(),
			value: value.into_value(),
		}
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
		PatchOp::Remove {
			path: path.into(),
		}
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
		PatchOp::Replace {
			path: path.into(),
			value: value.into_value(),
		}
	}

	/// Changes a value
	pub fn change(path: impl Into<String>, diff: String) -> Self {
		PatchOp::Change {
			path: path.into(),
			value: diff,
		}
	}
}

/// Multiple patch operations
#[derive(Debug, Default)]
#[must_use]
pub struct PatchOps(pub(crate) Vec<PatchOp>);

// impl From<PatchOps> for PatchOp {
// 	fn from(ops: PatchOps) -> Self {
// 		let mut merged = PatchOp(Value::Array(Array::with_capacity(ops.0.len())));
// 		for PatchOp(result) in ops.0 {
// 			if let Value::Array(value) = &mut merged.0 {
// 				value.push(result);
// 			}
// 		}
// 		merged
// 	}
// }

impl From<PatchOp> for PatchOps {
	fn from(op: PatchOp) -> Self {
		Self(vec![op])
	}
}

impl From<Vec<PatchOp>> for PatchOps {
	fn from(ops: Vec<PatchOp>) -> Self {
		Self(ops)
	}
}

impl PatchOps {
	/// Prepare for multiple patch operations
	pub const fn new() -> Self {
		Self(Vec::new())
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn push(mut self, patch: PatchOp) -> Self {
		self.0.push(patch);
		self
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

impl IntoIterator for PatchOps {
	type Item = PatchOp;
	type IntoIter = std::vec::IntoIter<PatchOp>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

// impl IntoIterator for PatchOps {
// 	type Item = Value;
// 	type IntoIter = std::vec::IntoIter<Value>;
// 	fn into_iter(self) -> Self::IntoIter {
// 		self.0.into_iter()
// 	}
// }

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
