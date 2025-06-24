//! The different options and types for use in API functions

use serde::Serialize;
use surrealdb_core::expr::{Object, Value};
use std::{borrow::Cow, collections::BTreeMap};

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
#[cfg(any(feature = "native-tls", feature = "rustls"))]
pub use tls::*;

/// A patch operation.
#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
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
		value: Value,
	},
}

impl PatchOp {
	/// Adds a value to an object or inserts it into an array.
	///
	/// In the case of an array, the value is inserted before the given index.
	/// The `-` character can be used instead of an index to insert at the end of an array.
	///
	/// # Examples
	///
	/// ```
	/// # use serde_json::json;
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::add("/biscuits/1", json!({ "name": "Ginger Nut" }))
	/// # ;
	/// ```
	pub fn add(path: impl Into<String>, value: Value) -> Self {
		Self::Add {
			path: path.into(),
			value,
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
		Self::Remove {
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
	pub fn replace(path: impl Into<String>, value: Value) -> Self {
		Self::Replace {
			path: path.into(),
			value,
		}
	}

	/// Changes a value
	pub fn change(path: impl Into<String>, diff: Value) -> Self {
		Self::Change {
			path: path.into(),
			value: diff,
		}
	}
}

impl TryFrom<PatchOp> for Value {
	type Error = anyhow::Error;

	fn try_from(op: PatchOp) -> Result<Self, Self::Error> {
		// Convert the PatchOp into a Value
		let value = match op {
			PatchOp::Add { path, value } => {
				let mut map = BTreeMap::new();
				map.insert("op".to_string(), "add".into());
				map.insert("path".to_string(), path.into());
				map.insert("value".to_string(), value);
				Value::Object(Object::new(map))
			},
			PatchOp::Remove { path } => {
				let mut map = BTreeMap::new();
				map.insert("op".to_string(), "remove".into());
				map.insert("path".to_string(), path.into());
				Value::Object(Object::new(map))
			},
			PatchOp::Replace { path, value } => {
				let mut map = BTreeMap::new();
				map.insert("op".to_string(), "replace".into());
				map.insert("path".to_string(), path.into());
				map.insert("value".to_string(), value);
				Value::Object(Object::new(map))
			},
			PatchOp::Change { path, value } => {
				let mut map = BTreeMap::new();
				map.insert("op".to_string(), "change".into());
				map.insert("path".to_string(), path.into());
				map.insert("value".to_string(), value);
				Value::Object(Object::new(map))
			},
		};
		Ok(value)
	}
}

/// Multiple patch operations
#[derive(Debug, Default)]
#[must_use]
pub struct PatchOps(pub Vec<PatchOp>);

impl From<PatchOps> for PatchOp {
	fn from(ops: PatchOps) -> Self {
		todo!("STU: What is going on here?")
		// let mut merged = PatchOp(Ok(Content::Seq(Vec::with_capacity(ops.0.len()))));
		// for PatchOp(result) in ops.0 {
		// 	if let Ok(Content::Seq(value)) = &mut merged.0 {
		// 		match result {
		// 			Ok(op) => value.push(op),
		// 			Err(error) => {
		// 				merged.0 = Err(error);
		// 				// This operation produced an error, no need to continue
		// 				break;
		// 			}
		// 		}
		// 	}
		// }
		// merged
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
	/// The `-` character can be used instead of an index to insert at the end of an array.
	///
	/// # Examples
	///
	/// ```
	/// # use serde_json::json;
	/// # use surrealdb::opt::PatchOps;
	/// PatchOps::new().add("/biscuits/1", json!({ "name": "Ginger Nut" }))
	/// # ;
	/// ```
	pub fn add(mut self, path: &str, value: Value) -> Self
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
	pub fn replace<T>(mut self, path: &str, value: Value) -> Self
	{
		self.0.push(PatchOp::replace(path, value));
		self
	}

	/// Changes a value
	pub fn change(mut self, path: &str, diff: String) -> Self {
		self.0.push(PatchOp::change(path, diff.into()));
		self
	}

	pub fn push(&mut self, op: PatchOp) {
		self.0.push(op);
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn iter(&self) -> impl Iterator<Item = &PatchOp> {
		self.0.iter()
	}
	pub fn into_iter(self) -> impl Iterator<Item = PatchOp> {
		self.0.into_iter()
	}
}

/// Makes the client wait for a certain event or call to happen before continuing
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
