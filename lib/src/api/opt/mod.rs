//! The different options and types for use in API functions

pub mod auth;
pub mod capabilities;

mod config;
mod endpoint;
mod export;
mod query;
mod resource;
mod tls;

use crate::sql::to_value;
use crate::sql::Thing;
use crate::sql::Value;
use dmp::Diff;
use serde::Serialize;

pub use config::*;
pub use endpoint::*;
pub use export::*;
pub use query::*;
pub use resource::*;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
pub use tls::*;

/// Record ID
pub type RecordId = Thing;

type UnitOp<'a> = InnerOp<'a, ()>;

#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum InnerOp<'a, T> {
	Add {
		path: &'a str,
		value: T,
	},
	Remove {
		path: &'a str,
	},
	Replace {
		path: &'a str,
		value: T,
	},
	Change {
		path: &'a str,
		value: String,
	},
}

/// A [JSON Patch] operation
///
/// From the official website:
///
/// > JSON Patch is a format for describing changes to a JSON document.
/// > It can be used to avoid sending a whole document when only a part has changed.
///
/// [JSON Patch]: https://jsonpatch.com/
#[derive(Debug)]
#[must_use]
pub struct PatchOp(pub(crate) Result<Value, crate::err::Error>);

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
	pub fn add<T>(path: &str, value: T) -> Self
	where
		T: Serialize,
	{
		Self(to_value(InnerOp::Add {
			path,
			value,
		}))
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
	pub fn remove(path: &str) -> Self {
		Self(to_value(UnitOp::Remove {
			path,
		}))
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
	pub fn replace<T>(path: &str, value: T) -> Self
	where
		T: Serialize,
	{
		Self(to_value(InnerOp::Replace {
			path,
			value,
		}))
	}

	/// Changes a value
	pub fn change(path: &str, diff: Diff) -> Self {
		Self(to_value(UnitOp::Change {
			path,
			value: diff.text,
		}))
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
