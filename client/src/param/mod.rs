//! Parameter types

mod credentials;
mod jwt;
mod query;
mod resource;
mod server_addrs;

use crate::Result;
use dmp::Diff;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
#[cfg(feature = "http")]
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use surrealdb::sql;
use surrealdb::sql::Value;

pub use credentials::*;
pub use jwt::*;
pub use query::*;
pub use resource::*;
pub use server_addrs::*;

/// Record ID
pub type RecordId = sql::Thing;

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
pub struct PatchOp(pub(crate) Value);

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
	/// PatchOp::add("/biscuits/1", json!({ "name": "Ginger Nut" }))
	/// ```
	#[must_use]
	pub fn add<T>(path: &str, value: T) -> Self
	where
		T: Serialize,
	{
		let value = from_json(json!(InnerOp::Add {
			path,
			value
		}));
		Self(value)
	}

	/// Removes a value from an object or array.
	///
	/// # Examples
	///
	/// ```
	/// PatchOp::remove("/biscuits")
	/// ```
	///
	/// Remove the first element of the array at `biscuits`
	/// (or just removes the “0” key if `biscuits` is an object)
	///
	/// ```
	/// PatchOp::remove("/biscuits/0")
	/// ```
	#[must_use]
	pub fn remove(path: &str) -> Self {
		let value = from_json(json!(UnitOp::Remove {
			path
		}));
		Self(value)
	}

	/// Replaces a value.
	///
	/// Equivalent to a “remove” followed by an “add”.
	///
	/// # Examples
	///
	/// ```
	/// PatchOp::replace("/biscuits/0/name", "Chocolate Digestive")
	/// ```
	#[must_use]
	pub fn replace<T>(path: &str, value: T) -> Self
	where
		T: Serialize,
	{
		let value = from_json(json!(InnerOp::Replace {
			path,
			value
		}));
		Self(value)
	}

	/// Changes a value
	#[must_use]
	pub fn change(path: &str, diff: Diff) -> Self {
		let value = from_json(json!(UnitOp::Change {
			path,
			value: diff.text,
		}));
		Self(value)
	}
}

/// Holds the parameters given to the caller
#[derive(Debug)]
pub struct Param {
	pub(crate) query: Vec<sql::Value>,
	#[cfg(feature = "http")]
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) file: Option<PathBuf>,
}

impl Param {
	pub(crate) fn new(query: Vec<sql::Value>) -> Self {
		Self {
			query,
			#[cfg(feature = "http")]
			#[cfg(not(target_arch = "wasm32"))]
			file: None,
		}
	}

	#[cfg(feature = "http")]
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) fn file(file: PathBuf) -> Self {
		Self {
			query: Vec::new(),
			file: Some(file),
		}
	}
}

/// The database response sent from the router to the caller
#[derive(Debug)]
pub enum DbResponse {
	/// The response sent for the `query` method
	Query(Vec<Result<Vec<sql::Value>>>),
	/// The response sent for any method except `query`
	Other(sql::Value),
}

/// Deserializes a value `T` from `SurrealDB` `Value`
pub fn from_value<T>(value: &sql::Value) -> Result<T>
where
	T: DeserializeOwned,
{
	let bytes = serde_pack::to_vec(&value)?;
	let response = serde_pack::from_slice(&bytes)?;
	Ok(response)
}

pub(crate) fn from_json(json: JsonValue) -> sql::Value {
	match json {
		JsonValue::Null => sql::Value::None,
		JsonValue::Bool(boolean) => boolean.into(),
		JsonValue::Number(number) => match (number.as_u64(), number.as_i64(), number.as_f64()) {
			(Some(number), _, _) => number.into(),
			(_, Some(number), _) => number.into(),
			(_, _, Some(number)) => number.into(),
			_ => unreachable!(),
		},
		JsonValue::String(string) => string.into(),
		JsonValue::Array(array) => array.into_iter().map(from_json).collect::<Vec<_>>().into(),
		JsonValue::Object(object) => object
			.into_iter()
			.map(|(key, value)| (key, from_json(value)))
			.collect::<BTreeMap<_, _>>()
			.into(),
	}
}
