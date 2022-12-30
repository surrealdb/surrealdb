//! The different options and types for use in API functions

pub mod auth;

mod endpoint;
mod query;
mod resource;
mod strict;
mod tls;

use crate::api::err::Error;
use crate::api::Result;
use crate::sql;
use crate::sql::Thing;
use crate::sql::Value;
use dmp::Diff;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub use endpoint::*;
pub use query::*;
pub use resource::*;
pub use strict::*;
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
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::add("/biscuits/1", json!({ "name": "Ginger Nut" }))
	/// # ;
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
	/// # use surrealdb::opt::PatchOp;
	/// PatchOp::replace("/biscuits/0/name", "Chocolate Digestive")
	/// # ;
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

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub(crate) fn from_value<T>(value: sql::Value) -> Result<T>
where
	T: DeserializeOwned,
{
	let bytes = match msgpack::to_vec(&value) {
		Ok(bytes) => bytes,
		Err(error) => {
			return Err(Error::FromValue {
				value,
				error: error.to_string(),
			}
			.into());
		}
	};
	match msgpack::from_slice(&bytes) {
		Ok(response) => Ok(response),
		Err(error) => Err(Error::FromValue {
			value,
			error: error.to_string(),
		}
		.into()),
	}
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
		JsonValue::String(string) => match sql::thing(&string) {
			Ok(thing) => thing.into(),
			Err(_) => string.into(),
		},
		JsonValue::Array(array) => array.into_iter().map(from_json).collect::<Vec<_>>().into(),
		JsonValue::Object(object) => object
			.into_iter()
			.map(|(key, value)| (key, from_json(value)))
			.collect::<BTreeMap<_, _>>()
			.into(),
	}
}
