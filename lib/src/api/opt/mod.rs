//! The different options and types for use in API functions

pub mod auth;

mod endpoint;
mod query;
mod resource;
mod strict;
mod tls;

use crate::api::err::Error;
use crate::sql::serde::serialize_internal;
use crate::sql::to_value;
use crate::sql::Thing;
use crate::sql::Value;
use dmp::Diff;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value as JsonValue;

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

fn into_json(value: Value) -> serde_json::Result<JsonValue> {
	use crate::sql;
	use crate::sql::Number;
	use serde_json::Error;

	#[derive(Serialize)]
	struct Array(Vec<JsonValue>);

	impl TryFrom<sql::Array> for Array {
		type Error = Error;

		fn try_from(arr: sql::Array) -> Result<Self, Self::Error> {
			let mut vec = Vec::with_capacity(arr.0.len());
			for value in arr.0 {
				vec.push(into_json(value)?);
			}
			Ok(Self(vec))
		}
	}

	#[derive(Serialize)]
	struct Object(Map<String, JsonValue>);

	impl TryFrom<sql::Object> for Object {
		type Error = Error;

		fn try_from(obj: sql::Object) -> Result<Self, Self::Error> {
			let mut map = Map::with_capacity(obj.0.len());
			for (key, value) in obj.0 {
				map.insert(key.to_owned(), into_json(value)?);
			}
			Ok(Self(map))
		}
	}

	#[derive(Serialize)]
	enum Id {
		Number(i64),
		String(String),
		Array(Array),
		Object(Object),
	}

	impl TryFrom<sql::Id> for Id {
		type Error = Error;

		fn try_from(id: sql::Id) -> Result<Self, Self::Error> {
			use sql::Id::*;
			Ok(match id {
				Number(n) => Id::Number(n),
				String(s) => Id::String(s),
				Array(arr) => Id::Array(arr.try_into()?),
				Object(obj) => Id::Object(obj.try_into()?),
			})
		}
	}

	#[derive(Serialize)]
	struct Thing {
		tb: String,
		id: Id,
	}

	impl TryFrom<sql::Thing> for Thing {
		type Error = Error;

		fn try_from(thing: sql::Thing) -> Result<Self, Self::Error> {
			Ok(Self {
				tb: thing.tb,
				id: thing.id.try_into()?,
			})
		}
	}

	match value {
		Value::None | Value::Null => Ok(JsonValue::Null),
		Value::False => Ok(false.into()),
		Value::True => Ok(true.into()),
		Value::Number(Number::Int(n)) => Ok(n.into()),
		Value::Number(Number::Float(n)) => Ok(n.into()),
		Value::Number(Number::Decimal(n)) => serde_json::to_value(n),
		Value::Strand(strand) => Ok(strand.0.into()),
		Value::Duration(d) => serde_json::to_value(d),
		Value::Datetime(d) => serde_json::to_value(d),
		Value::Uuid(uuid) => serde_json::to_value(uuid),
		Value::Array(arr) => Ok(JsonValue::Array(Array::try_from(arr)?.0)),
		Value::Object(obj) => Ok(JsonValue::Object(Object::try_from(obj)?.0)),
		Value::Geometry(geometry) => serde_json::to_value(geometry),
		Value::Bytes(bytes) => serde_json::to_value(bytes),
		Value::Param(param) => serde_json::to_value(param),
		Value::Idiom(idiom) => serde_json::to_value(idiom),
		Value::Table(table) => serde_json::to_value(table),
		Value::Thing(thing) => serde_json::to_value(thing),
		Value::Model(model) => serde_json::to_value(model),
		Value::Regex(regex) => serde_json::to_value(regex),
		Value::Block(block) => serde_json::to_value(block),
		Value::Range(range) => serde_json::to_value(range),
		Value::Edges(edges) => serde_json::to_value(edges),
		Value::Future(future) => serde_json::to_value(future),
		Value::Constant(constant) => serde_json::to_value(constant),
		Value::Function(function) => serde_json::to_value(function),
		Value::Subquery(subquery) => serde_json::to_value(subquery),
		Value::Expression(expression) => serde_json::to_value(expression),
	}
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub(crate) fn from_value<T>(value: Value) -> Result<T, Error>
where
	T: DeserializeOwned,
{
	let json = match serialize_internal(|| into_json(value.clone())) {
		Ok(json) => json,
		Err(error) => {
			return Err(Error::FromValue {
				value,
				error: error.to_string(),
			})
		}
	};
	serde_json::from_value(json).map_err(|error| Error::FromValue {
		value,
		error: error.to_string(),
	})
}
