//! The different options and types for use in API functions

pub mod auth;

mod endpoint;
mod query;
mod resource;
mod strict;
mod tls;

use crate::api::err::Error;
use crate::sql::to_value;
use crate::sql::Thing;
use crate::sql::Value;
use dmp::Diff;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
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

impl From<Value> for serde_json::Value {
	fn from(value: Value) -> Self {
		into_json(value, true)
	}
}

fn into_json(value: Value, simplify: bool) -> JsonValue {
	use crate::sql;
	use crate::sql::Number;

	#[derive(Serialize)]
	struct Array(Vec<JsonValue>);

	impl From<(sql::Array, bool)> for Array {
		fn from((arr, simplify): (sql::Array, bool)) -> Self {
			let mut vec = Vec::with_capacity(arr.0.len());
			for value in arr.0 {
				vec.push(into_json(value, simplify));
			}
			Self(vec)
		}
	}

	#[derive(Serialize)]
	struct Object(Map<String, JsonValue>);

	impl From<(sql::Object, bool)> for Object {
		fn from((obj, simplify): (sql::Object, bool)) -> Self {
			let mut map = Map::with_capacity(obj.0.len());
			for (key, value) in obj.0 {
				map.insert(key.to_owned(), into_json(value, simplify));
			}
			Self(map)
		}
	}

	#[derive(Serialize)]
	enum CoordinatesType {
		Point,
		LineString,
		Polygon,
		MultiPoint,
		MultiLineString,
		MultiPolygon,
	}

	#[derive(Serialize)]
	struct Coordinates {
		#[serde(rename = "type")]
		typ: CoordinatesType,
		coordinates: JsonValue,
	}

	#[derive(Serialize)]
	struct GeometryCollection;

	#[derive(Serialize)]
	struct Geometries {
		#[serde(rename = "type")]
		typ: GeometryCollection,
		geometries: Vec<JsonValue>,
	}

	#[derive(Serialize)]
	struct Geometry(JsonValue);

	impl From<sql::Geometry> for Geometry {
		fn from(geo: sql::Geometry) -> Self {
			Self(match geo {
				sql::Geometry::Point(v) => json!(Coordinates {
					typ: CoordinatesType::Point,
					coordinates: vec![json!(v.x()), json!(v.y())].into(),
				}),
				sql::Geometry::Line(v) => json!(Coordinates {
					typ: CoordinatesType::LineString,
					coordinates: v
						.points()
						.map(|p| vec![json!(p.x()), json!(p.y())].into())
						.collect::<Vec<JsonValue>>()
						.into(),
				}),
				sql::Geometry::Polygon(v) => json!(Coordinates {
					typ: CoordinatesType::Polygon,
					coordinates: vec![v
						.exterior()
						.points()
						.map(|p| vec![json!(p.x()), json!(p.y())].into())
						.collect::<Vec<JsonValue>>()]
					.into_iter()
					.chain(
						v.interiors()
							.iter()
							.map(|i| {
								i.points()
									.map(|p| vec![json!(p.x()), json!(p.y())].into())
									.collect::<Vec<JsonValue>>()
							})
							.collect::<Vec<Vec<JsonValue>>>(),
					)
					.collect::<Vec<Vec<JsonValue>>>()
					.into(),
				}),
				sql::Geometry::MultiPoint(v) => json!(Coordinates {
					typ: CoordinatesType::MultiPoint,
					coordinates: v
						.0
						.iter()
						.map(|v| vec![json!(v.x()), json!(v.y())].into())
						.collect::<Vec<JsonValue>>()
						.into()
				}),
				sql::Geometry::MultiLine(v) => json!(Coordinates {
					typ: CoordinatesType::MultiLineString,
					coordinates: v
						.0
						.iter()
						.map(|v| {
							v.points()
								.map(|v| vec![json!(v.x()), json!(v.y())].into())
								.collect::<Vec<JsonValue>>()
						})
						.collect::<Vec<Vec<JsonValue>>>()
						.into()
				}),
				sql::Geometry::MultiPolygon(v) => json!(Coordinates {
					typ: CoordinatesType::MultiPolygon,
					coordinates: v
						.0
						.iter()
						.map(|v| {
							vec![v
								.exterior()
								.points()
								.map(|p| vec![json!(p.x()), json!(p.y())].into())
								.collect::<Vec<JsonValue>>()]
							.into_iter()
							.chain(
								v.interiors()
									.iter()
									.map(|i| {
										i.points()
											.map(|p| vec![json!(p.x()), json!(p.y())].into())
											.collect::<Vec<JsonValue>>()
									})
									.collect::<Vec<Vec<JsonValue>>>(),
							)
							.collect::<Vec<Vec<JsonValue>>>()
						})
						.collect::<Vec<Vec<Vec<JsonValue>>>>()
						.into(),
				}),
				sql::Geometry::Collection(v) => json!(Geometries {
					typ: GeometryCollection,
					geometries: v.into_iter().map(Geometry::from).map(|x| x.0).collect(),
				}),
			})
		}
	}

	#[derive(Serialize)]
	enum Id {
		Number(i64),
		String(String),
		Array(Array),
		Object(Object),
	}

	impl From<(sql::Id, bool)> for Id {
		fn from((id, simplify): (sql::Id, bool)) -> Self {
			match id {
				sql::Id::Number(n) => Id::Number(n),
				sql::Id::String(s) => Id::String(s),
				sql::Id::Array(arr) => Id::Array((arr, simplify).into()),
				sql::Id::Object(obj) => Id::Object((obj, simplify).into()),
			}
		}
	}

	#[derive(Serialize)]
	struct Thing {
		tb: String,
		id: Id,
	}

	impl From<(sql::Thing, bool)> for Thing {
		fn from((thing, simplify): (sql::Thing, bool)) -> Self {
			Self {
				tb: thing.tb,
				id: (thing.id, simplify).into(),
			}
		}
	}

	match value {
		Value::None | Value::Null => JsonValue::Null,
		Value::Bool(boolean) => boolean.into(),
		Value::Number(Number::Int(n)) => n.into(),
		Value::Number(Number::Float(n)) => n.into(),
		Value::Number(Number::Decimal(n)) => json!(n),
		Value::Strand(strand) => match simplify {
			true => strand.0.into(),
			false => json!(strand),
		},
		Value::Duration(d) => match simplify {
			true => d.to_string().into(),
			false => json!(d),
		},
		Value::Datetime(d) => json!(d),
		Value::Uuid(uuid) => json!(uuid),
		Value::Array(arr) => JsonValue::Array(Array::from((arr, simplify)).0),
		Value::Object(obj) => JsonValue::Object(Object::from((obj, simplify)).0),
		Value::Geometry(geo) => match simplify {
			true => Geometry::from(geo).0,
			false => json!(geo),
		},
		Value::Bytes(bytes) => json!(bytes),
		Value::Param(param) => json!(param),
		Value::Idiom(idiom) => json!(idiom),
		Value::Table(table) => json!(table),
		Value::Thing(thing) => match simplify {
			true => thing.to_string().into(),
			false => json!(thing),
		},
		Value::Model(model) => json!(model),
		Value::Regex(regex) => json!(regex),
		Value::Block(block) => json!(block),
		Value::Range(range) => json!(range),
		Value::Edges(edges) => json!(edges),
		Value::Future(future) => json!(future),
		Value::Constant(constant) => match simplify {
			true => constant.as_f64().into(),
			false => json!(constant),
		},
		Value::Function(function) => json!(function),
		Value::Subquery(subquery) => json!(subquery),
		Value::Expression(expression) => json!(expression),
	}
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub(crate) fn from_value<T>(value: Value) -> Result<T, Error>
where
	T: DeserializeOwned,
{
	let json = into_json(value.clone(), false);
	serde_json::from_value(json).map_err(|error| Error::FromValue {
		value,
		error: error.to_string(),
	})
}
