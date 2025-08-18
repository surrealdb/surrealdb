use std::borrow::Cow;

use anyhow::Result;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_content::{Deserializer, Number, Serializer, Value as Content};

use crate::core::val;

mod ser;

fn into_content(this: val::Value) -> Result<Content<'static>> {
	let serializer = Serializer::new();
	match this {
		val::Value::None => Ok(Content::Option(None)),
		val::Value::Null => Ok(Content::Option(None)),
		val::Value::Bool(v) => Ok(Content::Bool(v)),
		val::Value::Number(v) => match v {
			val::Number::Int(v) => Ok(Content::Number(Number::I64(v))),
			val::Number::Float(v) => Ok(Content::Number(Number::F64(v))),
			val::Number::Decimal(v) => serializer.serialize(v).map_err(Into::into),
		},
		val::Value::Strand(v) => Ok(Content::String(Cow::Owned(v.into_string()))),
		val::Value::Duration(val::Duration(v)) => serializer.serialize(v).map_err(Into::into),
		val::Value::Datetime(val::Datetime(v)) => serializer.serialize(v).map_err(Into::into),
		val::Value::Uuid(val::Uuid(v)) => serializer.serialize(v).map_err(Into::into),
		val::Value::Array(val::Array(v)) => {
			let seq = v.into_iter().map(into_content).collect::<Result<Vec<Content<'static>>>>()?;
			Ok(Content::Seq(seq))
		}
		val::Value::Object(val::Object(v)) => {
			let mut vec = Vec::with_capacity(v.len());
			for (key, value) in v {
				let key = Content::String(Cow::Owned(key));
				let value = into_content(value)?;
				vec.push((key, value));
			}
			Ok(Content::Map(vec))
		}
		val::Value::Geometry(v) => match v {
			val::Geometry::Point(v) => serializer.serialize(v).map_err(Into::into),
			val::Geometry::Line(v) => serializer.serialize(v).map_err(Into::into),
			val::Geometry::Polygon(v) => serializer.serialize(v).map_err(Into::into),
			val::Geometry::MultiPoint(v) => serializer.serialize(v).map_err(Into::into),
			val::Geometry::MultiLine(v) => serializer.serialize(v).map_err(Into::into),
			val::Geometry::MultiPolygon(v) => serializer.serialize(v).map_err(Into::into),
			val::Geometry::Collection(v) => serializer.serialize(v).map_err(Into::into),
		},
		val::Value::Bytes(v) => Ok(Content::Bytes(Cow::Owned(v.into_inner()))),
		val::Value::Table(v) => serializer.serialize(v.into_string()).map_err(Into::into),
		val::Value::RecordId(v) => serializer.serialize(v).map_err(Into::into),
		val::Value::Range(v) => serializer.serialize(v).map_err(Into::into),
		val::Value::File(v) => serializer.serialize(v).map_err(Into::into),
		val::Value::Regex(v) => serializer.serialize(v).map_err(Into::into),
		val::Value::Closure(_) => Err(anyhow::anyhow!("Cannot serialize closure")),
	}
}

/// Converts a urrealdb_core::val::Value into a deserializable value
pub fn from_value<T: DeserializeOwned>(value: val::Value) -> Result<T> {
	let v = into_content(value)?;
	let deserializer = Deserializer::new(v).coerce_numbers();
	T::deserialize(deserializer).map_err(From::from)
}

/// Converts a serializable type into crate::core::val::Value
pub fn to_value<T: Serialize + 'static>(value: T) -> Result<val::Value> {
	ser::to_value(value)
}
