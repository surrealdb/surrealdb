use crate::expr;
use crate::val::{
	Array, Bytes, Datetime, Duration, Geometry, Number as ValNumber, Object, Strand, Uuid, Value,
};
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde_content::{Deserializer, Number, Serializer, Value as Content};
use std::borrow::Cow;

impl Value {
	fn into_content(self) -> Result<Content<'static>> {
		let serializer = Serializer::new();
		match self {
			Value::None => Ok(Content::Option(None)),
			Value::Null => Ok(Content::Option(None)),
			Value::Bool(v) => Ok(Content::Bool(v)),
			Value::Number(v) => match v {
				ValNumber::Int(v) => Ok(Content::Number(Number::I64(v))),
				ValNumber::Float(v) => Ok(Content::Number(Number::F64(v))),
				ValNumber::Decimal(v) => serializer.serialize(v).map_err(Into::into),
			},
			Value::Strand(Strand(v)) => Ok(Content::String(Cow::Owned(v))),
			Value::Duration(Duration(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Datetime(Datetime(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Uuid(Uuid(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Array(Array(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for value in v {
					vec.push(value.into_content()?);
				}
				Ok(Content::Seq(vec))
			}
			Value::Object(Object(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for (key, value) in v {
					let key = Content::String(Cow::Owned(key));
					let value = value.into_content()?;
					vec.push((key, value));
				}
				Ok(Content::Map(vec))
			}
			Value::Geometry(v) => match v {
				Geometry::Point(v) => serializer.serialize(v).map_err(Into::into),
				Geometry::Line(v) => serializer.serialize(v).map_err(Into::into),
				Geometry::Polygon(v) => serializer.serialize(v).map_err(Into::into),
				Geometry::MultiPoint(v) => serializer.serialize(v).map_err(Into::into),
				Geometry::MultiLine(v) => serializer.serialize(v).map_err(Into::into),
				Geometry::MultiPolygon(v) => serializer.serialize(v).map_err(Into::into),
				Geometry::Collection(v) => serializer.serialize(v).map_err(Into::into),
			},
			Value::Bytes(Bytes(v)) => Ok(Content::Bytes(Cow::Owned(v))),
			Value::Thing(v) => serializer.serialize(v).map_err(Into::into),
			Value::Regex(v) => serializer.serialize(v).map_err(Into::into),
			Value::Range(v) => serializer.serialize(v).map_err(Into::into),
			Value::Closure(v) => serializer.serialize(v).map_err(Into::into),
			Value::File(v) => serializer.serialize(v).map_err(Into::into),
		}
	}
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub fn from_value<T>(value: Value) -> Result<T>
where
	T: DeserializeOwned,
{
	let content = value.into_content()?;
	let deserializer = Deserializer::new(content).coerce_numbers();
	T::deserialize(deserializer).map_err(Into::into)
}
