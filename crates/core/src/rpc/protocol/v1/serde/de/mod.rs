use crate::rpc::protocol::v1::types::{
	V1Array, V1Bytes, V1Datetime, V1Duration, V1Object, V1Strand, V1Uuid, V1Value,
};
use crate::rpc::protocol::v1::types::{V1Geometry, V1Number};
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde_content::Deserializer;
use serde_content::Number;
use serde_content::Serializer;
use serde_content::Value as Content;
use std::borrow::Cow;

impl V1Value {
	/// Convert the value to a Serde Content representation which allows for using serde to translate
	/// the value into a different type.
	pub fn into_content(self) -> Result<Content<'static>> {
		let serializer = Serializer::new();
		match self {
			V1Value::None => Ok(Content::Option(None)),
			V1Value::Null => Ok(Content::Option(None)),
			V1Value::Bool(v) => Ok(Content::Bool(v)),
			V1Value::Number(v) => match v {
				V1Number::Int(v) => Ok(Content::Number(Number::I64(v))),
				V1Number::Float(v) => Ok(Content::Number(Number::F64(v))),
				V1Number::Decimal(v) => serializer.serialize(v).map_err(Into::into),
			},
			V1Value::Strand(V1Strand(v)) => Ok(Content::String(Cow::Owned(v))),
			V1Value::Duration(V1Duration(v)) => serializer.serialize(v).map_err(Into::into),
			V1Value::Datetime(V1Datetime(v)) => serializer.serialize(v).map_err(Into::into),
			V1Value::Uuid(V1Uuid(v)) => serializer.serialize(v).map_err(Into::into),
			V1Value::Array(V1Array(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for value in v {
					vec.push(value.into_content()?);
				}
				Ok(Content::Seq(vec))
			}
			V1Value::Object(V1Object(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for (key, value) in v {
					let key = Content::String(Cow::Owned(key));
					let value = value.into_content()?;
					vec.push((key, value));
				}
				Ok(Content::Map(vec))
			}
			V1Value::Geometry(v) => match v {
				V1Geometry::Point(v) => serializer.serialize(v).map_err(Into::into),
				V1Geometry::Line(v) => serializer.serialize(v).map_err(Into::into),
				V1Geometry::Polygon(v) => serializer.serialize(v).map_err(Into::into),
				V1Geometry::MultiPoint(v) => serializer.serialize(v).map_err(Into::into),
				V1Geometry::MultiLine(v) => serializer.serialize(v).map_err(Into::into),
				V1Geometry::MultiPolygon(v) => serializer.serialize(v).map_err(Into::into),
				V1Geometry::Collection(v) => serializer.serialize(v).map_err(Into::into),
			},
			V1Value::Bytes(V1Bytes(v)) => Ok(Content::Bytes(Cow::Owned(v))),
			V1Value::RecordId(v) => serializer.serialize(v).map_err(Into::into),
			V1Value::File(v) => serializer.serialize(v).map_err(Into::into),
			V1Value::Table(v) => serializer.serialize(v).map_err(Into::into),
			V1Value::Model(v) => serializer.serialize(v).map_err(Into::into),
			V1Value::Regex(v) => serializer.serialize(v).map_err(Into::into),
			V1Value::Range(v) => serializer.serialize(v).map_err(Into::into),
		}
	}
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub fn from_value<T>(value: V1Value) -> Result<T>
where
	T: DeserializeOwned,
{
	let content = value.into_content()?;
	let deserializer = Deserializer::new(content).coerce_numbers();
	T::deserialize(deserializer).map_err(Into::into)
}
