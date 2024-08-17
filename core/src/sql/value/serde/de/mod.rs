use crate::err::Error;
use crate::sql;
use crate::sql::Value;
use serde::de::DeserializeOwned;
use serde_content::Deserializer;
use serde_content::Number;
use serde_content::Serializer;
use serde_content::Value as Content;
use std::borrow::Cow;

impl Value {
	fn into_content(self) -> Result<Content<'static>, Error> {
		let serializer = Serializer::new();
		match self {
			Value::None => Ok(Content::Option(None)),
			Value::Null => Ok(Content::Option(None)),
			Value::Bool(v) => Ok(Content::Bool(v)),
			Value::Number(v) => match v {
				sql::Number::Int(v) => Ok(Content::Number(Number::I64(v))),
				sql::Number::Float(v) => Ok(Content::Number(Number::F64(v))),
				sql::Number::Decimal(v) => serializer.serialize(v).map_err(Into::into),
			},
			Value::Strand(sql::Strand(v)) => Ok(Content::String(Cow::Owned(v))),
			Value::Duration(sql::Duration(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Datetime(sql::Datetime(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Uuid(sql::Uuid(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Array(sql::Array(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for value in v {
					vec.push(value.into_content()?);
				}
				Ok(Content::Seq(vec))
			}
			Value::Object(sql::Object(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for (key, value) in v {
					let key = Content::String(Cow::Owned(key));
					let value = value.into_content()?;
					vec.push((key, value));
				}
				Ok(Content::Map(vec))
			}
			Value::Geometry(v) => match v {
				sql::Geometry::Point(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::Line(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::Polygon(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::MultiPoint(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::MultiLine(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::MultiPolygon(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::Collection(v) => serializer.serialize(v).map_err(Into::into),
			},
			Value::Bytes(sql::Bytes(v)) => Ok(Content::Bytes(Cow::Owned(v))),
			Value::Thing(v) => serializer.serialize(v).map_err(Into::into),
			Value::Param(sql::Param(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Idiom(sql::Idiom(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Table(sql::Table(v)) => serializer.serialize(v).map_err(Into::into),
			Value::Mock(v) => serializer.serialize(v).map_err(Into::into),
			Value::Regex(v) => serializer.serialize(v).map_err(Into::into),
			Value::Cast(v) => serializer.serialize(v).map_err(Into::into),
			Value::Block(v) => serializer.serialize(v).map_err(Into::into),
			Value::Range(v) => serializer.serialize(v).map_err(Into::into),
			Value::Edges(v) => serializer.serialize(v).map_err(Into::into),
			Value::Future(v) => serializer.serialize(v).map_err(Into::into),
			Value::Constant(v) => serializer.serialize(v).map_err(Into::into),
			Value::Function(v) => serializer.serialize(v).map_err(Into::into),
			Value::Subquery(v) => serializer.serialize(v).map_err(Into::into),
			Value::Expression(v) => serializer.serialize(v).map_err(Into::into),
			Value::Query(v) => serializer.serialize(v).map_err(Into::into),
			Value::Model(v) => serializer.serialize(v).map_err(Into::into),
			Value::Closure(v) => serializer.serialize(v).map_err(Into::into),
		}
	}
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub fn from_value<T>(value: Value) -> Result<T, Error>
where
	T: DeserializeOwned,
{
	let content = value.into_content()?;
	let deserializer = Deserializer::new(content).coerce_numbers();
	T::deserialize(deserializer).map_err(Into::into)
}
