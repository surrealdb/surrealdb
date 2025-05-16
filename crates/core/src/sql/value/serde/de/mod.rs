use crate::err::Error;
use crate::sql;
use crate::sql::SqlValue;
use serde::de::DeserializeOwned;
use serde_content::Deserializer;
use serde_content::Number;
use serde_content::Serializer;
use serde_content::Value as Content;
use std::borrow::Cow;

impl SqlValue {
	fn into_content(self) -> Result<Content<'static>, Error> {
		let serializer = Serializer::new();
		match self {
			SqlValue::None => Ok(Content::Option(None)),
			SqlValue::Null => Ok(Content::Option(None)),
			SqlValue::Bool(v) => Ok(Content::Bool(v)),
			SqlValue::Number(v) => match v {
				sql::Number::Int(v) => Ok(Content::Number(Number::I64(v))),
				sql::Number::Float(v) => Ok(Content::Number(Number::F64(v))),
				sql::Number::Decimal(v) => serializer.serialize(v).map_err(Into::into),
			},
			SqlValue::Strand(sql::Strand(v)) => Ok(Content::String(Cow::Owned(v))),
			SqlValue::Duration(sql::Duration(v)) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Datetime(sql::Datetime(v)) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Uuid(sql::Uuid(v)) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Array(sql::Array(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for value in v {
					vec.push(value.into_content()?);
				}
				Ok(Content::Seq(vec))
			}
			SqlValue::Object(sql::Object(v)) => {
				let mut vec = Vec::with_capacity(v.len());
				for (key, value) in v {
					let key = Content::String(Cow::Owned(key));
					let value = value.into_content()?;
					vec.push((key, value));
				}
				Ok(Content::Map(vec))
			}
			SqlValue::Geometry(v) => match v {
				sql::Geometry::Point(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::Line(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::Polygon(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::MultiPoint(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::MultiLine(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::MultiPolygon(v) => serializer.serialize(v).map_err(Into::into),
				sql::Geometry::Collection(v) => serializer.serialize(v).map_err(Into::into),
			},
			SqlValue::Bytes(sql::Bytes(v)) => Ok(Content::Bytes(Cow::Owned(v))),
			SqlValue::Thing(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Param(sql::Param(v)) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Idiom(sql::Idiom(v)) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Table(sql::Table(v)) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Mock(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Regex(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Cast(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Block(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Range(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Edges(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Future(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Constant(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Function(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Subquery(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Expression(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Query(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Model(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Closure(v) => serializer.serialize(v).map_err(Into::into),
			SqlValue::Refs(_) => Ok(Content::Seq(vec![])),
			SqlValue::File(v) => serializer.serialize(v).map_err(Into::into),
		}
	}
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
pub fn from_value<T>(value: SqlValue) -> Result<T, Error>
where
	T: DeserializeOwned,
{
	let content = value.into_content()?;
	let deserializer = Deserializer::new(content).coerce_numbers();
	T::deserialize(deserializer).map_err(Into::into)
}
