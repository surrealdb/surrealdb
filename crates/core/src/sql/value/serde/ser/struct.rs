use super::Content;
use crate::err::Error;
use crate::sql;
use crate::sql::Value;
use serde::de::IntoDeserializer;
use serde::Deserialize;
use serde_content::{Data, Expected, Unexpected};

pub(super) fn to_value(content: Content) -> Result<Value, Error> {
	match content {
		Content::Struct(v) => match v.name.as_ref() {
			sql::strand::TOKEN => sql::Strand::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::model::TOKEN => sql::Model::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::query::TOKEN => sql::Query::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::future::TOKEN => sql::Future::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::edges::TOKEN => sql::Edges::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::range::TOKEN => sql::Range::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::block::TOKEN => sql::Block::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::cast::TOKEN => sql::Cast::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::regex::TOKEN => sql::Regex::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::table::TOKEN => sql::Table::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::idiom::TOKEN => sql::Idiom::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::param::TOKEN => sql::Param::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::thing::TOKEN => sql::Thing::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::object::TOKEN => sql::Object::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::array::TOKEN => sql::Array::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::uuid::TOKEN => sql::Uuid::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::datetime::TOKEN => {
				sql::Datetime::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::duration::TOKEN => {
				sql::Duration::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::closure::TOKEN => {
				sql::Closure::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			_ => match v.data {
				Data::Unit => Ok(Value::None),
				Data::NewType {
					value,
				} => value.try_into(),
				Data::Tuple {
					values,
				} => values.try_into(),
				Data::Struct {
					fields,
				} => fields.try_into(),
			},
		},
		content => Err(content.unexpected(Expected::Struct {
			name: None,
			typ: None,
		}))?,
	}
}
