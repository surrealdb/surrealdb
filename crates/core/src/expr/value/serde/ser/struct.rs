use super::Content;
use crate::err::Error;
use crate::expr;
use crate::expr::Value;
use serde::de::IntoDeserializer;
use serde::Deserialize;
use serde_content::{Data, Expected, Unexpected};

pub(super) fn to_value(content: Content) -> Result<Value, Error> {
	match content {
		Content::Struct(v) => match v.name.as_ref() {
			expr::strand::TOKEN => expr::Strand::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::model::TOKEN => expr::Model::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::query::TOKEN => expr::Query::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::future::TOKEN => expr::Future::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::edges::TOKEN => expr::Edges::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::range::TOKEN => expr::Range::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::block::TOKEN => expr::Block::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::cast::TOKEN => expr::Cast::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::regex::TOKEN => expr::Regex::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::table::TOKEN => expr::Table::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::idiom::TOKEN => expr::Idiom::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::param::TOKEN => expr::Param::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::thing::TOKEN => expr::Thing::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::object::TOKEN => expr::Object::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::array::TOKEN => expr::Array::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::uuid::TOKEN => expr::Uuid::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::datetime::TOKEN => {
				expr::Datetime::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::duration::TOKEN => {
				expr::Duration::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::closure::TOKEN => {
				expr::Closure::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::file::TOKEN => expr::File::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
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
