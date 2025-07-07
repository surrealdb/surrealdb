use super::Content;
use crate::val::{self, Value, value::serde as ser};
use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::{Data, Expected, Unexpected};

pub(super) fn to_value(content: Content) -> Result<Value> {
	match content {
		Content::Struct(v) => match v.name.as_ref() {
			ser::STRAND_TOKEN => val::Strand::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			/*
			expr::future::TOKEN => {
				expr::Future::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}*/
			ser::RANGE_TOKEN => val::Range::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::REGEX_TOKEN => val::Regex::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::TABLE_TOKEN => val::Strand::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::THING_TOKEN => val::RecordId::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::OBJECT_TOKEN => val::Object::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::ARRAY_TOKEN => val::Array::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::UUID_TOKEN => val::Uuid::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::DATETIME_TOKEN => {
				val::Datetime::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			ser::DURATION_TOKEN => {
				val::Duration::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			ser::CLOSURE_TOKEN => val::Closure::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			ser::FILE_TOKEN => val::File::deserialize(Content::Struct(v).into_deserializer())
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
