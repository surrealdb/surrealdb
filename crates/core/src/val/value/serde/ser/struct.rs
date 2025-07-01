use super::Content;
use crate::val::{self, Value};
use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::{Data, Expected, Unexpected};

pub(super) fn to_value(content: Content) -> Result<Value> {
	match content {
		Content::Struct(v) => match v.name.as_ref() {
			val::strand::TOKEN => val::Strand::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			/*
			expr::future::TOKEN => {
				expr::Future::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}*/
			val::range::TOKEN => val::Range::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::regex::TOKEN => val::Regex::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::table::TOKEN => expr::Table::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::thing::TOKEN => val::RecordId::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::object::TOKEN => val::Object::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::array::TOKEN => val::Array::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::uuid::TOKEN => val::Uuid::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::datetime::TOKEN => {
				val::Datetime::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			val::duration::TOKEN => {
				val::Duration::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			val::closure::TOKEN => {
				val::Closure::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			val::file::TOKEN => val::File::deserialize(Content::Struct(v).into_deserializer())
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
