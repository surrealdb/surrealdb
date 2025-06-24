use super::Content;
use crate::val::{self, Object, Value};
use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::{Data, Expected, Unexpected};
use std::collections::BTreeMap;

pub(super) fn to_value(content: Content) -> Result<Value> {
	match content {
		Content::Enum(v) => match v.name.as_ref() {
			val::number::TOKEN => val::Number::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			val::geometry::TOKEN => {
				val::Geometry::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			val::TOKEN => {
				Value::deserialize(Content::Enum(v).into_deserializer()).map_err(Into::into)
			}
			_ => match v.data {
				Data::Unit => Ok(v.variant.into_owned().into()),
				Data::NewType {
					value,
				} => (v.variant, value).try_into(),
				Data::Tuple {
					values,
				} => (v.variant, Content::Seq(values)).try_into(),
				Data::Struct {
					fields,
				} => {
					let mut map = BTreeMap::new();
					let value = fields.try_into()?;
					map.insert(v.variant.into_owned(), value);
					Ok(Value::Object(Object(map)))
				}
			},
		},
		content => Err(content.unexpected(Expected::Enum {
			name: None,
			typ: None,
		}))?,
	}
}
