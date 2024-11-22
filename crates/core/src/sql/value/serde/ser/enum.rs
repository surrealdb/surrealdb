use super::Content;
use crate::err::Error;
use crate::sql;
use crate::sql::Object;
use crate::sql::Value;
use serde::de::IntoDeserializer;
use serde::Deserialize;
use serde_content::Data;
use serde_content::Expected;
use serde_content::Unexpected;
use std::collections::BTreeMap;

pub(super) fn to_value(content: Content) -> Result<Value, Error> {
	match content {
		Content::Enum(v) => match v.name.as_ref() {
			sql::expression::TOKEN => {
				sql::Expression::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::subquery::TOKEN => {
				sql::Subquery::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::function::TOKEN => {
				sql::Function::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::constant::TOKEN => {
				sql::Constant::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::mock::TOKEN => sql::Mock::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::number::TOKEN => sql::Number::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			sql::geometry::TOKEN => {
				sql::Geometry::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			sql::value::TOKEN => sql::Value::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
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
