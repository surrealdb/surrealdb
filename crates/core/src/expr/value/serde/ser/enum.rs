use super::Content;
use crate::expr;
use crate::expr::Object;
use crate::expr::Value;
use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::Data;
use serde_content::Expected;
use serde_content::Unexpected;
use std::collections::BTreeMap;

pub(super) fn to_value(content: Content) -> Result<Value> {
	match content {
		Content::Enum(v) => match v.name.as_ref() {
			expr::expression::TOKEN => {
				expr::Expression::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::subquery::TOKEN => {
				expr::Subquery::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::function::TOKEN => {
				expr::Function::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::constant::TOKEN => {
				expr::Constant::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::mock::TOKEN => expr::Mock::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::number::TOKEN => expr::Number::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::geometry::TOKEN => {
				expr::Geometry::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::value::TOKEN => {
				expr::Value::deserialize(Content::Enum(v).into_deserializer()).map_err(Into::into)
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
