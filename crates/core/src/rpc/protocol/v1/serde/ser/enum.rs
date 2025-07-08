use super::Content;
use crate::expr;
use crate::rpc::protocol::v1::types::{V1Geometry, V1Number, V1Object, V1Value};
use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::Data;
use serde_content::Expected;
use serde_content::Unexpected;
use std::collections::BTreeMap;

pub(in crate::rpc) fn to_value(content: Content) -> Result<V1Value> {
	match content {
		Content::Enum(v) => match v.name.as_ref() {
			expr::number::TOKEN => V1Number::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::geometry::TOKEN => V1Geometry::deserialize(Content::Enum(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::value::TOKEN => {
				V1Value::deserialize(Content::Enum(v).into_deserializer()).map_err(Into::into)
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
					Ok(V1Value::Object(V1Object(map)))
				}
			},
		},
		content => Err(content.unexpected(Expected::Enum {
			name: None,
			typ: None,
		}))?,
	}
}
