use super::Content;
use crate::expr::Value;
use crate::{
	expr,
	rpc::protocol::v1::types::{
		V1Array, V1Datetime, V1Duration, V1File, V1Model, V1Object, V1RecordId, V1Strand, V1Uuid,
		V1Value,
	},
};
use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::{Data, Expected, Unexpected};

pub(super) fn to_value(content: Content) -> Result<V1Value> {
	match content {
		Content::Struct(v) => match v.name.as_ref() {
			expr::strand::TOKEN => V1Strand::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::model::TOKEN => V1Model::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::thing::TOKEN => V1RecordId::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::object::TOKEN => V1Object::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::array::TOKEN => V1Array::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::uuid::TOKEN => V1Uuid::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			expr::datetime::TOKEN => {
				V1Datetime::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::duration::TOKEN => {
				V1Duration::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			expr::file::TOKEN => V1File::deserialize(Content::Struct(v).into_deserializer())
				.map(Into::into)
				.map_err(Into::into),
			_ => match v.data {
				Data::Unit => Ok(V1Value::None),
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
