use std::collections::BTreeMap;

use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::{Data, Expected, Unexpected};

use super::{Content, object_from_content_struct, value_from_content};
use crate::core::val::{self, Object, Strand, Value};

pub(super) fn to_value(content: Content) -> Result<Value> {
	match content {
		Content::Enum(v) => match v.name.as_ref() {
			"$surrealdb::private::Number" => {
				val::Number::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Geometry" => {
				val::Geometry::deserialize(Content::Enum(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Value" => {
				Value::deserialize(Content::Enum(v).into_deserializer()).map_err(Into::into)
			}
			_ => match v.data {
				//TODO:  Null byte validity
				Data::Unit => Ok(unsafe { Strand::new_unchecked(v.variant.into_owned()) }.into()),
				Data::NewType {
					value,
				} => {
					let mut res = val::Object::new();
					res.insert(v.variant.into_owned(), value_from_content(value)?);
					Ok(res.into())
				}
				Data::Tuple {
					values,
				} => {
					let mut res = val::Object::new();
					res.insert(v.variant.into_owned(), value_from_content(Content::Seq(values))?);
					Ok(res.into())
				}
				Data::Struct {
					fields,
				} => {
					let mut map = BTreeMap::new();
					let value = object_from_content_struct(fields)?;
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

#[cfg(test)]
mod test {
	use geo::Point;
	use serde::Serialize;

	use crate::core::val;

	fn val_to_serde_name<T: Serialize>(t: T) -> String {
		let ser = t.serialize(serde_content::Serializer::new()).unwrap();
		match ser {
			serde_content::Value::Enum(x) => x.name.into_owned(),
			_ => panic!("type didn't serialize to a enum"),
		}
	}

	// These strings and the ones in the above implementation must be kept in sync.
	#[test]
	fn serde_enum_names_are_correct() {
		assert_eq!("$surrealdb::private::Number", val_to_serde_name(val::Number::Int(0)));
		assert_eq!(
			"$surrealdb::private::Geometry",
			val_to_serde_name(val::Geometry::Point(Point::new(0.0, 0.0)))
		);
		assert_eq!("$surrealdb::private::Value", val_to_serde_name(val::Value::None));
	}
}
