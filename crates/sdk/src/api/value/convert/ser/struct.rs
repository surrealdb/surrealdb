use anyhow::Result;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_content::{Data, Expected, Unexpected};

use super::{Content, array_from_content, object_from_content_struct, value_from_content};
use crate::core::val::{self, Value};

pub(super) fn to_value(content: Content) -> Result<Value> {
	match content {
		Content::Struct(v) => match v.name.as_ref() {
			"$surrealdb::private::Strand" => {
				val::Strand::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Range" => {
				val::Range::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Regex" => {
				val::Regex::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::RecordId" => {
				val::RecordId::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Object" => {
				val::Object::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Array" => {
				val::Array::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Uuid" => {
				val::Uuid::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Datetime" => {
				val::Datetime::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			"$surrealdb::private::Duration" => {
				val::Duration::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			// "$surrealdb::private::Closure" => {
			// 	val::Closure::deserialize(Content::Struct(v).into_deserializer())
			// 		.map(Into::into)
			// 		.map_err(Into::into)
			// }
			"$surrealdb::private::File" => {
				val::File::deserialize(Content::Struct(v).into_deserializer())
					.map(Into::into)
					.map_err(Into::into)
			}
			_ => match v.data {
				Data::Unit => Ok(Value::None),
				Data::NewType {
					value,
				} => value_from_content(value),
				Data::Tuple {
					values,
				} => array_from_content(values),
				Data::Struct {
					fields,
				} => object_from_content_struct(fields),
			},
		},
		content => Err(content.unexpected(Expected::Struct {
			name: None,
			typ: None,
		}))?,
	}
}

#[cfg(test)]
mod test {
	use serde::Serialize;

	use crate::core::val;

	fn val_to_serde_name<T: Serialize>(t: T) -> String {
		let ser = t.serialize(serde_content::Serializer::new()).unwrap();
		match ser {
			serde_content::Value::Struct(x) => x.name.into_owned(),
			_ => panic!("type didn't serialize to a struct"),
		}
	}

	// These strings and the ones in the above implementation must be kept in sync.
	#[test]
	fn serde_struct_names_are_correct() {
		assert_eq!("$surrealdb::private::Strand", val_to_serde_name(val::Strand::default()));
		assert_eq!("$surrealdb::private::Range", val_to_serde_name(val::Range::unbounded()));
		assert_eq!(
			"$surrealdb::private::Regex",
			val_to_serde_name("a".parse::<val::Regex>().unwrap())
		);
		assert_eq!(
			"$surrealdb::private::RecordId",
			val_to_serde_name(val::RecordId::new("a".to_string(), 1i64))
		);
		assert_eq!("$surrealdb::private::Object", val_to_serde_name(val::Object::default()));
		assert_eq!("$surrealdb::private::Array", val_to_serde_name(val::Array::default()));
		assert_eq!("$surrealdb::private::Uuid", val_to_serde_name(val::Uuid::default()));
		assert_eq!("$surrealdb::private::Datetime", val_to_serde_name(val::Datetime::now()));
		assert_eq!("$surrealdb::private::Duration", val_to_serde_name(val::Duration::default()));
		// assert_eq!(
		// 	"$surrealdb::private::Closure",
		// 	val_to_serde_name(val::Closure {
		// 		args: Vec::new(),
		// 		returns: None,
		// 		body: crate::core::expr::Expr::Break,
		// 	})
		// );
		assert_eq!(
			"$surrealdb::private::File",
			val_to_serde_name(val::File {
				bucket: "a".to_string(),
				key: "b".to_string()
			})
		);
	}
}
