mod r#enum;
mod r#struct;

use crate::err::Error;
use crate::rpc::protocol::v1::types::{
	V1Array, V1Bytes, V1Datetime, V1Duration, V1File, V1Geometry, V1Number, V1Object, V1RecordId,
	V1Regex, V1Strand, V1Table, V1Uuid, V1Value,
};
use anyhow::Result;
use castaway::match_type;
use serde::ser::Serialize;
use serde_content::Number;
use serde_content::Serializer;
use serde_content::Unexpected;
use std::borrow::Cow;
use std::collections::BTreeMap;

type Content = serde_content::Value<'static>;

/// Convert a `T` into `surrealdb::expr::V1Value` which is an enum that can represent any valid SQL data.
pub fn to_value<T>(value: T) -> Result<V1Value>
where
	T: Serialize + 'static,
{
	match_type!(value, {
		V1Value as v => Ok(v),
		V1Number as v => Ok(v.into()),
		rust_decimal::Decimal as v => Ok(v.into()),
		V1Strand as v => Ok(v.into()),
		V1Duration as v => Ok(v.into()),
		core::time::Duration as v => Ok(v.into()),
		V1Datetime as v => Ok(v.into()),
		chrono::DateTime<chrono::Utc> as v => Ok(v.into()),
		V1Uuid as v => Ok(v.into()),
		uuid::Uuid as v => Ok(v.into()),
		V1Array as v => Ok(v.into()),
		V1Object as v => Ok(v.into()),
		V1Geometry as v => Ok(v.into()),
		geo_types::Point as v => Ok(v.into()),
		geo_types::LineString as v => Ok(V1Value::Geometry(v.into())),
		geo_types::Polygon as v => Ok(V1Value::Geometry(v.into())),
		geo_types::MultiPoint as v => Ok(V1Value::Geometry(v.into())),
		geo_types::MultiLineString as v => Ok(V1Value::Geometry(v.into())),
		geo_types::MultiPolygon as v => Ok(V1Value::Geometry(v.into())),
		geo_types::Point as v => Ok(V1Value::Geometry(v.into())),
		V1Bytes as v => Ok(v.into()),
		V1Table as v => Ok(v.into()),
		V1RecordId as v => Ok(v.into()),
		V1File as v => Ok(v.into()),
		V1Regex as v => Ok(v.into()),
		value => Serializer::new().serialize(value)?.try_into(),
	})
}

impl TryFrom<Content> for V1Value {
	type Error = anyhow::Error;

	fn try_from(content: Content) -> Result<Self, Self::Error> {
		match content {
			Content::Unit => Ok(V1Value::None),
			Content::Bool(v) => Ok(v.into()),
			Content::Number(v) => match v {
				Number::I8(v) => Ok(v.into()),
				Number::U8(v) => Ok(v.into()),
				Number::I16(v) => Ok(v.into()),
				Number::U16(v) => Ok(v.into()),
				Number::I32(v) => Ok(v.into()),
				Number::U32(v) => Ok(v.into()),
				Number::F32(v) => Ok(v.into()),
				Number::I64(v) => Ok(v.into()),
				Number::U64(v) => Ok(v.into()),
				Number::F64(v) => Ok(v.into()),
				Number::I128(v) => Ok(v.into()),
				Number::U128(v) => Ok(v.into()),
				_ => Err(anyhow::Error::new(Error::Serialization("unsupported number".to_owned()))),
			},
			Content::Char(v) => Ok(v.to_string().into()),
			Content::String(v) => match v {
				Cow::Borrowed(v) => Ok(v.into()),
				Cow::Owned(v) => Ok(v.into()),
			},
			Content::Bytes(v) => match v {
				Cow::Borrowed(v) => Ok(V1Value::Bytes(V1Bytes(v.to_vec()))),
				Cow::Owned(v) => Ok(V1Value::Bytes(V1Bytes(v))),
			},
			Content::Seq(v) => v.try_into(),
			Content::Map(v) => v.try_into(),
			Content::Option(v) => match v {
				Some(v) => (*v).try_into(),
				None => Ok(V1Value::None),
			},
			Content::Struct(_) => r#struct::to_value(content),
			Content::Enum(_) => r#enum::to_value(content),
			Content::Tuple(v) => v.try_into(),
		}
	}
}

impl TryFrom<Vec<Content>> for V1Value {
	type Error = anyhow::Error;

	fn try_from(v: Vec<Content>) -> Result<Self, Self::Error> {
		let mut vec = Vec::with_capacity(v.len());
		for content in v {
			vec.push(content.try_into()?);
		}
		Ok(Self::Array(V1Array(vec)))
	}
}

impl TryFrom<Vec<(Content, Content)>> for V1Value {
	type Error = anyhow::Error;

	fn try_from(v: Vec<(Content, Content)>) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		for (key, value) in v {
			let key = match key {
				Content::String(v) => match v {
					Cow::Borrowed(v) => v.to_owned(),
					Cow::Owned(v) => v,
				},
				content => {
					return Err(content.unexpected(serde_content::Expected::String))?;
				}
			};
			let value = value.try_into()?;
			map.insert(key, value);
		}
		Ok(Self::Object(V1Object(map)))
	}
}

impl TryFrom<Vec<(Cow<'static, str>, Content)>> for V1Value {
	type Error = anyhow::Error;

	fn try_from(v: Vec<(Cow<'static, str>, Content)>) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		for (key, value) in v {
			map.insert(key.into_owned(), value.try_into()?);
		}
		Ok(Self::Object(V1Object(map)))
	}
}

impl TryFrom<(Cow<'static, str>, Content)> for V1Value {
	type Error = anyhow::Error;

	fn try_from((key, value): (Cow<'static, str>, Content)) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		map.insert(key.into_owned(), value.try_into()?);
		Ok(Self::Object(V1Object(map)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::rpc::V1Regex;
	use crate::rpc::V1Table;
	use crate::sql;

	#[test]
	fn value_none() {
		let expected = V1Value::None;
		assert_eq!(expected, to_value(None::<u32>).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn null() {
		let expected = V1Value::Null;
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#false() {
		let expected = V1Value::Bool(false);
		assert_eq!(expected, to_value(false).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#true() {
		let expected = V1Value::Bool(true);
		assert_eq!(expected, to_value(true).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn number() {
		let number = V1Number::Int(Default::default());
		let value = to_value(number).unwrap();
		let expected = V1Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = V1Number::Float(Default::default());
		let value = to_value(number).unwrap();
		let expected = V1Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = V1Number::Decimal(Default::default());
		let value = to_value(number).unwrap();
		let expected = V1Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn strand() {
		let strand = V1Strand("foobar".to_owned());
		let value = to_value(strand.clone()).unwrap();
		let expected = V1Value::Strand(strand);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar".to_owned();
		let value = to_value(strand.clone()).unwrap();
		let expected = V1Value::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar";
		let value = to_value(strand).unwrap();
		let expected = V1Value::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn duration() {
		let duration = V1Duration::default();
		let value = to_value(duration).unwrap();
		let expected = V1Value::Duration(duration);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn datetime() {
		let datetime = V1Datetime::default();
		let value = to_value(datetime.clone()).unwrap();
		let expected = V1Value::Datetime(datetime);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn uuid() {
		let uuid = V1Uuid::default();
		let value = to_value(uuid).unwrap();
		let expected = V1Value::Uuid(uuid);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn array() {
		let array = V1Array::default();
		let value = to_value(array.clone()).unwrap();
		let expected = V1Value::Array(array);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn object() {
		let object = V1Object::default();
		let value = to_value(object.clone()).unwrap();
		let expected = V1Value::Object(object);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn geometry() {
		let geometry = V1Geometry::Collection(Vec::new());
		let value = to_value(geometry.clone()).unwrap();
		let expected = V1Value::Geometry(geometry);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn bytes() {
		let bytes = V1Bytes("foobar".as_bytes().to_owned());
		let value = to_value(bytes.clone()).unwrap();
		let expected = V1Value::Bytes(bytes);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn table() {
		let table = V1Table("foo".to_owned());
		let value = to_value(table.clone()).unwrap();
		let expected = V1Value::Table(table);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn thing() {
		let record_id: V1RecordId = sql::thing("foo:bar").unwrap().try_into().unwrap();
		let value = to_value(record_id.clone()).unwrap();
		let expected = V1Value::RecordId(record_id);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn regex() {
		let regex = "abc".parse::<V1Regex>().unwrap();
		let value = to_value(regex.clone()).unwrap();
		let expected = V1Value::Regex(regex);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn none() {
		let option: Option<V1Value> = None;
		let serialized = to_value(option).unwrap();
		assert_eq!(V1Value::None, serialized);
	}

	#[test]
	fn some() {
		let option = Some(V1Value::Bool(true));
		let serialized = to_value(option).unwrap();
		assert_eq!(V1Value::Bool(true), serialized);
	}

	#[test]
	fn empty_map() {
		let map: BTreeMap<String, V1Value> = Default::default();
		let serialized = to_value(map).unwrap();
		assert_eq!(V1Value::Object(Default::default()), serialized);
	}

	#[test]
	fn map() {
		let map = map! {
			String::from("foo") => V1Value::from("bar"),
		};
		let serialized = to_value(map.clone()).unwrap();
		assert_eq!(serialized, map.into());
	}

	#[test]
	fn empty_vec() {
		let vec: Vec<V1Value> = Vec::new();
		let serialized = to_value(vec).unwrap();
		assert_eq!(V1Value::Array(Default::default()), serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![V1Value::default()];
		let serialized = to_value(vec).unwrap();
		assert_eq!(V1Value::Array(vec![V1Value::None].into()), serialized);
	}
}
