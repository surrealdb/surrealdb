mod r#enum;
mod r#struct;

use std::borrow::Cow;

use anyhow::Result;
use castaway::match_type;
use serde::ser::Serialize;
use serde_content::{Number, Serializer, Unexpected, Value as Content};
use surrealdb_core::val;

use crate::error::Api;

//type Content = serde_content::Value<'static>;

/// Convert a `T` into `surrealdb::expr::Value` which is an enum that can represent any valid SQL
/// data.
pub fn to_value<T>(value: T) -> Result<val::Value>
where
	T: Serialize + 'static,
{
	match_type!(value, {
		val::Value as v => Ok(v),
		val::Number as v => Ok(v.into()),
		rust_decimal::Decimal as v => Ok(val::Number::Decimal(v).into()),
		val::Strand as v => Ok(v.into()),
		val::Duration as v => Ok(v.into()),
		core::time::Duration as v => Ok(val::Duration(v).into()),
		val::Datetime as v => Ok(v.into()),
		chrono::DateTime<chrono::Utc> as v => Ok(val::Datetime(v).into()),
		val::Uuid as v => Ok(v.into()),
		uuid::Uuid as v => Ok(val::Uuid(v).into()),
		val::Array as v => Ok(v.into()),
		val::Object as v => Ok(v.into()),
		val::Geometry as v => Ok(v.into()),
		geo::Point as v => Ok(v.into()),
		geo::LineString as v => Ok(val::Value::Geometry(v.into())),
		geo::Polygon as v => Ok(val::Value::Geometry(v.into())),
		geo::MultiPoint as v => Ok(val::Value::Geometry(v.into())),
		geo::MultiLineString as v => Ok(val::Value::Geometry(v.into())),
		geo::MultiPolygon as v => Ok(val::Value::Geometry(v.into())),
		geo::Point as v => Ok(val::Value::Geometry(v.into())),
		val::Bytes as v => Ok(v.into()),
		val::RecordId as v => Ok(v.into()),
		val::Table as v => Ok(v.into()),
		val::Regex as v => Ok(v.into()),
		val::Range as v => Ok(v.into()),
		val::Closure as v => Ok(v.into()),
		val::File as v => Ok(v.into()),
		value => value_from_content(Serializer::new().serialize(value)?),
	})
}

fn value_from_content(content: Content) -> Result<val::Value> {
	match content {
		Content::Unit => Ok(val::Value::None),
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
			_ => Err(anyhow::Error::new(Api::DeSerializeValue("unsupported number".to_owned()))),
		},
		Content::Char(v) => Ok(v.to_string().into()),
		// TODO: Null byte validity
		Content::String(v) => Ok(unsafe { val::Strand::new_unchecked(v.into_owned()) }.into()),
		Content::Bytes(v) => Ok(val::Bytes::from(v.into_owned()).into()),
		Content::Seq(v) => array_from_content(v),
		Content::Map(v) => object_from_content(v),
		Content::Option(v) => {
			Ok(v.map(|x| value_from_content(*x)).transpose()?.unwrap_or(val::Value::None))
		}
		Content::Struct(_) => r#struct::to_value(content),
		Content::Enum(_) => r#enum::to_value(content),
		Content::Tuple(v) => array_from_content(v),
	}
}

fn array_from_content(map: Vec<Content>) -> Result<val::Value> {
	map.into_iter().map(value_from_content).collect()
}

fn object_from_content(map: Vec<(Content, Content)>) -> Result<val::Value> {
	let mut res = val::Object::new();
	for (k, v) in map {
		let k = match k {
			Content::String(v) => v.into_owned(),
			x => return Err(x.unexpected(serde_content::Expected::String).into()),
		};
		let v = value_from_content(v)?;
		res.insert(k, v);
	}
	Ok(res.into())
}

fn object_from_content_struct(map: Vec<(Cow<'static, str>, Content)>) -> Result<val::Value> {
	let mut res = val::Object::new();
	for (k, v) in map {
		let v = value_from_content(v)?;
		res.insert(k.into_owned(), v);
	}
	Ok(res.into())
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::ops::Bound;

	use ::serde::Serialize;
	use surrealdb_core::val::Regex;
	use surrealdb_core::{map, syn};

	use super::*;

	#[test]
	fn value_none() {
		let expected = val::Value::None;
		assert_eq!(expected, to_value(None::<u32>).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn null() {
		let expected = val::Value::Null;
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#false() {
		let expected = val::Value::Bool(false);
		assert_eq!(expected, to_value(false).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#true() {
		let expected = val::Value::Bool(true);
		assert_eq!(expected, to_value(true).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn number() {
		let number = val::Number::Int(Default::default());
		let value = to_value(number).unwrap();
		let expected = val::Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = val::Number::Float(Default::default());
		let value = to_value(number).unwrap();
		let expected = val::Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = val::Number::Decimal(Default::default());
		let value = to_value(number).unwrap();
		let expected = val::Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn strand() {
		let strand = val::Strand::new("foobar".to_owned()).unwrap();
		let value = to_value(strand.clone()).unwrap();
		let expected = val::Value::Strand(strand);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar".to_owned();
		let value = to_value(strand.clone()).unwrap();
		let expected = val::Value::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar";
		let value = to_value(strand).unwrap();
		let expected = val::Value::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn duration() {
		let duration = val::Duration::default();
		let value = to_value(duration).unwrap();
		let expected = val::Value::Duration(duration);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn datetime() {
		let datetime = val::Datetime::now();
		let value = to_value(datetime.clone()).unwrap();
		let expected = val::Value::Datetime(datetime);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn uuid() {
		let uuid = val::Uuid::default();
		let value = to_value(uuid).unwrap();
		let expected = val::Value::Uuid(uuid);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn array() {
		let array = val::Array::default();
		let value = to_value(array.clone()).unwrap();
		let expected = val::Value::Array(array);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn object() {
		let object = val::Object::default();
		let value = to_value(object.clone()).unwrap();
		let expected = val::Value::Object(object);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn geometry() {
		let geometry = val::Geometry::Collection(Vec::new());
		let value = to_value(geometry.clone()).unwrap();
		let expected = val::Value::Geometry(geometry);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn bytes() {
		let bytes = val::Bytes::from("foobar".as_bytes().to_owned());
		let value = to_value(bytes.clone()).unwrap();
		let expected = val::Value::Bytes(bytes);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn table() {
		let table = val::Table::new("foo".to_owned()).unwrap();
		let value = to_value(table.clone()).unwrap();
		let expected = val::Value::Table(table);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn thing() {
		let record_id = syn::record_id("foo:bar").unwrap();
		let value = to_value(record_id.clone()).unwrap();
		let expected = val::Value::RecordId(record_id);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn regex() {
		let regex = "abc".parse::<Regex>().unwrap();
		let value = to_value(regex.clone()).unwrap();
		let expected = val::Value::Regex(regex);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn range() {
		let range = val::Range {
			start: Bound::Included(val::Strand::new("foo".to_owned()).unwrap().into()),
			end: Bound::Unbounded,
		};
		let value = to_value(range.clone()).unwrap();
		let expected = val::Value::Range(Box::new(range));
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn unknown_struct() {
		#[derive(Debug, Serialize)]
		struct FooBar {
			foo: String,
			bar: i32,
		}

		let foo = "Foo";
		let bar = Default::default();
		let foo_bar = FooBar {
			bar,
			foo: foo.to_owned(),
		};
		let value = to_value(foo_bar).unwrap();
		let expected = val::Value::Object(
			map! {
				"foo".to_owned() => val::Value::from(foo),
				"bar".to_owned() => val::Value::from(bar),
			}
			.into(),
		);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn none() {
		let option: Option<val::Value> = None;
		let serialized = to_value(option).unwrap();
		assert_eq!(val::Value::None, serialized);
	}

	#[test]
	fn some() {
		let option = Some(val::Value::Bool(true));
		let serialized = to_value(option).unwrap();
		assert_eq!(val::Value::Bool(true), serialized);
	}

	#[test]
	fn empty_map() {
		let map: BTreeMap<String, val::Value> = Default::default();
		let serialized = to_value(map).unwrap();
		assert_eq!(val::Value::Object(Default::default()), serialized);
	}

	#[test]
	fn map() {
		let map = map! {
			String::from("foo") => val::Value::from("bar"),
		};
		let serialized = to_value(map.clone()).unwrap();
		assert_eq!(serialized, val::Value::from(map));
	}

	#[test]
	fn empty_vec() {
		let vec: Vec<val::Value> = Vec::new();
		let serialized = to_value(vec).unwrap();
		assert_eq!(val::Value::Array(Default::default()), serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![val::Value::default()];
		let serialized = to_value(vec).unwrap();
		assert_eq!(val::Value::Array(vec![val::Value::None].into()), serialized);
	}
}
