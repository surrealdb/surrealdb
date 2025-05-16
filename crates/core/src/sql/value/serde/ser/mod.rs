mod r#enum;
mod r#struct;

use crate::err::Error;
use crate::sql;
use crate::sql::value::SqlValue;
use crate::sql::Bytes;
use castaway::match_type;
use serde::ser::Serialize;
use serde_content::Number;
use serde_content::Serializer;
use serde_content::Unexpected;
use std::borrow::Cow;
use std::collections::BTreeMap;

type Content = serde_content::Value<'static>;

/// Convert a `T` into `surrealdb::sql::Value` which is an enum that can represent any valid SQL data.
pub fn to_value<T>(value: T) -> Result<SqlValue, Error>
where
	T: Serialize + 'static,
{
	match_type!(value, {
		SqlValue as v => Ok(v),
		sql::Number as v => Ok(v.into()),
		rust_decimal::Decimal as v => Ok(v.into()),
		sql::Strand as v => Ok(v.into()),
		sql::Duration as v => Ok(v.into()),
		core::time::Duration as v => Ok(v.into()),
		sql::Datetime as v => Ok(v.into()),
		chrono::DateTime<chrono::Utc> as v => Ok(v.into()),
		sql::Uuid as v => Ok(v.into()),
		uuid::Uuid as v => Ok(v.into()),
		sql::Array as v => Ok(v.into()),
		sql::Object as v => Ok(v.into()),
		sql::Geometry as v => Ok(v.into()),
		geo_types::Point as v => Ok(v.into()),
		geo_types::LineString as v => Ok(SqlValue::Geometry(v.into())),
		geo_types::Polygon as v => Ok(SqlValue::Geometry(v.into())),
		geo_types::MultiPoint as v => Ok(SqlValue::Geometry(v.into())),
		geo_types::MultiLineString as v => Ok(SqlValue::Geometry(v.into())),
		geo_types::MultiPolygon as v => Ok(SqlValue::Geometry(v.into())),
		geo_types::Point as v => Ok(SqlValue::Geometry(v.into())),
		sql::Bytes as v => Ok(v.into()),
		sql::Thing as v => Ok(v.into()),
		sql::Param as v => Ok(v.into()),
		sql::Idiom as v => Ok(v.into()),
		sql::Table as v => Ok(v.into()),
		sql::Mock as v => Ok(v.into()),
		sql::Regex as v => Ok(v.into()),
		sql::Cast as v => Ok(v.into()),
		sql::Block as v => Ok(v.into()),
		sql::Range as v => Ok(v.into()),
		sql::Edges as v => Ok(v.into()),
		sql::Future as v => Ok(v.into()),
		sql::Constant as v => Ok(v.into()),
		sql::Function as v => Ok(v.into()),
		sql::Subquery as v => Ok(v.into()),
		sql::Expression as v => Ok(v.into()),
		sql::Query as v => Ok(v.into()),
		sql::Model as v => Ok(v.into()),
		sql::Closure as v => Ok(v.into()),
		sql::File as v => Ok(v.into()),
		value => Serializer::new().serialize(value)?.try_into(),
	})
}

impl TryFrom<Content> for SqlValue {
	type Error = Error;

	fn try_from(content: Content) -> Result<Self, Self::Error> {
		match content {
			Content::Unit => Ok(SqlValue::None),
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
				_ => Err(Error::Serialization("unsupported number".to_owned())),
			},
			Content::Char(v) => Ok(v.to_string().into()),
			Content::String(v) => match v {
				Cow::Borrowed(v) => Ok(v.into()),
				Cow::Owned(v) => Ok(v.into()),
			},
			Content::Bytes(v) => match v {
				Cow::Borrowed(v) => Ok(SqlValue::Bytes(Bytes(v.to_vec()))),
				Cow::Owned(v) => Ok(SqlValue::Bytes(Bytes(v))),
			},
			Content::Seq(v) => v.try_into(),
			Content::Map(v) => v.try_into(),
			Content::Option(v) => match v {
				Some(v) => (*v).try_into(),
				None => Ok(SqlValue::None),
			},
			Content::Struct(_) => r#struct::to_value(content),
			Content::Enum(_) => r#enum::to_value(content),
			Content::Tuple(v) => v.try_into(),
		}
	}
}

impl TryFrom<Vec<Content>> for SqlValue {
	type Error = Error;

	fn try_from(v: Vec<Content>) -> Result<Self, Self::Error> {
		let mut vec = Vec::with_capacity(v.len());
		for content in v {
			vec.push(content.try_into()?);
		}
		Ok(Self::Array(sql::Array(vec)))
	}
}

impl TryFrom<Vec<(Content, Content)>> for SqlValue {
	type Error = Error;

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
		Ok(Self::Object(sql::Object(map)))
	}
}

impl TryFrom<Vec<(Cow<'static, str>, Content)>> for SqlValue {
	type Error = Error;

	fn try_from(v: Vec<(Cow<'static, str>, Content)>) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		for (key, value) in v {
			map.insert(key.into_owned(), value.try_into()?);
		}
		Ok(Self::Object(sql::Object(map)))
	}
}

impl TryFrom<(Cow<'static, str>, Content)> for SqlValue {
	type Error = Error;

	fn try_from((key, value): (Cow<'static, str>, Content)) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		map.insert(key.into_owned(), value.try_into()?);
		Ok(Self::Object(sql::Object(map)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql;
	use crate::sql::block::Entry;
	use crate::sql::statements::CreateStatement;
	use crate::sql::Number;
	use crate::sql::*;
	use ::serde::Serialize;
	use graph::{GraphSubject, GraphSubjects};
	use std::ops::Bound;

	#[test]
	fn value_none() {
		let expected = SqlValue::None;
		assert_eq!(expected, to_value(None::<u32>).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn null() {
		let expected = SqlValue::Null;
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#false() {
		let expected = SqlValue::Bool(false);
		assert_eq!(expected, to_value(false).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#true() {
		let expected = SqlValue::Bool(true);
		assert_eq!(expected, to_value(true).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn number() {
		let number = Number::Int(Default::default());
		let value = to_value(number).unwrap();
		let expected = SqlValue::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = Number::Float(Default::default());
		let value = to_value(number).unwrap();
		let expected = SqlValue::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = Number::Decimal(Default::default());
		let value = to_value(number).unwrap();
		let expected = SqlValue::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn strand() {
		let strand = Strand("foobar".to_owned());
		let value = to_value(strand.clone()).unwrap();
		let expected = SqlValue::Strand(strand);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar".to_owned();
		let value = to_value(strand.clone()).unwrap();
		let expected = SqlValue::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar";
		let value = to_value(strand).unwrap();
		let expected = SqlValue::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn duration() {
		let duration = Duration::default();
		let value = to_value(duration).unwrap();
		let expected = SqlValue::Duration(duration);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn datetime() {
		let datetime = Datetime::default();
		let value = to_value(datetime.clone()).unwrap();
		let expected = SqlValue::Datetime(datetime);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn uuid() {
		let uuid = Uuid::default();
		let value = to_value(uuid).unwrap();
		let expected = SqlValue::Uuid(uuid);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn array() {
		let array = Array::default();
		let value = to_value(array.clone()).unwrap();
		let expected = SqlValue::Array(array);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn object() {
		let object = Object::default();
		let value = to_value(object.clone()).unwrap();
		let expected = SqlValue::Object(object);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn geometry() {
		let geometry = Geometry::Collection(Vec::new());
		let value = to_value(geometry.clone()).unwrap();
		let expected = SqlValue::Geometry(geometry);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn bytes() {
		let bytes = Bytes("foobar".as_bytes().to_owned());
		let value = to_value(bytes.clone()).unwrap();
		let expected = SqlValue::Bytes(bytes);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn param() {
		let param = Param::default();
		let value = to_value(param.clone()).unwrap();
		let expected = SqlValue::Param(param);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn idiom() {
		let idiom = Idiom::default();
		let value = to_value(idiom.clone()).unwrap();
		let expected = SqlValue::Idiom(idiom);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn table() {
		let table = Table("foo".to_owned());
		let value = to_value(table.clone()).unwrap();
		let expected = SqlValue::Table(table);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn thing() {
		let record_id = sql::thing("foo:bar").unwrap();
		let value = to_value(record_id.clone()).unwrap();
		let expected = SqlValue::Thing(record_id);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn model() {
		let model = Mock::Count("foo".to_owned(), Default::default());
		let value = to_value(model.clone()).unwrap();
		let expected = SqlValue::Mock(model);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn regex() {
		let regex = "abc".parse::<Regex>().unwrap();
		let value = to_value(regex.clone()).unwrap();
		let expected = SqlValue::Regex(regex);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn block() {
		let block: Box<Block> = Default::default();
		let value = to_value(block.clone()).unwrap();
		let expected = SqlValue::Block(block);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn range() {
		let range = Box::new(Range {
			beg: Bound::Included("foo".into()),
			end: Bound::Unbounded,
		});
		let value = to_value(range.clone()).unwrap();
		let expected = SqlValue::Range(range);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn edges() {
		let edges = Box::new(Edges {
			dir: Dir::In,
			from: sql::thing("foo:bar").unwrap(),
			what: GraphSubjects(vec![GraphSubject::Table(Table("foo".into()))]),
		});
		let value = to_value(edges.clone()).unwrap();
		let expected = SqlValue::Edges(edges);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn future() {
		let future = Box::new(Future(SqlValue::default().into()));
		let value = to_value(future.clone()).unwrap();
		let expected = SqlValue::Future(future);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let future = Box::new(Future(Block(vec![Entry::Create(CreateStatement::default())])));
		let value = to_value(future.clone()).unwrap();
		let expected = SqlValue::Future(future);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn constant() {
		let constant = Constant::MathPi;
		let value = to_value(constant.clone()).unwrap();
		let expected = SqlValue::Constant(constant);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn function() {
		let function = Box::new(Function::Normal(Default::default(), Default::default()));
		let value = to_value(function.clone()).unwrap();
		let expected = SqlValue::Function(function);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn query() {
		let query = sql::parse("SELECT * FROM foo").unwrap();
		let value = to_value(query.clone()).unwrap();
		let expected = SqlValue::Query(query);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn subquery() {
		let subquery = Box::new(Subquery::Value(SqlValue::None));
		let value = to_value(subquery.clone()).unwrap();
		let expected = SqlValue::Subquery(subquery);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn expression() {
		let expression = Box::new(Expression::Binary {
			l: "foo".into(),
			o: Operator::Equal,
			r: "Bar".into(),
		});
		let value = to_value(expression.clone()).unwrap();
		let expected = SqlValue::Expression(expression);
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
		let expected = SqlValue::Object(
			map! {
				"foo".to_owned() => SqlValue::from(foo),
				"bar".to_owned() => SqlValue::from(bar),
			}
			.into(),
		);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn none() {
		let option: Option<SqlValue> = None;
		let serialized = to_value(option).unwrap();
		assert_eq!(SqlValue::None, serialized);
	}

	#[test]
	fn some() {
		let option = Some(SqlValue::Bool(true));
		let serialized = to_value(option).unwrap();
		assert_eq!(SqlValue::Bool(true), serialized);
	}

	#[test]
	fn empty_map() {
		let map: BTreeMap<String, SqlValue> = Default::default();
		let serialized = to_value(map).unwrap();
		assert_eq!(SqlValue::Object(Default::default()), serialized);
	}

	#[test]
	fn map() {
		let map = map! {
			String::from("foo") => SqlValue::from("bar"),
		};
		let serialized = to_value(map.clone()).unwrap();
		assert_eq!(serialized, map.into());
	}

	#[test]
	fn empty_vec() {
		let vec: Vec<SqlValue> = Vec::new();
		let serialized = to_value(vec).unwrap();
		assert_eq!(SqlValue::Array(Default::default()), serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![SqlValue::default()];
		let serialized = to_value(vec).unwrap();
		assert_eq!(SqlValue::Array(vec![SqlValue::None].into()), serialized);
	}
}
