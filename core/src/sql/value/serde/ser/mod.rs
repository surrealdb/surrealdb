mod r#enum;
mod r#struct;

use crate::err::Error;
use crate::sql;
use crate::sql::value::Value;
use castaway::match_type;
use serde::ser::Serialize;
use serde_content::Number;
use serde_content::Serializer;
use serde_content::Unexpected;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;

type Content = serde_content::Value<'static>;

/// Convert a `T` into `surrealdb::sql::Value` which is an enum that can represent any valid SQL data.
pub fn to_value<T>(value: T) -> Result<Value, Error>
where
	T: Serialize + 'static,
{
	match_type!(value, {
		Value as v => Ok(v),
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
		geo_types::LineString as v => Ok(Value::Geometry(v.into())),
		geo_types::Polygon as v => Ok(Value::Geometry(v.into())),
		geo_types::MultiPoint as v => Ok(Value::Geometry(v.into())),
		geo_types::MultiLineString as v => Ok(Value::Geometry(v.into())),
		geo_types::MultiPolygon as v => Ok(Value::Geometry(v.into())),
		geo_types::Point as v => Ok(Value::Geometry(v.into())),
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
		value => Serializer::new().serialize(value)?.try_into(),
	})
}

impl TryFrom<Content> for Value {
	type Error = Error;

	fn try_from(content: Content) -> Result<Self, Self::Error> {
		match content {
			Content::Unit => Ok(Value::None),
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
				Cow::Borrowed(v) => Ok(v.to_vec().into()),
				Cow::Owned(v) => Ok(v.into()),
			},
			Content::Seq(v) => v.try_into(),
			Content::Map(v) => v.try_into(),
			Content::Option(v) => match v {
				Some(v) => (*v).try_into(),
				None => Ok(Value::None),
			},
			Content::Struct(_) => r#struct::to_value(content),
			Content::Enum(_) => r#enum::to_value(content),
			Content::Tuple(v) => v.try_into(),
		}
	}
}

impl TryFrom<Vec<Content>> for Value {
	type Error = Error;

	fn try_from(v: Vec<Content>) -> Result<Self, Self::Error> {
		let mut vec = Vec::with_capacity(v.len());
		for content in v {
			vec.push(content.try_into()?);
		}
		Ok(Self::Array(sql::Array(vec)))
	}
}

impl TryFrom<Vec<(Content, Content)>> for Value {
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

impl TryFrom<Vec<(Cow<'static, str>, Content)>> for Value {
	type Error = Error;

	fn try_from(v: Vec<(Cow<'static, str>, Content)>) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		for (key, value) in v {
			map.insert(key.into_owned(), value.try_into()?);
		}
		Ok(Self::Object(sql::Object(map)))
	}
}

impl TryFrom<(Cow<'static, str>, Content)> for Value {
	type Error = Error;

	fn try_from((key, value): (Cow<'static, str>, Content)) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		map.insert(key.into_owned(), value.try_into()?);
		Ok(Self::Object(sql::Object(map)))
	}
}

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: Display,
	{
		Self::Serialization(msg.to_string())
	}
}

impl From<serde_content::Error> for Error {
	fn from(error: serde_content::Error) -> Self {
		Self::Serialization(error.to_string())
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
	use std::ops::Bound;

	#[test]
	fn value_none() {
		let expected = Value::None;
		assert_eq!(expected, to_value(None::<u32>).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn null() {
		let expected = Value::Null;
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#false() {
		let expected = Value::Bool(false);
		assert_eq!(expected, to_value(false).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn r#true() {
		let expected = Value::Bool(true);
		assert_eq!(expected, to_value(true).unwrap());
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn number() {
		let number = Number::Int(Default::default());
		let value = to_value(number.clone()).unwrap();
		let expected = Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = Number::Float(Default::default());
		let value = to_value(number.clone()).unwrap();
		let expected = Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let number = Number::Decimal(Default::default());
		let value = to_value(number.clone()).unwrap();
		let expected = Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn strand() {
		let strand = Strand("foobar".to_owned());
		let value = to_value(strand.clone()).unwrap();
		let expected = Value::Strand(strand);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar".to_owned();
		let value = to_value(strand.clone()).unwrap();
		let expected = Value::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let strand = "foobar";
		let value = to_value(strand).unwrap();
		let expected = Value::Strand(strand.into());
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn duration() {
		let duration = Duration::default();
		let value = to_value(duration).unwrap();
		let expected = Value::Duration(duration);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn datetime() {
		let datetime = Datetime::default();
		let value = to_value(datetime.clone()).unwrap();
		let expected = Value::Datetime(datetime);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn uuid() {
		let uuid = Uuid::default();
		let value = to_value(uuid).unwrap();
		let expected = Value::Uuid(uuid);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn array() {
		let array = Array::default();
		let value = to_value(array.clone()).unwrap();
		let expected = Value::Array(array);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn object() {
		let object = Object::default();
		let value = to_value(object.clone()).unwrap();
		let expected = Value::Object(object);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn geometry() {
		let geometry = Geometry::Collection(Vec::new());
		let value = to_value(geometry.clone()).unwrap();
		let expected = Value::Geometry(geometry);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn bytes() {
		let bytes = Bytes("foobar".as_bytes().to_owned());
		let value = to_value(bytes.clone()).unwrap();
		let expected = Value::Bytes(bytes);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn param() {
		let param = Param::default();
		let value = to_value(param.clone()).unwrap();
		let expected = Value::Param(param);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn idiom() {
		let idiom = Idiom::default();
		let value = to_value(idiom.clone()).unwrap();
		let expected = Value::Idiom(idiom);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn table() {
		let table = Table("foo".to_owned());
		let value = to_value(table.clone()).unwrap();
		let expected = Value::Table(table);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn thing() {
		let record_id = sql::thing("foo:bar").unwrap();
		let value = to_value(record_id.clone()).unwrap();
		let expected = Value::Thing(record_id);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn model() {
		let model = Mock::Count("foo".to_owned(), Default::default());
		let value = to_value(model.clone()).unwrap();
		let expected = Value::Mock(model);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn regex() {
		let regex = "abc".parse::<Regex>().unwrap();
		let value = to_value(regex.clone()).unwrap();
		let expected = Value::Regex(regex);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn block() {
		let block: Box<Block> = Default::default();
		let value = to_value(block.clone()).unwrap();
		let expected = Value::Block(block);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn range() {
		let range = Box::new(Range {
			tb: "foo".to_owned(),
			beg: Bound::Included("foo".into()),
			end: Bound::Unbounded,
		});
		let value = to_value(range.clone()).unwrap();
		let expected = Value::Range(range);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn edges() {
		let edges = Box::new(Edges {
			dir: Dir::In,
			from: sql::thing("foo:bar").unwrap(),
			what: Tables(vec!["foo".into()]),
		});
		let value = to_value(edges.clone()).unwrap();
		let expected = Value::Edges(edges);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn future() {
		let future = Box::new(Future(Value::default().into()));
		let value = to_value(future.clone()).unwrap();
		let expected = Value::Future(future);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());

		let future = Box::new(Future(Block(vec![Entry::Create(CreateStatement::default())])));
		let value = to_value(future.clone()).unwrap();
		let expected = Value::Future(future);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn constant() {
		let constant = Constant::MathPi;
		let value = to_value(constant.clone()).unwrap();
		let expected = Value::Constant(constant);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn function() {
		let function = Box::new(Function::Normal(Default::default(), Default::default()));
		let value = to_value(function.clone()).unwrap();
		let expected = Value::Function(function);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn query() {
		let query = sql::parse("SELECT * FROM foo").unwrap();
		let value = to_value(query.clone()).unwrap();
		let expected = Value::Query(query);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn subquery() {
		let subquery = Box::new(Subquery::Value(Value::None));
		let value = to_value(subquery.clone()).unwrap();
		let expected = Value::Subquery(subquery);
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
		let expected = Value::Expression(expression);
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
		let expected = Value::Object(
			map! {
				"foo".to_owned() => foo.into(),
				"bar".to_owned() => bar.into(),
			}
			.into(),
		);
		assert_eq!(value, expected);
		assert_eq!(expected.clone(), to_value(expected).unwrap());
	}

	#[test]
	fn none() {
		let option: Option<Value> = None;
		let serialized = to_value(option).unwrap();
		assert_eq!(Value::None, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Value::Bool(true));
		let serialized = to_value(option).unwrap();
		assert_eq!(Value::Bool(true), serialized);
	}

	#[test]
	fn empty_map() {
		let map: BTreeMap<String, Value> = Default::default();
		let serialized = to_value(map).unwrap();
		assert_eq!(Value::Object(Default::default()), serialized);
	}

	#[test]
	fn map() {
		let map = map! {
			String::from("foo") => Value::from("bar"),
		};
		let serialized = to_value(map.clone()).unwrap();
		assert_eq!(serialized, map.into());
	}

	#[test]
	fn empty_vec() {
		let vec: Vec<Value> = Vec::new();
		let serialized = to_value(vec).unwrap();
		assert_eq!(Value::Array(Default::default()), serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Value::default()];
		let serialized = to_value(vec).unwrap();
		assert_eq!(Value::Array(vec![Value::None].into()), serialized);
	}
}
