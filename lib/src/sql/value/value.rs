#![allow(clippy::derive_ord_xor_partial_ord)]

use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::array::Uniq;
use crate::sql::array::{array, Array};
use crate::sql::block::{block, Block};
use crate::sql::bytes::Bytes;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::constant::{constant, Constant};
use crate::sql::datetime::{datetime, Datetime};
use crate::sql::duration::{duration, Duration};
use crate::sql::edges::{edges, Edges};
use crate::sql::error::IResult;
use crate::sql::expression::{expression, Expression};
use crate::sql::fmt::{Fmt, Pretty};
use crate::sql::function::{self, function, Function};
use crate::sql::future::{future, Future};
use crate::sql::geometry::{geometry, Geometry};
use crate::sql::id::Id;
use crate::sql::idiom::{self, Idiom};
use crate::sql::kind::Kind;
use crate::sql::model::{model, Model};
use crate::sql::number::{number, Number};
use crate::sql::object::{key, object, Object};
use crate::sql::operation::Operation;
use crate::sql::param::{param, Param};
use crate::sql::part::Part;
use crate::sql::range::{range, Range};
use crate::sql::regex::{regex, Regex};
use crate::sql::strand::{strand, Strand};
use crate::sql::subquery::{subquery, Subquery};
use crate::sql::table::{table, Table};
use crate::sql::thing::{thing, Thing};
use crate::sql::uuid::{uuid as unique, Uuid};
use async_recursion::async_recursion;
use bigdecimal::BigDecimal;
use bigdecimal::FromPrimitive;
use bigdecimal::ToPrimitive;
use chrono::{DateTime, Utc};
use derive::Store;
use fuzzy_matcher::skim::SkimMatcherV2;
use fuzzy_matcher::FuzzyMatcher;
use geo::Point;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::{map, opt};
use nom::multi::separated_list0;
use nom::multi::separated_list1;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;
use std::str::FromStr;

static MATCHER: Lazy<SkimMatcherV2> = Lazy::new(|| SkimMatcherV2::default().ignore_case());

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Value";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Values(pub Vec<Value>);

impl Deref for Values {
	type Target = Vec<Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Values {
	type Item = Value;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Values {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

pub fn values(i: &str) -> IResult<&str, Values> {
	let (i, v) = separated_list1(commas, value)(i)?;
	Ok((i, Values(v)))
}

pub fn selects(i: &str) -> IResult<&str, Values> {
	let (i, v) = separated_list1(commas, select)(i)?;
	Ok((i, Values(v)))
}

pub fn whats(i: &str) -> IResult<&str, Values> {
	let (i, v) = separated_list1(commas, what)(i)?;
	Ok((i, Values(v)))
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[format(Named)]
pub enum Value {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(Number),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Geometry(Geometry),
	Bytes(Bytes),
	// ---
	Param(Param),
	Idiom(Idiom),
	Table(Table),
	Thing(Thing),
	Model(Model),
	Regex(Regex),
	Block(Box<Block>),
	Range(Box<Range>),
	Edges(Box<Edges>),
	Future(Box<Future>),
	Constant(Constant),
	Function(Box<Function>),
	Subquery(Box<Subquery>),
	Expression(Box<Expression>),
	// Add new variants here
}

impl Eq for Value {}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}

impl From<bool> for Value {
	#[inline]
	fn from(v: bool) -> Self {
		Value::Bool(v)
	}
}

impl From<Uuid> for Value {
	fn from(v: Uuid) -> Self {
		Value::Uuid(v)
	}
}

impl From<Param> for Value {
	fn from(v: Param) -> Self {
		Value::Param(v)
	}
}

impl From<Idiom> for Value {
	fn from(v: Idiom) -> Self {
		Value::Idiom(v)
	}
}

impl From<Model> for Value {
	fn from(v: Model) -> Self {
		Value::Model(v)
	}
}

impl From<Table> for Value {
	fn from(v: Table) -> Self {
		Value::Table(v)
	}
}

impl From<Thing> for Value {
	fn from(v: Thing) -> Self {
		Value::Thing(v)
	}
}

impl From<Regex> for Value {
	fn from(v: Regex) -> Self {
		Value::Regex(v)
	}
}

impl From<Bytes> for Value {
	fn from(v: Bytes) -> Self {
		Value::Bytes(v)
	}
}

impl From<Array> for Value {
	fn from(v: Array) -> Self {
		Value::Array(v)
	}
}

impl From<Object> for Value {
	fn from(v: Object) -> Self {
		Value::Object(v)
	}
}

impl From<Number> for Value {
	fn from(v: Number) -> Self {
		Value::Number(v)
	}
}

impl From<Strand> for Value {
	fn from(v: Strand) -> Self {
		Value::Strand(v)
	}
}

impl From<Geometry> for Value {
	fn from(v: Geometry) -> Self {
		Value::Geometry(v)
	}
}

impl From<Datetime> for Value {
	fn from(v: Datetime) -> Self {
		Value::Datetime(v)
	}
}

impl From<Duration> for Value {
	fn from(v: Duration) -> Self {
		Value::Duration(v)
	}
}

impl From<Constant> for Value {
	fn from(v: Constant) -> Self {
		Value::Constant(v)
	}
}

impl From<Block> for Value {
	fn from(v: Block) -> Self {
		Value::Block(Box::new(v))
	}
}

impl From<Range> for Value {
	fn from(v: Range) -> Self {
		Value::Range(Box::new(v))
	}
}

impl From<Edges> for Value {
	fn from(v: Edges) -> Self {
		Value::Edges(Box::new(v))
	}
}

impl From<Future> for Value {
	fn from(v: Future) -> Self {
		Value::Future(Box::new(v))
	}
}

impl From<Function> for Value {
	fn from(v: Function) -> Self {
		Value::Function(Box::new(v))
	}
}

impl From<Subquery> for Value {
	fn from(v: Subquery) -> Self {
		Value::Subquery(Box::new(v))
	}
}

impl From<Expression> for Value {
	fn from(v: Expression) -> Self {
		Value::Expression(Box::new(v))
	}
}

impl From<Box<Edges>> for Value {
	fn from(v: Box<Edges>) -> Self {
		Value::Edges(v)
	}
}

impl From<i8> for Value {
	fn from(v: i8) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<i16> for Value {
	fn from(v: i16) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<i32> for Value {
	fn from(v: i32) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<i64> for Value {
	fn from(v: i64) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<i128> for Value {
	fn from(v: i128) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<isize> for Value {
	fn from(v: isize) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u8> for Value {
	fn from(v: u8) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u16> for Value {
	fn from(v: u16) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u32> for Value {
	fn from(v: u32) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u64> for Value {
	fn from(v: u64) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u128> for Value {
	fn from(v: u128) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<usize> for Value {
	fn from(v: usize) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<f32> for Value {
	fn from(v: f32) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<f64> for Value {
	fn from(v: f64) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<BigDecimal> for Value {
	fn from(v: BigDecimal) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<String> for Value {
	fn from(v: String) -> Self {
		Value::Strand(Strand::from(v))
	}
}

impl From<&str> for Value {
	fn from(v: &str) -> Self {
		Value::Strand(Strand::from(v))
	}
}

impl From<DateTime<Utc>> for Value {
	fn from(v: DateTime<Utc>) -> Self {
		Value::Datetime(Datetime::from(v))
	}
}

impl From<(f64, f64)> for Value {
	fn from(v: (f64, f64)) -> Self {
		Value::Geometry(Geometry::from(v))
	}
}

impl From<[f64; 2]> for Value {
	fn from(v: [f64; 2]) -> Self {
		Value::Geometry(Geometry::from(v))
	}
}

impl From<Point<f64>> for Value {
	fn from(v: Point<f64>) -> Self {
		Value::Geometry(Geometry::from(v))
	}
}

impl From<Operation> for Value {
	fn from(v: Operation) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<uuid::Uuid> for Value {
	fn from(v: uuid::Uuid) -> Self {
		Value::Uuid(Uuid(v))
	}
}

impl From<Vec<&str>> for Value {
	fn from(v: Vec<&str>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<Vec<String>> for Value {
	fn from(v: Vec<String>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<Vec<i32>> for Value {
	fn from(v: Vec<i32>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<Vec<Value>> for Value {
	fn from(v: Vec<Value>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<Vec<Number>> for Value {
	fn from(v: Vec<Number>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<Vec<Operation>> for Value {
	fn from(v: Vec<Operation>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<HashMap<String, Value>> for Value {
	fn from(v: HashMap<String, Value>) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<BTreeMap<String, Value>> for Value {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<Option<Value>> for Value {
	fn from(v: Option<Value>) -> Self {
		match v {
			Some(v) => v,
			None => Value::None,
		}
	}
}

impl From<Option<String>> for Value {
	fn from(v: Option<String>) -> Self {
		match v {
			Some(v) => Value::from(v),
			None => Value::None,
		}
	}
}

impl From<Id> for Value {
	fn from(v: Id) -> Self {
		match v {
			Id::Number(v) => v.into(),
			Id::String(v) => v.into(),
			Id::Object(v) => v.into(),
			Id::Array(v) => v.into(),
		}
	}
}

impl TryFrom<Value> for i8 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "i8")),
		}
	}
}

impl TryFrom<Value> for i16 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "i16")),
		}
	}
}

impl TryFrom<Value> for i32 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "i32")),
		}
	}
}

impl TryFrom<Value> for i64 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "i64")),
		}
	}
}

impl TryFrom<Value> for i128 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "i128")),
		}
	}
}

impl TryFrom<Value> for u8 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "u8")),
		}
	}
}

impl TryFrom<Value> for u16 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "u16")),
		}
	}
}

impl TryFrom<Value> for u32 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "u32")),
		}
	}
}

impl TryFrom<Value> for u64 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "u64")),
		}
	}
}

impl TryFrom<Value> for u128 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "u128")),
		}
	}
}

impl TryFrom<Value> for f32 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "f32")),
		}
	}
}

impl TryFrom<Value> for f64 {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "f64")),
		}
	}
}

impl TryFrom<Value> for BigDecimal {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "BigDecimal")),
		}
	}
}

impl TryFrom<Value> for String {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Strand(x) => Ok(x.into()),
			_ => Err(Error::TryFrom(value.to_string(), "String")),
		}
	}
}

impl TryFrom<Value> for bool {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Bool(boolean) => Ok(boolean),
			_ => Err(Error::TryFrom(value.to_string(), "bool")),
		}
	}
}

impl TryFrom<Value> for std::time::Duration {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Duration(x) => Ok(x.into()),
			_ => Err(Error::TryFrom(value.to_string(), "time::Duration")),
		}
	}
}

impl TryFrom<Value> for DateTime<Utc> {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Datetime(x) => Ok(x.into()),
			_ => Err(Error::TryFrom(value.to_string(), "chrono::DateTime<Utc>")),
		}
	}
}

impl TryFrom<Value> for uuid::Uuid {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Uuid(x) => Ok(x.into()),
			_ => Err(Error::TryFrom(value.to_string(), "uuid::Uuid")),
		}
	}
}

impl TryFrom<Value> for Vec<Value> {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Array(x) => Ok(x.into()),
			_ => Err(Error::TryFrom(value.to_string(), "Vec<Value>")),
		}
	}
}

impl TryFrom<Value> for Object {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Object(x) => Ok(x),
			_ => Err(Error::TryFrom(value.to_string(), "Object")),
		}
	}
}

impl Value {
	// -----------------------------------
	// Initial record value
	// -----------------------------------

	/// Create an empty Object Value
	pub fn base() -> Self {
		Value::Object(Object::default())
	}

	// -----------------------------------
	// Builtin types
	// -----------------------------------

	/// Convert this Value to a Result
	pub fn ok(self) -> Result<Value, Error> {
		Ok(self)
	}

	// -----------------------------------
	// Simple value detection
	// -----------------------------------

	/// Check if this Value is NONE or NULL
	pub fn is_none_or_null(&self) -> bool {
		matches!(self, Value::None | Value::Null)
	}

	/// Check if this Value is NONE
	pub fn is_none(&self) -> bool {
		matches!(self, Value::None)
	}

	/// Check if this Value is NULL
	pub fn is_null(&self) -> bool {
		matches!(self, Value::Null)
	}

	/// Check if this Value not NONE or NULL
	pub fn is_some(&self) -> bool {
		!self.is_none() && !self.is_null()
	}

	/// Check if this Value is a boolean value
	pub fn is_bool(&self) -> bool {
		self.is_true() || self.is_false()
	}

	/// Check if this Value is TRUE or 'true'
	pub fn is_true(&self) -> bool {
		matches!(self, Value::Bool(true))
	}

	/// Check if this Value is FALSE or 'false'
	pub fn is_false(&self) -> bool {
		matches!(self, Value::Bool(false))
	}

	/// Check if this Value is truthy
	pub fn is_truthy(&self) -> bool {
		match self {
			Value::Bool(boolean) => *boolean,
			Value::Uuid(_) => true,
			Value::Thing(_) => true,
			Value::Geometry(_) => true,
			Value::Array(v) => !v.is_empty(),
			Value::Object(v) => !v.is_empty(),
			Value::Strand(v) => !v.is_empty() && !v.eq_ignore_ascii_case("false"),
			Value::Number(v) => v.is_truthy(),
			Value::Duration(v) => v.as_nanos() > 0,
			Value::Datetime(v) => v.timestamp() > 0,
			_ => false,
		}
	}

	/// Check if this Value is a UUID
	pub fn is_uuid(&self) -> bool {
		matches!(self, Value::Uuid(_))
	}

	/// Check if this Value is a Thing
	pub fn is_thing(&self) -> bool {
		matches!(self, Value::Thing(_))
	}

	/// Check if this Value is a Model
	pub fn is_model(&self) -> bool {
		matches!(self, Value::Model(_))
	}

	/// Check if this Value is a Range
	pub fn is_range(&self) -> bool {
		matches!(self, Value::Range(_))
	}

	/// Check if this Value is a Table
	pub fn is_table(&self) -> bool {
		matches!(self, Value::Table(_))
	}

	/// Check if this Value is a Strand
	pub fn is_strand(&self) -> bool {
		matches!(self, Value::Strand(_))
	}

	/// Check if this Value is a float Number
	pub fn is_bytes(&self) -> bool {
		matches!(self, Value::Bytes(_))
	}

	/// Check if this Value is an Array
	pub fn is_array(&self) -> bool {
		matches!(self, Value::Array(_))
	}

	/// Check if this Value is an Object
	pub fn is_object(&self) -> bool {
		matches!(self, Value::Object(_))
	}

	/// Check if this Value is a Number
	pub fn is_number(&self) -> bool {
		matches!(self, Value::Number(_))
	}

	/// Check if this Value is a Datetime
	pub fn is_datetime(&self) -> bool {
		matches!(self, Value::Datetime(_))
	}

	/// Check if this Value is a Duration
	pub fn is_duration(&self) -> bool {
		matches!(self, Value::Duration(_))
	}

	/// Check if this Value is a Thing
	pub fn is_record(&self) -> bool {
		matches!(self, Value::Thing(_))
	}

	/// Check if this Value is a Geometry
	pub fn is_geometry(&self) -> bool {
		matches!(self, Value::Geometry(_))
	}

	/// Check if this Value is an int Number
	pub fn is_int(&self) -> bool {
		matches!(self, Value::Number(Number::Int(_)))
	}

	/// Check if this Value is a float Number
	pub fn is_float(&self) -> bool {
		matches!(self, Value::Number(Number::Float(_)))
	}

	/// Check if this Value is a decimal Number
	pub fn is_decimal(&self) -> bool {
		matches!(self, Value::Number(Number::Decimal(_)))
	}

	/// Check if this Value is a Number but is a NAN
	pub fn is_nan(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_nan())
	}

	/// Check if this Value is a Number and is an integer
	pub fn is_integer(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_integer())
	}

	/// Check if this Value is a Number and is positive
	pub fn is_positive(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_positive())
	}

	/// Check if this Value is a Number and is negative
	pub fn is_negative(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_negative())
	}

	/// Check if this Value is a Number and is zero or positive
	pub fn is_zero_or_positive(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_zero_or_positive())
	}

	/// Check if this Value is a Number and is zero or negative
	pub fn is_zero_or_negative(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_zero_or_negative())
	}

	/// Check if this Value is a Thing of a specific type
	pub fn is_record_type(&self, types: &[Table]) -> bool {
		match self {
			Value::Thing(v) => types.is_empty() || types.iter().any(|tb| tb.0 == v.tb),
			_ => false,
		}
	}

	/// Check if this Value is a Geometry of a specific type
	pub fn is_geometry_type(&self, types: &[String]) -> bool {
		match self {
			Value::Geometry(Geometry::Point(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "point"))
			}
			Value::Geometry(Geometry::Line(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "line"))
			}
			Value::Geometry(Geometry::Polygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "polygon"))
			}
			Value::Geometry(Geometry::MultiPoint(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipoint"))
			}
			Value::Geometry(Geometry::MultiLine(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multiline"))
			}
			Value::Geometry(Geometry::MultiPolygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipolygon"))
			}
			Value::Geometry(Geometry::Collection(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "collection"))
			}
			_ => false,
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	/// Convert this Value into a String
	pub fn as_string(self) -> String {
		match self {
			Value::Strand(v) => v.0,
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into an unquoted String
	pub fn as_raw_string(self) -> String {
		match self {
			Value::Strand(v) => v.0,
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	// -----------------------------------
	// Expensive conversion of value
	// -----------------------------------

	/// Converts this Value into an unquoted String
	pub fn to_raw_string(&self) -> String {
		match self {
			Value::Strand(v) => v.0.to_owned(),
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			Value::Idiom(v) => v.simplify(),
			Value::Param(v) => v.to_raw().into(),
			Value::Strand(v) => v.0.to_string().into(),
			Value::Datetime(v) => v.0.to_string().into(),
			Value::Future(_) => "future".to_string().into(),
			Value::Function(v) => v.to_idiom(),
			_ => self.to_string().into(),
		}
	}

	/// Try to convert this Value into a set of JSONPatch operations
	pub fn to_operations(&self) -> Result<Vec<Operation>, Error> {
		match self {
			Value::Array(v) => v
				.iter()
				.map(|v| match v {
					Value::Object(v) => v.to_operation(),
					_ => Err(Error::InvalidPatch {
						message: String::from("Operation must be an object"),
					}),
				})
				.collect::<Result<Vec<_>, Error>>(),
			_ => Err(Error::InvalidPatch {
				message: String::from("Operations must be an array"),
			}),
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	/// Treat a string as a table name
	pub fn could_be_table(self) -> Value {
		match self {
			Value::Strand(v) => Table::from(v.0).into(),
			_ => self,
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	/// Try to convert this value to the specified `Kind`
	pub(crate) fn convert_to(self, kind: &Kind) -> Result<Value, Error> {
		// Attempt to convert to the desired type
		let res = match kind {
			Kind::Any => Ok(self),
			Kind::Bool => self.convert_to_bool().map(Value::from),
			Kind::Int => self.convert_to_int().map(Value::from),
			Kind::Float => self.convert_to_float().map(Value::from),
			Kind::Decimal => self.convert_to_decimal().map(Value::from),
			Kind::Number => self.convert_to_number().map(Value::from),
			Kind::String => self.convert_to_strand().map(Value::from),
			Kind::Datetime => self.convert_to_datetime().map(Value::from),
			Kind::Duration => self.convert_to_duration().map(Value::from),
			Kind::Object => self.convert_to_object().map(Value::from),
			Kind::Point => self.convert_to_point().map(Value::from),
			Kind::Bytes => self.convert_to_bytes().map(Value::from),
			Kind::Uuid => self.convert_to_uuid().map(Value::from),
			Kind::Set(t, l) => self.convert_to_set_type(t, l).map(Value::from),
			Kind::Array(t, l) => self.convert_to_array_type(t, l).map(Value::from),
			Kind::Record(t) => match t.is_empty() {
				true => self.convert_to_record().map(Value::from),
				false => self.convert_to_record_type(t).map(Value::from),
			},
			Kind::Geometry(t) => match t.is_empty() {
				true => self.convert_to_geometry().map(Value::from),
				false => self.convert_to_geometry_type(t).map(Value::from),
			},
			Kind::Option(k) => match self {
				Self::None => Ok(Self::None),
				Self::Null => Ok(Self::None),
				v => v.convert_to(k),
			},
			Kind::Either(k) => {
				let mut val = self;
				for k in k {
					match val.convert_to(k) {
						Err(Error::ConvertTo {
							from,
							..
						}) => val = from,
						Err(e) => return Err(e),
						Ok(v) => return Ok(v),
					}
				}
				Err(Error::ConvertTo {
					from: val,
					into: kind.to_string().into(),
				})
			}
		};
		// Check for any conversion errors
		match res {
			// There was a conversion error
			Err(Error::ConvertTo {
				from,
				..
			}) => Err(Error::ConvertTo {
				from,
				into: kind.to_string().into(),
			}),
			// There was a different error
			Err(e) => Err(e),
			// Everything converted ok
			Ok(v) => Ok(v),
		}
	}

	/// Try to convert this value to an `i64`
	pub(crate) fn convert_to_i64(self) -> Result<i64, Error> {
		match self {
			// Allow any int number
			Value::Number(Number::Int(v)) => Ok(v),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as i64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(ref v)) if v.is_integer() => match v.to_i64() {
				// The Decimal can be represented as an i64
				Some(v) => Ok(v),
				// The Decimal is out of bounds
				_ => Err(Error::ConvertTo {
					from: self,
					into: "i64".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<i64>() {
				// The Strand can be represented as an i64
				Ok(v) => Ok(v),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "i64".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "i64".into(),
			}),
		}
	}

	/// Try to convert this value to an `u64`
	pub(crate) fn convert_to_u64(self) -> Result<u64, Error> {
		match self {
			// Allow any int number
			Value::Number(Number::Int(v)) => Ok(v as u64),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as u64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(ref v)) if v.is_integer() => match v.to_u64() {
				// The Decimal can be represented as an u64
				Some(v) => Ok(v),
				// The Decimal is out of bounds
				_ => Err(Error::ConvertTo {
					from: self,
					into: "u64".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<u64>() {
				// The Strand can be represented as a Float
				Ok(v) => Ok(v),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "u64".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "u64".into(),
			}),
		}
	}

	/// Try to convert this value to an `f64`
	pub(crate) fn convert_to_f64(self) -> Result<f64, Error> {
		match self {
			// Allow any float number
			Value::Number(Number::Float(v)) => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => Ok(v as f64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(ref v)) => match v.to_f64() {
				// The Decimal can be represented as a f64
				Some(v) => Ok(v),
				// Ths Decimal loses precision
				None => Err(Error::ConvertTo {
					from: self,
					into: "f64".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<f64>() {
				// The Strand can be represented as a f64
				Ok(v) => Ok(v),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "f64".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "f64".into(),
			}),
		}
	}

	/// Try to convert this value to a `bool`
	pub(crate) fn convert_to_bool(self) -> Result<bool, Error> {
		match self {
			// Allow any boolean value
			Value::Bool(boolean) => Ok(boolean),
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<bool>() {
				// The string can be represented as a Float
				Ok(v) => Ok(v),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "bool".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "bool".into(),
			}),
		}
	}

	/// Try to convert this value to an integer `Number`
	pub(crate) fn convert_to_int(self) -> Result<Number, Error> {
		match self {
			// Allow any int number
			Value::Number(v) if v.is_int() => Ok(v),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(Number::Int(v as i64)),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(ref v)) if v.is_integer() => match v.to_i64() {
				// The Decimal can be represented as an Int
				Some(v) => Ok(Number::Int(v)),
				// The Decimal is out of bounds
				_ => Err(Error::ConvertTo {
					from: self,
					into: "int".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<i64>() {
				// The string can be represented as a Float
				Ok(v) => Ok(Number::Int(v)),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "int".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "int".into(),
			}),
		}
	}

	/// Try to convert this value to a float `Number`
	pub(crate) fn convert_to_float(self) -> Result<Number, Error> {
		match self {
			// Allow any float number
			Value::Number(v) if v.is_float() => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => Ok(Number::Float(v as f64)),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(ref v)) => match v.to_f64() {
				// The Decimal can be represented as a Float
				Some(v) => Ok(Number::Float(v)),
				// Ths BigDecimal loses precision
				None => Err(Error::ConvertTo {
					from: self,
					into: "float".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<f64>() {
				// The string can be represented as a Float
				Ok(v) => Ok(Number::Float(v)),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "float".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "float".into(),
			}),
		}
	}

	/// Try to convert this value to a decimal `Number`
	pub(crate) fn convert_to_decimal(self) -> Result<Number, Error> {
		match self {
			// Allow any decimal number
			Value::Number(v) if v.is_decimal() => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(ref v)) => match BigDecimal::from_i64(*v) {
				// The Int can be represented as a Decimal
				Some(v) => Ok(Number::Decimal(v)),
				// Ths Int does not convert to a Decimal
				None => Err(Error::ConvertTo {
					from: self,
					into: "decimal".into(),
				}),
			},
			// Attempt to convert an float number
			Value::Number(Number::Float(ref v)) => match BigDecimal::from_f64(*v) {
				// The Float can be represented as a Decimal
				Some(v) => Ok(Number::Decimal(v)),
				// Ths Float does not convert to a Decimal
				None => Err(Error::ConvertTo {
					from: self,
					into: "decimal".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match BigDecimal::from_str(v) {
				// The string can be represented as a Float
				Ok(v) => Ok(Number::Decimal(v)),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "decimal".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "decimal".into(),
			}),
		}
	}

	/// Try to convert this value to a `Number`
	pub(crate) fn convert_to_number(self) -> Result<Number, Error> {
		match self {
			// Allow any number
			Value::Number(v) => Ok(v),
			// Attempt to convert a string value
			Value::Strand(ref v) => match Number::from_str(v) {
				// The string can be represented as a Float
				Ok(v) => Ok(v),
				// Ths string is not a float
				_ => Err(Error::ConvertTo {
					from: self,
					into: "number".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "number".into(),
			}),
		}
	}

	/// Try to convert this value to a `String`
	pub(crate) fn convert_to_string(self) -> Result<String, Error> {
		match self {
			// Bytes can't convert to strings
			Value::Bytes(_) => Err(Error::ConvertTo {
				from: self,
				into: "string".into(),
			}),
			// None can't convert to a string
			Value::None => Err(Error::ConvertTo {
				from: self,
				into: "string".into(),
			}),
			// Null can't convert to a string
			Value::Null => Err(Error::ConvertTo {
				from: self,
				into: "string".into(),
			}),
			// Stringify anything else
			_ => Ok(self.as_string()),
		}
	}

	/// Try to convert this value to a `Strand`
	pub(crate) fn convert_to_strand(self) -> Result<Strand, Error> {
		match self {
			// Bytes can't convert to strings
			Value::Bytes(_) => Err(Error::ConvertTo {
				from: self,
				into: "string".into(),
			}),
			// None can't convert to a string
			Value::None => Err(Error::ConvertTo {
				from: self,
				into: "string".into(),
			}),
			// Null can't convert to a string
			Value::Null => Err(Error::ConvertTo {
				from: self,
				into: "string".into(),
			}),
			// Allow any string value
			Value::Strand(v) => Ok(v),
			// Stringify anything else
			Value::Uuid(v) => Ok(v.to_raw().into()),
			// Stringify anything else
			Value::Datetime(v) => Ok(v.to_raw().into()),
			// Stringify anything else
			_ => Ok(self.to_string().into()),
		}
	}

	/// Try to convert this value to a `Uuid`
	pub(crate) fn convert_to_uuid(self) -> Result<Uuid, Error> {
		match self {
			// Uuids are allowed
			Value::Uuid(v) => Ok(v),
			// Attempt to parse a string
			Value::Strand(ref v) => match Uuid::try_from(v.as_str()) {
				// The string can be represented as a uuid
				Ok(v) => Ok(v),
				// Ths string is not a uuid
				Err(_) => Err(Error::ConvertTo {
					from: self,
					into: "uuid".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "uuid".into(),
			}),
		}
	}

	/// Try to convert this value to a `Datetime`
	pub(crate) fn convert_to_datetime(self) -> Result<Datetime, Error> {
		match self {
			// Datetimes are allowed
			Value::Datetime(v) => Ok(v),
			// Attempt to parse a string
			Value::Strand(ref v) => match Datetime::try_from(v.as_str()) {
				// The string can be represented as a datetime
				Ok(v) => Ok(v),
				// Ths string is not a datetime
				Err(_) => Err(Error::ConvertTo {
					from: self,
					into: "datetime".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "datetime".into(),
			}),
		}
	}

	/// Try to convert this value to a `Duration`
	pub(crate) fn convert_to_duration(self) -> Result<Duration, Error> {
		match self {
			// Durations are allowed
			Value::Duration(v) => Ok(v),
			// Attempt to parse a string
			Value::Strand(ref v) => match Duration::try_from(v.as_str()) {
				// The string can be represented as a duration
				Ok(v) => Ok(v),
				// Ths string is not a duration
				Err(_) => Err(Error::ConvertTo {
					from: self,
					into: "duration".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "duration".into(),
			}),
		}
	}

	/// Try to convert this value to a `Bytes`
	pub(crate) fn convert_to_bytes(self) -> Result<Bytes, Error> {
		match self {
			// Bytes are allowed
			Value::Bytes(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "bytes".into(),
			}),
		}
	}

	/// Try to convert this value to an `Object`
	pub(crate) fn convert_to_object(self) -> Result<Object, Error> {
		match self {
			// Objects are allowed
			Value::Object(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "object".into(),
			}),
		}
	}

	/// Try to convert this value to an `Array`
	pub(crate) fn convert_to_array(self) -> Result<Array, Error> {
		match self {
			// Arrays are allowed
			Value::Array(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "array".into(),
			}),
		}
	}

	/// Try to convert this value to an `Geometry` point
	pub(crate) fn convert_to_point(self) -> Result<Geometry, Error> {
		match self {
			// Geometry points are allowed
			Value::Geometry(Geometry::Point(v)) => Ok(v.into()),
			// An array of two floats are allowed
			Value::Array(ref v) if v.len() == 2 => match v.as_slice() {
				// The array can be represented as a point
				[Value::Number(v), Value::Number(w)] => Ok((v.to_float(), w.to_float()).into()),
				// The array is not a geometry point
				_ => Err(Error::ConvertTo {
					from: self,
					into: "point".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "point".into(),
			}),
		}
	}

	/// Try to convert this value to a Record or `Thing`
	pub(crate) fn convert_to_record(self) -> Result<Thing, Error> {
		match self {
			// Records are allowed
			Value::Thing(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "record".into(),
			}),
		}
	}

	/// Try to convert this value to an `Geometry` type
	pub(crate) fn convert_to_geometry(self) -> Result<Geometry, Error> {
		match self {
			// Geometries are allowed
			Value::Geometry(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "geometry".into(),
			}),
		}
	}

	/// Try to convert this value to a Record of a certain type
	pub(crate) fn convert_to_record_type(self, val: &[Table]) -> Result<Thing, Error> {
		match self {
			// Records are allowed if correct type
			Value::Thing(v) if self.is_record_type(val) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "record".into(),
			}),
		}
	}

	/// Try to convert this value to a `Geometry` of a certain type
	pub(crate) fn convert_to_geometry_type(self, val: &[String]) -> Result<Geometry, Error> {
		match self {
			// Geometries are allowed if correct type
			Value::Geometry(v) if self.is_geometry_type(val) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "geometry".into(),
			}),
		}
	}

	/// Try to convert this value to ab `Array` of a certain type and optional length
	pub(crate) fn convert_to_array_type(
		self,
		kind: &Kind,
		size: &Option<u64>,
	) -> Result<Array, Error> {
		match size {
			Some(l) => self
				.convert_to_array()?
				.into_iter()
				.map(|value| value.convert_to(kind))
				.collect::<Result<Array, Error>>()
				.map(|mut v| {
					v.truncate(*l as usize);
					v
				}),
			None => self
				.convert_to_array()?
				.into_iter()
				.map(|value| value.convert_to(kind))
				.collect::<Result<Array, Error>>(),
		}
	}

	/// Try to convert this value to an `Array` of a certain type, unique values, and optional length
	pub(crate) fn convert_to_set_type(
		self,
		kind: &Kind,
		size: &Option<u64>,
	) -> Result<Array, Error> {
		match size {
			Some(l) => self
				.convert_to_array()?
				.uniq()
				.into_iter()
				.map(|value| value.convert_to(kind))
				.collect::<Result<Array, Error>>()
				.map(|mut v| {
					v.truncate(*l as usize);
					v
				}),
			None => self
				.convert_to_array()?
				.uniq()
				.into_iter()
				.map(|value| value.convert_to(kind))
				.collect::<Result<Array, Error>>(),
		}
	}

	// -----------------------------------
	// Record ID extraction
	// -----------------------------------

	/// Fetch the record id if there is one
	pub fn record(self) -> Option<Thing> {
		match self {
			// This is an object so look for the id field
			Value::Object(mut v) => match v.remove("id") {
				Some(Value::Thing(v)) => Some(v),
				_ => None,
			},
			// This is an array so take the first item
			Value::Array(mut v) => match v.len() {
				1 => v.remove(0).record(),
				_ => None,
			},
			// This is a record id already
			Value::Thing(v) => Some(v),
			// There is no valid record id
			_ => None,
		}
	}

	// -----------------------------------
	// JSON Path conversion
	// -----------------------------------

	/// Converts this Value into a JSONPatch path
	pub(crate) fn jsonpath(&self) -> Idiom {
		self.to_raw_string()
			.as_str()
			.trim_start_matches('/')
			.split(&['.', '/'][..])
			.map(Part::from)
			.collect::<Vec<Part>>()
			.into()
	}

	// -----------------------------------
	// JSON Path conversion
	// -----------------------------------

	/// Checks whether this value is a static value
	pub(crate) fn is_static(&self) -> bool {
		match self {
			Value::None => true,
			Value::Null => true,
			Value::Bool(_) => true,
			Value::Uuid(_) => true,
			Value::Number(_) => true,
			Value::Strand(_) => true,
			Value::Duration(_) => true,
			Value::Datetime(_) => true,
			Value::Geometry(_) => true,
			Value::Array(v) => v.iter().all(Value::is_static),
			Value::Object(v) => v.values().all(Value::is_static),
			Value::Constant(_) => true,
			_ => false,
		}
	}

	// -----------------------------------
	// Value operations
	// -----------------------------------

	/// Check if this Value is equal to another Value
	pub fn equal(&self, other: &Value) -> bool {
		match self {
			Value::None => other.is_none(),
			Value::Null => other.is_null(),
			Value::Bool(boolean) => *boolean,
			Value::Uuid(v) => match other {
				Value::Uuid(w) => v == w,
				Value::Regex(w) => w.regex().is_match(v.to_raw().as_str()),
				_ => false,
			},
			Value::Thing(v) => match other {
				Value::Thing(w) => v == w,
				Value::Regex(w) => w.regex().is_match(v.to_raw().as_str()),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => v == w,
				Value::Regex(w) => w.regex().is_match(v.as_str()),
				_ => false,
			},
			Value::Regex(v) => match other {
				Value::Regex(w) => v == w,
				Value::Uuid(w) => v.regex().is_match(w.to_raw().as_str()),
				Value::Thing(w) => v.regex().is_match(w.to_raw().as_str()),
				Value::Strand(w) => v.regex().is_match(w.as_str()),
				_ => false,
			},
			Value::Array(v) => match other {
				Value::Array(w) => v == w,
				_ => false,
			},
			Value::Object(v) => match other {
				Value::Object(w) => v == w,
				_ => false,
			},
			Value::Number(v) => match other {
				Value::Number(w) => v == w,
				_ => false,
			},
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v == w,
				_ => false,
			},
			Value::Duration(v) => match other {
				Value::Duration(w) => v == w,
				_ => false,
			},
			Value::Datetime(v) => match other {
				Value::Datetime(w) => v == w,
				_ => false,
			},
			_ => self == other,
		}
	}

	/// Check if all Values in an Array are equal to another Value
	pub fn all_equal(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().all(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	/// Check if any Values in an Array are equal to another Value
	pub fn any_equal(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().any(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	/// Fuzzy check if this Value is equal to another Value
	pub fn fuzzy(&self, other: &Value) -> bool {
		match self {
			Value::Uuid(v) => match other {
				Value::Strand(w) => MATCHER.fuzzy_match(v.to_raw().as_str(), w.as_str()).is_some(),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => MATCHER.fuzzy_match(v.as_str(), w.as_str()).is_some(),
				_ => false,
			},
			_ => self.equal(other),
		}
	}

	/// Fuzzy check if all Values in an Array are equal to another Value
	pub fn all_fuzzy(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().all(|v| v.fuzzy(other)),
			_ => self.fuzzy(other),
		}
	}

	/// Fuzzy check if any Values in an Array are equal to another Value
	pub fn any_fuzzy(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().any(|v| v.fuzzy(other)),
			_ => self.fuzzy(other),
		}
	}

	/// Check if this Value contains another Value
	pub fn contains(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().any(|v| v.equal(other)),
			Value::Uuid(v) => match other {
				Value::Strand(w) => v.to_raw().contains(w.as_str()),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => v.contains(w.as_str()),
				_ => false,
			},
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v.contains(w),
				_ => false,
			},
			_ => false,
		}
	}

	/// Check if all Values in an Array contain another Value
	pub fn contains_all(&self, other: &Value) -> bool {
		match other {
			Value::Array(v) => v.iter().all(|v| match self {
				Value::Array(w) => w.iter().any(|w| v.equal(w)),
				Value::Geometry(_) => self.contains(v),
				_ => false,
			}),
			_ => false,
		}
	}

	/// Check if any Values in an Array contain another Value
	pub fn contains_any(&self, other: &Value) -> bool {
		match other {
			Value::Array(v) => v.iter().any(|v| match self {
				Value::Array(w) => w.iter().any(|w| v.equal(w)),
				Value::Geometry(_) => self.contains(v),
				_ => false,
			}),
			_ => false,
		}
	}

	/// Check if this Value intersects another Value
	pub fn intersects(&self, other: &Value) -> bool {
		match self {
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v.intersects(w),
				_ => false,
			},
			_ => false,
		}
	}

	// -----------------------------------
	// Sorting operations
	// -----------------------------------

	/// Compare this Value to another Value lexicographically
	pub fn lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value using natrual numerical comparison
	pub fn natural_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::natural_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value lexicographically and using natrual numerical comparison
	pub fn natural_lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::natural_lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}
}

impl fmt::Display for Value {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match self {
			Value::None => write!(f, "NONE"),
			Value::Null => write!(f, "NULL"),
			Value::Bool(v) => write!(f, "{v}"),
			Value::Number(v) => write!(f, "{v}"),
			Value::Strand(v) => write!(f, "{v}"),
			Value::Duration(v) => write!(f, "{v}"),
			Value::Datetime(v) => write!(f, "{v}"),
			Value::Uuid(v) => write!(f, "{v}"),
			Value::Array(v) => write!(f, "{v}"),
			Value::Object(v) => write!(f, "{v}"),
			Value::Geometry(v) => write!(f, "{v}"),
			Value::Param(v) => write!(f, "{v}"),
			Value::Idiom(v) => write!(f, "{v}"),
			Value::Table(v) => write!(f, "{v}"),
			Value::Thing(v) => write!(f, "{v}"),
			Value::Model(v) => write!(f, "{v}"),
			Value::Regex(v) => write!(f, "{v}"),
			Value::Block(v) => write!(f, "{v}"),
			Value::Range(v) => write!(f, "{v}"),
			Value::Edges(v) => write!(f, "{v}"),
			Value::Future(v) => write!(f, "{v}"),
			Value::Constant(v) => write!(f, "{v}"),
			Value::Function(v) => write!(f, "{v}"),
			Value::Subquery(v) => write!(f, "{v}"),
			Value::Expression(v) => write!(f, "{v}"),
			Value::Bytes(_) => write!(f, "<bytes>"),
		}
	}
}

impl Value {
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Value::Block(v) => v.writeable(),
			Value::Array(v) => v.iter().any(Value::writeable),
			Value::Object(v) => v.iter().any(|(_, v)| v.writeable()),
			Value::Function(v) => v.is_custom() || v.args().iter().any(Value::writeable),
			Value::Subquery(v) => v.writeable(),
			Value::Expression(v) => v.l.writeable() || v.r.writeable(),
			_ => false,
		}
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion Value>,
	) -> Result<Value, Error> {
		match self {
			Value::Thing(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Block(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Range(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Param(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Idiom(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Array(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Object(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Future(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Constant(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Function(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Subquery(v) => v.compute(ctx, opt, txn, doc).await,
			Value::Expression(v) => v.compute(ctx, opt, txn, doc).await,
			_ => Ok(self.to_owned()),
		}
	}
}

// ------------------------------

pub(crate) trait TryAdd<Rhs = Self> {
	type Output;
	fn try_add(self, rhs: Rhs) -> Result<Self::Output, Error>;
}

impl TryAdd for Value {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self, Error> {
		match (self, other) {
			(Value::Number(v), Value::Number(w)) => Ok(Value::Number(v + w)),
			(Value::Strand(v), Value::Strand(w)) => Ok(Value::Strand(v + w)),
			(Value::Datetime(v), Value::Duration(w)) => Ok(Value::Datetime(w + v)),
			(Value::Duration(v), Value::Datetime(w)) => Ok(Value::Datetime(v + w)),
			(Value::Duration(v), Value::Duration(w)) => Ok(Value::Duration(v + w)),
			(v, w) => Err(Error::TryAdd(v.to_raw_string(), w.to_raw_string())),
		}
	}
}

// ------------------------------

pub(crate) trait TrySub<Rhs = Self> {
	type Output;
	fn try_sub(self, v: Self) -> Result<Self::Output, Error>;
}

impl TrySub for Value {
	type Output = Self;
	fn try_sub(self, other: Self) -> Result<Self, Error> {
		match (self, other) {
			(Value::Number(v), Value::Number(w)) => Ok(Value::Number(v - w)),
			(Value::Datetime(v), Value::Datetime(w)) => Ok(Value::Duration(v - w)),
			(Value::Datetime(v), Value::Duration(w)) => Ok(Value::Datetime(w - v)),
			(Value::Duration(v), Value::Datetime(w)) => Ok(Value::Datetime(v - w)),
			(Value::Duration(v), Value::Duration(w)) => Ok(Value::Duration(v - w)),
			(v, w) => Err(Error::TrySub(v.to_raw_string(), w.to_raw_string())),
		}
	}
}

// ------------------------------

pub(crate) trait TryMul<Rhs = Self> {
	type Output;
	fn try_mul(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryMul for Value {
	type Output = Self;
	fn try_mul(self, other: Self) -> Result<Self, Error> {
		match (self, other) {
			(Value::Number(v), Value::Number(w)) => Ok(Value::Number(v * w)),
			(v, w) => Err(Error::TryMul(v.to_raw_string(), w.to_raw_string())),
		}
	}
}

// ------------------------------

pub(crate) trait TryDiv<Rhs = Self> {
	type Output;
	fn try_div(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryDiv for Value {
	type Output = Self;
	fn try_div(self, other: Self) -> Result<Self, Error> {
		match (self, other) {
			(Value::Number(v), Value::Number(w)) => match (v, w) {
				(_, w) if w == Number::Int(0) => Ok(Value::None),
				(v, w) => Ok(Value::Number(v / w)),
			},
			(v, w) => Err(Error::TryDiv(v.to_raw_string(), w.to_raw_string())),
		}
	}
}

// ------------------------------

pub(crate) trait TryPow<Rhs = Self> {
	type Output;
	fn try_pow(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryPow for Value {
	type Output = Self;
	fn try_pow(self, other: Self) -> Result<Self, Error> {
		match (self, other) {
			(Value::Number(v), Value::Number(w)) => Ok(Value::Number(v.pow(w))),
			(v, w) => Err(Error::TryPow(v.to_raw_string(), w.to_raw_string())),
		}
	}
}

// ------------------------------

/// Parse any `Value` including binary expressions
pub fn value(i: &str) -> IResult<&str, Value> {
	alt((map(expression, Value::from), single))(i)
}

/// Parse any `Value` excluding binary expressions
pub fn single(i: &str) -> IResult<&str, Value> {
	alt((
		alt((
			map(tag_no_case("NONE"), |_| Value::None),
			map(tag_no_case("NULL"), |_| Value::Null),
			map(tag_no_case("true"), |_| Value::Bool(true)),
			map(tag_no_case("false"), |_| Value::Bool(false)),
		)),
		alt((
			map(idiom::multi, Value::from),
			map(function, Value::from),
			map(subquery, Value::from),
			map(constant, Value::from),
			map(datetime, Value::from),
			map(duration, Value::from),
			map(geometry, Value::from),
			map(future, Value::from),
			map(unique, Value::from),
			map(number, Value::from),
			map(object, Value::from),
			map(array, Value::from),
			map(block, Value::from),
			map(param, Value::from),
			map(regex, Value::from),
			map(model, Value::from),
			map(edges, Value::from),
			map(range, Value::from),
			map(thing, Value::from),
			map(strand, Value::from),
			map(idiom::path, Value::from),
		)),
	))(i)
}

pub fn select(i: &str) -> IResult<&str, Value> {
	alt((
		alt((
			map(expression, Value::from),
			map(tag_no_case("NONE"), |_| Value::None),
			map(tag_no_case("NULL"), |_| Value::Null),
			map(tag_no_case("true"), |_| Value::Bool(true)),
			map(tag_no_case("false"), |_| Value::Bool(false)),
		)),
		alt((
			map(idiom::multi, Value::from),
			map(function, Value::from),
			map(subquery, Value::from),
			map(constant, Value::from),
			map(datetime, Value::from),
			map(duration, Value::from),
			map(geometry, Value::from),
			map(future, Value::from),
			map(unique, Value::from),
			map(number, Value::from),
			map(object, Value::from),
			map(array, Value::from),
			map(block, Value::from),
			map(param, Value::from),
			map(regex, Value::from),
			map(model, Value::from),
			map(edges, Value::from),
			map(range, Value::from),
			map(thing, Value::from),
			map(table, Value::from),
			map(strand, Value::from),
		)),
	))(i)
}

/// Used as the starting part of a complex Idiom
pub fn start(i: &str) -> IResult<&str, Value> {
	alt((
		map(function::normal, Value::from),
		map(function::custom, Value::from),
		map(subquery, Value::from),
		map(constant, Value::from),
		map(datetime, Value::from),
		map(duration, Value::from),
		map(unique, Value::from),
		map(number, Value::from),
		map(strand, Value::from),
		map(object, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(edges, Value::from),
		map(thing, Value::from),
	))(i)
}

/// Used in CREATE, UPDATE, and DELETE clauses
pub fn what(i: &str) -> IResult<&str, Value> {
	alt((
		map(function, Value::from),
		map(subquery, Value::from),
		map(constant, Value::from),
		map(datetime, Value::from),
		map(duration, Value::from),
		map(future, Value::from),
		map(block, Value::from),
		map(param, Value::from),
		map(model, Value::from),
		map(edges, Value::from),
		map(range, Value::from),
		map(thing, Value::from),
		map(table, Value::from),
	))(i)
}

/// Used to parse any simple JSON-like value
pub fn json(i: &str) -> IResult<&str, Value> {
	// Use a specific parser for JSON objects
	pub fn object(i: &str) -> IResult<&str, Object> {
		let (i, _) = char('{')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = separated_list0(commas, |i| {
			let (i, k) = key(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = char(':')(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, v) = json(i)?;
			Ok((i, (String::from(k), v)))
		})(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = opt(char(','))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char('}')(i)?;
		Ok((i, Object(v.into_iter().collect())))
	}
	// Use a specific parser for JSON arrays
	pub fn array(i: &str) -> IResult<&str, Array> {
		let (i, _) = char('[')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = separated_list0(commas, json)(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = opt(char(','))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char(']')(i)?;
		Ok((i, Array(v)))
	}
	// Parse any simple JSON-like value
	alt((
		map(tag_no_case("null".as_bytes()), |_| Value::Null),
		map(tag_no_case("true".as_bytes()), |_| Value::Bool(true)),
		map(tag_no_case("false".as_bytes()), |_| Value::Bool(false)),
		map(datetime, Value::from),
		map(geometry, Value::from),
		map(unique, Value::from),
		map(number, Value::from),
		map(object, Value::from),
		map(array, Value::from),
		map(thing, Value::from),
		map(strand, Value::from),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;
	use crate::sql::uuid::Uuid;

	#[test]
	fn check_none() {
		assert_eq!(true, Value::None.is_none());
		assert_eq!(false, Value::Null.is_none());
		assert_eq!(false, Value::from(1).is_none());
	}

	#[test]
	fn check_null() {
		assert_eq!(true, Value::Null.is_null());
		assert_eq!(false, Value::None.is_null());
		assert_eq!(false, Value::from(1).is_null());
	}

	#[test]
	fn check_true() {
		assert_eq!(false, Value::None.is_true());
		assert_eq!(true, Value::Bool(true).is_true());
		assert_eq!(false, Value::Bool(false).is_true());
		assert_eq!(false, Value::from(1).is_true());
		assert_eq!(false, Value::from("something").is_true());
	}

	#[test]
	fn check_false() {
		assert_eq!(false, Value::None.is_false());
		assert_eq!(false, Value::Bool(true).is_false());
		assert_eq!(true, Value::Bool(false).is_false());
		assert_eq!(false, Value::from(1).is_false());
		assert_eq!(false, Value::from("something").is_false());
	}

	#[test]
	fn convert_truthy() {
		assert_eq!(false, Value::None.is_truthy());
		assert_eq!(false, Value::Null.is_truthy());
		assert_eq!(true, Value::Bool(true).is_truthy());
		assert_eq!(false, Value::Bool(false).is_truthy());
		assert_eq!(false, Value::from(0).is_truthy());
		assert_eq!(true, Value::from(1).is_truthy());
		assert_eq!(true, Value::from(-1).is_truthy());
		assert_eq!(true, Value::from(1.1).is_truthy());
		assert_eq!(true, Value::from(-1.1).is_truthy());
		assert_eq!(true, Value::from("true").is_truthy());
		assert_eq!(false, Value::from("false").is_truthy());
		assert_eq!(true, Value::from("falsey").is_truthy());
		assert_eq!(true, Value::from("something").is_truthy());
		assert_eq!(true, Value::from(Uuid::new()).is_truthy());
	}

	#[test]
	fn convert_string() {
		assert_eq!(String::from("NONE"), Value::None.as_string());
		assert_eq!(String::from("NULL"), Value::Null.as_string());
		assert_eq!(String::from("true"), Value::Bool(true).as_string());
		assert_eq!(String::from("false"), Value::Bool(false).as_string());
		assert_eq!(String::from("0"), Value::from(0).as_string());
		assert_eq!(String::from("1"), Value::from(1).as_string());
		assert_eq!(String::from("-1"), Value::from(-1).as_string());
		assert_eq!(String::from("1.1"), Value::from(1.1).as_string());
		assert_eq!(String::from("-1.1"), Value::from(-1.1).as_string());
		assert_eq!(String::from("3"), Value::from("3").as_string());
		assert_eq!(String::from("true"), Value::from("true").as_string());
		assert_eq!(String::from("false"), Value::from("false").as_string());
		assert_eq!(String::from("something"), Value::from("something").as_string());
	}

	#[test]
	fn check_size() {
		assert_eq!(64, std::mem::size_of::<Value>());
		assert_eq!(104, std::mem::size_of::<Error>());
		assert_eq!(104, std::mem::size_of::<Result<Value, Error>>());
		assert_eq!(40, std::mem::size_of::<crate::sql::number::Number>());
		assert_eq!(24, std::mem::size_of::<crate::sql::strand::Strand>());
		assert_eq!(16, std::mem::size_of::<crate::sql::duration::Duration>());
		assert_eq!(12, std::mem::size_of::<crate::sql::datetime::Datetime>());
		assert_eq!(24, std::mem::size_of::<crate::sql::array::Array>());
		assert_eq!(24, std::mem::size_of::<crate::sql::object::Object>());
		assert_eq!(56, std::mem::size_of::<crate::sql::geometry::Geometry>());
		assert_eq!(24, std::mem::size_of::<crate::sql::param::Param>());
		assert_eq!(24, std::mem::size_of::<crate::sql::idiom::Idiom>());
		assert_eq!(24, std::mem::size_of::<crate::sql::table::Table>());
		assert_eq!(56, std::mem::size_of::<crate::sql::thing::Thing>());
		assert_eq!(48, std::mem::size_of::<crate::sql::model::Model>());
		assert_eq!(16, std::mem::size_of::<crate::sql::regex::Regex>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::range::Range>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::edges::Edges>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::function::Function>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::subquery::Subquery>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::expression::Expression>>());
	}

	#[test]
	fn check_serialize() {
		assert_eq!(5, Value::None.to_vec().len());
		assert_eq!(5, Value::Null.to_vec().len());
		assert_eq!(7, Value::Bool(true).to_vec().len());
		assert_eq!(7, Value::Bool(false).to_vec().len());
		assert_eq!(13, Value::from("test").to_vec().len());
		assert_eq!(29, Value::parse("{ hello: 'world' }").to_vec().len());
		assert_eq!(45, Value::parse("{ compact: true, schema: 0 }").to_vec().len());
	}

	#[test]
	fn serialize_deserialize() {
		let val = Value::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { something: false }] } }",
		);
		let res = Value::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { something: false }] } }",
		);
		let enc: Vec<u8> = val.into();
		let dec: Value = enc.into();
		assert_eq!(res, dec);
	}
}
