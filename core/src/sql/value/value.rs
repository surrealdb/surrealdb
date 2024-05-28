#![allow(clippy::derive_ord_xor_partial_ord)]

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc::util::string::fuzzy::Fuzzy;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{
	array::Uniq,
	fmt::{Fmt, Pretty},
	id::{Gen, Id},
	model::Model,
	Array, Block, Bytes, Cast, Constant, Datetime, Duration, Edges, Expression, Function, Future,
	Geometry, Idiom, Kind, Mock, Number, Object, Operation, Param, Part, Query, Range, Regex,
	Strand, Subquery, Table, Thing, Uuid,
};
use chrono::{DateTime, Utc};
use derive::Store;
use geo::Point;
use reblessive::tree::Stk;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Value";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Value {
	// These value types are simple values which
	// can be used in query responses sent to
	// the client. They typically do not need to
	// be computed, unless an un-computed value
	// is present inside an Array or Object type.
	// These types can also be used within indexes
	// and sort according to their order below.
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
	Thing(Thing),
	// These Value types are un-computed values
	// and are not used in query responses sent
	// to the client. These types need to be
	// computed, in order to convert them into
	// one of the simple types listed above.
	// These types are first computed into a
	// simple type before being used in indexes.
	Param(Param),
	Idiom(Idiom),
	Table(Table),
	Mock(Mock),
	Regex(Regex),
	Cast(Box<Cast>),
	Block(Box<Block>),
	Range(Box<Range>),
	Edges(Box<Edges>),
	Future(Box<Future>),
	Constant(Constant),
	Function(Box<Function>),
	Subquery(Box<Subquery>),
	Expression(Box<Expression>),
	Query(Query),
	Model(Box<Model>),
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

impl From<Mock> for Value {
	fn from(v: Mock) -> Self {
		Value::Mock(v)
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

impl From<Cast> for Value {
	fn from(v: Cast) -> Self {
		Value::Cast(Box::new(v))
	}
}

impl From<Function> for Value {
	fn from(v: Function) -> Self {
		Value::Function(Box::new(v))
	}
}

impl From<Model> for Value {
	fn from(v: Model) -> Self {
		Value::Model(Box::new(v))
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

impl From<Decimal> for Value {
	fn from(v: Decimal) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<String> for Value {
	fn from(v: String) -> Self {
		Self::Strand(Strand::from(v))
	}
}

impl From<&str> for Value {
	fn from(v: &str) -> Self {
		Self::Strand(Strand::from(v))
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

impl From<Vec<bool>> for Value {
	fn from(v: Vec<bool>) -> Self {
		Value::Array(Array::from(v))
	}
}

impl From<HashMap<&str, Value>> for Value {
	fn from(v: HashMap<&str, Value>) -> Self {
		Value::Object(Object::from(v))
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

impl From<BTreeMap<&str, Value>> for Value {
	fn from(v: BTreeMap<&str, Value>) -> Self {
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

impl From<Option<i64>> for Value {
	fn from(v: Option<i64>) -> Self {
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
			Id::Array(v) => v.into(),
			Id::Object(v) => v.into(),
			Id::Generate(v) => match v {
				Gen::Rand => Id::rand().into(),
				Gen::Ulid => Id::ulid().into(),
				Gen::Uuid => Id::uuid().into(),
			},
		}
	}
}

impl From<Query> for Value {
	fn from(q: Query) -> Self {
		Value::Query(q)
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

impl TryFrom<Value> for Decimal {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => x.try_into(),
			_ => Err(Error::TryFrom(value.to_string(), "Decimal")),
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
			Value::Bool(v) => Ok(v),
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

impl TryFrom<Value> for Number {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => Ok(x),
			_ => Err(Error::TryFrom(value.to_string(), "Number")),
		}
	}
}

impl TryFrom<&Value> for Number {
	type Error = Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Number(x) => Ok(x.clone()),
			_ => Err(Error::TryFrom(value.to_string(), "Number")),
		}
	}
}

impl TryFrom<Value> for Datetime {
	type Error = Error;
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Datetime(x) => Ok(x),
			_ => Err(Error::TryFrom(value.to_string(), "Datetime")),
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

impl FromIterator<Value> for Value {
	fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
		Value::Array(Array(iter.into_iter().collect()))
	}
}

impl FromIterator<(String, Value)> for Value {
	fn from_iter<I: IntoIterator<Item = (String, Value)>>(iter: I) -> Self {
		Value::Object(Object(iter.into_iter().collect()))
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

	/// Convert this Value to an Option
	pub fn some(self) -> Option<Value> {
		match self {
			Value::None => None,
			val => Some(val),
		}
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
		matches!(self, Value::Bool(_))
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
			Value::Bool(v) => *v,
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

	/// Check if this Value is a Mock
	pub fn is_mock(&self) -> bool {
		matches!(self, Value::Mock(_))
	}

	/// Check if this Value is a Param
	pub fn is_param(&self) -> bool {
		matches!(self, Value::Param(_))
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

	/// Check if this Value is a Query
	pub fn is_query(&self) -> bool {
		matches!(self, Value::Query(_))
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

	/// Check if this Value is a Thing, and belongs to a certain table
	pub fn is_record_of_table(&self, table: String) -> bool {
		match self {
			Value::Thing(Thing {
				tb,
				..
			}) => *tb == table,
			_ => false,
		}
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

	/// Returns if this value can be the start of a idiom production.
	pub fn can_start_idiom(&self) -> bool {
		match self {
			Value::Function(x) => !x.is_script(),
			Value::Model(_)
			| Value::Subquery(_)
			| Value::Constant(_)
			| Value::Datetime(_)
			| Value::Duration(_)
			| Value::Uuid(_)
			| Value::Number(_)
			| Value::Object(_)
			| Value::Array(_)
			| Value::Param(_)
			| Value::Edges(_)
			| Value::Thing(_)
			| Value::Table(_) => true,
			_ => false,
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

	/// Converts a `surrealdb::sq::Value` into a `serde_json::Value`
	///
	/// This converts certain types like `Thing` into their simpler formats
	/// instead of the format used internally by SurrealDB.
	pub fn into_json(self) -> Json {
		self.into()
	}

	// -----------------------------------
	// Simple conversion of values
	// -----------------------------------

	/// Treat a string as a table name
	pub fn could_be_table(self) -> Value {
		match self {
			Value::Strand(v) => Value::Table(v.0.into()),
			_ => self,
		}
	}

	// -----------------------------------
	// Simple output of value type
	// -----------------------------------

	/// Treat a string as a table name
	pub fn kindof(&self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Null => "null",
			Self::Bool(_) => "bool",
			Self::Uuid(_) => "uuid",
			Self::Array(_) => "array",
			Self::Object(_) => "object",
			Self::Strand(_) => "string",
			Self::Duration(_) => "duration",
			Self::Datetime(_) => "datetime",
			Self::Number(Number::Int(_)) => "int",
			Self::Number(Number::Float(_)) => "float",
			Self::Number(Number::Decimal(_)) => "decimal",
			Self::Geometry(Geometry::Point(_)) => "geometry<point>",
			Self::Geometry(Geometry::Line(_)) => "geometry<line>",
			Self::Geometry(Geometry::Polygon(_)) => "geometry<polygon>",
			Self::Geometry(Geometry::MultiPoint(_)) => "geometry<multipoint>",
			Self::Geometry(Geometry::MultiLine(_)) => "geometry<multiline>",
			Self::Geometry(Geometry::MultiPolygon(_)) => "geometry<multipolygon>",
			Self::Geometry(Geometry::Collection(_)) => "geometry<collection>",
			Self::Bytes(_) => "bytes",
			_ => "incorrect type",
		}
	}

	// -----------------------------------
	// Simple type coercion of values
	// -----------------------------------

	/// Try to coerce this value to the specified `Kind`
	pub(crate) fn coerce_to(self, kind: &Kind) -> Result<Value, Error> {
		// Attempt to convert to the desired type
		let res = match kind {
			Kind::Any => Ok(self),
			Kind::Null => self.coerce_to_null(),
			Kind::Bool => self.coerce_to_bool().map(Value::from),
			Kind::Int => self.coerce_to_int().map(Value::from),
			Kind::Float => self.coerce_to_float().map(Value::from),
			Kind::Decimal => self.coerce_to_decimal().map(Value::from),
			Kind::Number => self.coerce_to_number().map(Value::from),
			Kind::String => self.coerce_to_strand().map(Value::from),
			Kind::Datetime => self.coerce_to_datetime().map(Value::from),
			Kind::Duration => self.coerce_to_duration().map(Value::from),
			Kind::Object => self.coerce_to_object().map(Value::from),
			Kind::Point => self.coerce_to_point().map(Value::from),
			Kind::Bytes => self.coerce_to_bytes().map(Value::from),
			Kind::Uuid => self.coerce_to_uuid().map(Value::from),
			Kind::Set(t, l) => match l {
				Some(l) => self.coerce_to_set_type_len(t, l).map(Value::from),
				None => self.coerce_to_set_type(t).map(Value::from),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.coerce_to_array_type_len(t, l).map(Value::from),
				None => self.coerce_to_array_type(t).map(Value::from),
			},
			Kind::Record(t) => match t.is_empty() {
				true => self.coerce_to_record().map(Value::from),
				false => self.coerce_to_record_type(t).map(Value::from),
			},
			Kind::Geometry(t) => match t.is_empty() {
				true => self.coerce_to_geometry().map(Value::from),
				false => self.coerce_to_geometry_type(t).map(Value::from),
			},
			Kind::Option(k) => match self {
				Self::None => Ok(Self::None),
				v => v.coerce_to(k),
			},
			Kind::Either(k) => {
				let mut val = self;
				for k in k {
					match val.coerce_to(k) {
						Err(Error::CoerceTo {
							from,
							..
						}) => val = from,
						Err(e) => return Err(e),
						Ok(v) => return Ok(v),
					}
				}
				Err(Error::CoerceTo {
					from: val,
					into: kind.to_string(),
				})
			}
		};
		// Check for any conversion errors
		match res {
			// There was a conversion error
			Err(Error::CoerceTo {
				from,
				..
			}) => Err(Error::CoerceTo {
				from,
				into: kind.to_string(),
			}),
			// There was a different error
			Err(e) => Err(e),
			// Everything converted ok
			Ok(v) => Ok(v),
		}
	}

	/// Try to coerce this value to an `i64`
	#[doc(hidden)]
	pub fn coerce_to_i64(self) -> Result<i64, Error> {
		match self {
			// Allow any int number
			Value::Number(Number::Int(v)) => Ok(v),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as i64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(v)) if v.is_integer() => match v.try_into() {
				// The Decimal can be represented as an i64
				Ok(v) => Ok(v),
				// The Decimal is out of bounds
				_ => Err(Error::CoerceTo {
					from: self,
					into: "i64".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "i64".into(),
			}),
		}
	}

	/// Try to coerce this value to an `u64`
	pub(crate) fn coerce_to_u64(self) -> Result<u64, Error> {
		match self {
			// Allow any int number
			Value::Number(Number::Int(v)) => Ok(v as u64),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as u64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(v)) if v.is_integer() => match v.try_into() {
				// The Decimal can be represented as an u64
				Ok(v) => Ok(v),
				// The Decimal is out of bounds
				_ => Err(Error::CoerceTo {
					from: self,
					into: "u64".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "u64".into(),
			}),
		}
	}

	/// Try to coerce this value to an `f64`
	pub(crate) fn coerce_to_f64(self) -> Result<f64, Error> {
		match self {
			// Allow any float number
			Value::Number(Number::Float(v)) => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => Ok(v as f64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(v)) => match v.try_into() {
				// The Decimal can be represented as a f64
				Ok(v) => Ok(v),
				// This Decimal loses precision
				_ => Err(Error::CoerceTo {
					from: self,
					into: "f64".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "f64".into(),
			}),
		}
	}

	/// Try to coerce this value to a `null`
	pub(crate) fn coerce_to_null(self) -> Result<Value, Error> {
		match self {
			// Allow any null value
			Value::Null => Ok(Value::Null),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "null".into(),
			}),
		}
	}

	/// Try to coerce this value to a `bool`
	pub(crate) fn coerce_to_bool(self) -> Result<bool, Error> {
		match self {
			// Allow any boolean value
			Value::Bool(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "bool".into(),
			}),
		}
	}

	/// Try to coerce this value to an integer `Number`
	pub(crate) fn coerce_to_int(self) -> Result<Number, Error> {
		match self {
			// Allow any int number
			Value::Number(v) if v.is_int() => Ok(v),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(Number::Int(v as i64)),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(v)) if v.is_integer() => match v.to_i64() {
				// The Decimal can be represented as an Int
				Some(v) => Ok(Number::Int(v)),
				// The Decimal is out of bounds
				_ => Err(Error::CoerceTo {
					from: self,
					into: "int".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "int".into(),
			}),
		}
	}

	/// Try to coerce this value to a float `Number`
	pub(crate) fn coerce_to_float(self) -> Result<Number, Error> {
		match self {
			// Allow any float number
			Value::Number(v) if v.is_float() => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => Ok(Number::Float(v as f64)),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(ref v)) => match v.to_f64() {
				// The Decimal can be represented as a Float
				Some(v) => Ok(Number::Float(v)),
				// This BigDecimal loses precision
				None => Err(Error::CoerceTo {
					from: self,
					into: "float".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "float".into(),
			}),
		}
	}

	/// Try to coerce this value to a decimal `Number`
	pub(crate) fn coerce_to_decimal(self) -> Result<Number, Error> {
		match self {
			// Allow any decimal number
			Value::Number(v) if v.is_decimal() => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => match Decimal::from_i64(v) {
				// The Int can be represented as a Decimal
				Some(v) => Ok(Number::Decimal(v)),
				// This Int does not convert to a Decimal
				None => Err(Error::CoerceTo {
					from: self,
					into: "decimal".into(),
				}),
			},
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) => match Decimal::from_f64(v) {
				// The Float can be represented as a Decimal
				Some(v) => Ok(Number::Decimal(v)),
				// This Float does not convert to a Decimal
				None => Err(Error::CoerceTo {
					from: self,
					into: "decimal".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "decimal".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Number`
	pub(crate) fn coerce_to_number(self) -> Result<Number, Error> {
		match self {
			// Allow any number
			Value::Number(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "number".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Regex`
	pub(crate) fn coerce_to_regex(self) -> Result<Regex, Error> {
		match self {
			// Allow any Regex value
			Value::Regex(v) => Ok(v),
			// Allow any string value
			Value::Strand(v) => Ok(v.as_str().parse()?),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "regex".into(),
			}),
		}
	}

	/// Try to coerce this value to a `String`
	pub(crate) fn coerce_to_string(self) -> Result<String, Error> {
		match self {
			// Allow any uuid value
			Value::Uuid(v) => Ok(v.to_raw()),
			// Allow any datetime value
			Value::Datetime(v) => Ok(v.to_raw()),
			// Allow any string value
			Value::Strand(v) => Ok(v.as_string()),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "string".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Strand`
	pub(crate) fn coerce_to_strand(self) -> Result<Strand, Error> {
		match self {
			// Allow any uuid value
			Value::Uuid(v) => Ok(v.to_raw().into()),
			// Allow any datetime value
			Value::Datetime(v) => Ok(v.to_raw().into()),
			// Allow any string value
			Value::Strand(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "string".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Uuid`
	pub(crate) fn coerce_to_uuid(self) -> Result<Uuid, Error> {
		match self {
			// Uuids are allowed
			Value::Uuid(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "uuid".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Datetime`
	pub(crate) fn coerce_to_datetime(self) -> Result<Datetime, Error> {
		match self {
			// Datetimes are allowed
			Value::Datetime(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "datetime".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Duration`
	pub(crate) fn coerce_to_duration(self) -> Result<Duration, Error> {
		match self {
			// Durations are allowed
			Value::Duration(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "duration".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Bytes`
	pub(crate) fn coerce_to_bytes(self) -> Result<Bytes, Error> {
		match self {
			// Bytes are allowed
			Value::Bytes(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "bytes".into(),
			}),
		}
	}

	/// Try to coerce this value to an `Object`
	pub(crate) fn coerce_to_object(self) -> Result<Object, Error> {
		match self {
			// Objects are allowed
			Value::Object(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "object".into(),
			}),
		}
	}

	/// Try to coerce this value to an `Array`
	pub(crate) fn coerce_to_array(self) -> Result<Array, Error> {
		match self {
			// Arrays are allowed
			Value::Array(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "array".into(),
			}),
		}
	}

	/// Try to coerce this value to an `Geometry` point
	pub(crate) fn coerce_to_point(self) -> Result<Geometry, Error> {
		match self {
			// Geometry points are allowed
			Value::Geometry(Geometry::Point(v)) => Ok(v.into()),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "point".into(),
			}),
		}
	}

	/// Try to coerce this value to a Record or `Thing`
	pub(crate) fn coerce_to_record(self) -> Result<Thing, Error> {
		match self {
			// Records are allowed
			Value::Thing(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "record".into(),
			}),
		}
	}

	/// Try to coerce this value to an `Geometry` type
	pub(crate) fn coerce_to_geometry(self) -> Result<Geometry, Error> {
		match self {
			// Geometries are allowed
			Value::Geometry(v) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "geometry".into(),
			}),
		}
	}

	/// Try to coerce this value to a Record of a certain type
	pub(crate) fn coerce_to_record_type(self, val: &[Table]) -> Result<Thing, Error> {
		match self {
			// Records are allowed if correct type
			Value::Thing(v) if self.is_record_type(val) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "record".into(),
			}),
		}
	}

	/// Try to coerce this value to a `Geometry` of a certain type
	pub(crate) fn coerce_to_geometry_type(self, val: &[String]) -> Result<Geometry, Error> {
		match self {
			// Geometries are allowed if correct type
			Value::Geometry(v) if self.is_geometry_type(val) => Ok(v),
			// Anything else raises an error
			_ => Err(Error::CoerceTo {
				from: self,
				into: "geometry".into(),
			}),
		}
	}

	/// Try to coerce this value to an `Array` of a certain type
	pub(crate) fn coerce_to_array_type(self, kind: &Kind) -> Result<Array, Error> {
		self.coerce_to_array()?
			.into_iter()
			.map(|value| value.coerce_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::CoerceTo {
					from,
					..
				} => Error::CoerceTo {
					from,
					into: format!("array<{kind}>"),
				},
				e => e,
			})
	}

	/// Try to coerce this value to an `Array` of a certain type, and length
	pub(crate) fn coerce_to_array_type_len(self, kind: &Kind, len: &u64) -> Result<Array, Error> {
		self.coerce_to_array()?
			.into_iter()
			.map(|value| value.coerce_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::CoerceTo {
					from,
					..
				} => Error::CoerceTo {
					from,
					into: format!("array<{kind}, {len}>"),
				},
				e => e,
			})
			.and_then(|v| match v.len() {
				v if v > *len as usize => Err(Error::LengthInvalid {
					kind: format!("array<{kind}, {len}>"),
					size: v,
				}),
				_ => Ok(v),
			})
	}

	/// Try to coerce this value to an `Array` of a certain type, unique values
	pub(crate) fn coerce_to_set_type(self, kind: &Kind) -> Result<Array, Error> {
		self.coerce_to_array()?
			.uniq()
			.into_iter()
			.map(|value| value.coerce_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::CoerceTo {
					from,
					..
				} => Error::CoerceTo {
					from,
					into: format!("set<{kind}>"),
				},
				e => e,
			})
	}

	/// Try to coerce this value to an `Array` of a certain type, unique values, and length
	pub(crate) fn coerce_to_set_type_len(self, kind: &Kind, len: &u64) -> Result<Array, Error> {
		self.coerce_to_array()?
			.uniq()
			.into_iter()
			.map(|value| value.coerce_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::CoerceTo {
					from,
					..
				} => Error::CoerceTo {
					from,
					into: format!("set<{kind}, {len}>"),
				},
				e => e,
			})
			.and_then(|v| match v.len() {
				v if v > *len as usize => Err(Error::LengthInvalid {
					kind: format!("set<{kind}, {len}>"),
					size: v,
				}),
				_ => Ok(v),
			})
	}

	// -----------------------------------
	// Advanced type conversion of values
	// -----------------------------------

	/// Try to convert this value to the specified `Kind`
	pub(crate) fn convert_to(self, kind: &Kind) -> Result<Value, Error> {
		// Attempt to convert to the desired type
		let res = match kind {
			Kind::Any => Ok(self),
			Kind::Null => self.convert_to_null(),
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
			Kind::Set(t, l) => match l {
				Some(l) => self.convert_to_set_type_len(t, l).map(Value::from),
				None => self.convert_to_set_type(t).map(Value::from),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.convert_to_array_type_len(t, l).map(Value::from),
				None => self.convert_to_array_type(t).map(Value::from),
			},
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
					into: kind.to_string(),
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
				into: kind.to_string(),
			}),
			// There was a different error
			Err(e) => Err(e),
			// Everything converted ok
			Ok(v) => Ok(v),
		}
	}

	/// Try to convert this value to a `null`
	pub(crate) fn convert_to_null(self) -> Result<Value, Error> {
		match self {
			// Allow any boolean value
			Value::Null => Ok(Value::Null),
			// Anything else raises an error
			_ => Err(Error::ConvertTo {
				from: self,
				into: "null".into(),
			}),
		}
	}

	/// Try to convert this value to a `bool`
	pub(crate) fn convert_to_bool(self) -> Result<bool, Error> {
		match self {
			// Allow any boolean value
			Value::Bool(v) => Ok(v),
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<bool>() {
				// The string can be represented as a Float
				Ok(v) => Ok(v),
				// This string is not a float
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
			Value::Number(Number::Decimal(v)) if v.is_integer() => match v.try_into() {
				// The Decimal can be represented as an Int
				Ok(v) => Ok(Number::Int(v)),
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
				// This string is not a float
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
			Value::Number(Number::Decimal(v)) => match v.try_into() {
				// The Decimal can be represented as a Float
				Ok(v) => Ok(Number::Float(v)),
				// The Decimal loses precision
				_ => Err(Error::ConvertTo {
					from: self,
					into: "float".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match v.parse::<f64>() {
				// The string can be represented as a Float
				Ok(v) => Ok(Number::Float(v)),
				// This string is not a float
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
			Value::Number(Number::Int(ref v)) => Ok(Number::Decimal(Decimal::from(*v))),
			// Attempt to convert an float number
			Value::Number(Number::Float(ref v)) => match Decimal::try_from(*v) {
				// The Float can be represented as a Decimal
				Ok(v) => Ok(Number::Decimal(v)),
				// This Float does not convert to a Decimal
				_ => Err(Error::ConvertTo {
					from: self,
					into: "decimal".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref v) => match Decimal::from_str(v) {
				// The string can be represented as a Decimal
				Ok(v) => Ok(Number::Decimal(v)),
				// This string is not a Decimal
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
				// This string is not a float
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
	#[doc(hidden)]
	pub fn convert_to_string(self) -> Result<String, Error> {
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
				// This string is not a uuid
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
				// This string is not a datetime
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
				// This string is not a duration
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
			// Strings can be converted to bytes
			Value::Strand(s) => Ok(Bytes(s.0.into_bytes())),
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
			Value::Strand(v) => Thing::try_from(v.as_str()).map_err(move |_| Error::ConvertTo {
				from: Value::Strand(v),
				into: "record".into(),
			}),
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

	/// Try to convert this value to ab `Array` of a certain type
	pub(crate) fn convert_to_array_type(self, kind: &Kind) -> Result<Array, Error> {
		self.convert_to_array()?
			.into_iter()
			.map(|value| value.convert_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::ConvertTo {
					from,
					..
				} => Error::ConvertTo {
					from,
					into: format!("array<{kind}>"),
				},
				e => e,
			})
	}

	/// Try to convert this value to ab `Array` of a certain type and length
	pub(crate) fn convert_to_array_type_len(self, kind: &Kind, len: &u64) -> Result<Array, Error> {
		self.convert_to_array()?
			.into_iter()
			.map(|value| value.convert_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::ConvertTo {
					from,
					..
				} => Error::ConvertTo {
					from,
					into: format!("array<{kind}, {len}>"),
				},
				e => e,
			})
			.and_then(|v| match v.len() {
				v if v > *len as usize => Err(Error::LengthInvalid {
					kind: format!("array<{kind}, {len}>"),
					size: v,
				}),
				_ => Ok(v),
			})
	}

	/// Try to convert this value to an `Array` of a certain type, unique values
	pub(crate) fn convert_to_set_type(self, kind: &Kind) -> Result<Array, Error> {
		self.convert_to_array()?
			.uniq()
			.into_iter()
			.map(|value| value.convert_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::ConvertTo {
					from,
					..
				} => Error::ConvertTo {
					from,
					into: format!("set<{kind}>"),
				},
				e => e,
			})
	}

	/// Try to convert this value to an `Array` of a certain type, unique values, and length
	pub(crate) fn convert_to_set_type_len(self, kind: &Kind, len: &u64) -> Result<Array, Error> {
		self.convert_to_array()?
			.uniq()
			.into_iter()
			.map(|value| value.convert_to(kind))
			.collect::<Result<Array, Error>>()
			.map_err(|e| match e {
				Error::ConvertTo {
					from,
					..
				} => Error::ConvertTo {
					from,
					into: format!("set<{kind}, {len}>"),
				},
				e => e,
			})
			.and_then(|v| match v.len() {
				v if v > *len as usize => Err(Error::LengthInvalid {
					kind: format!("set<{kind}, {len}>"),
					size: v,
				}),
				_ => Ok(v),
			})
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
			Value::Bytes(_) => true,
			Value::Uuid(_) => true,
			Value::Number(_) => true,
			Value::Strand(_) => true,
			Value::Duration(_) => true,
			Value::Datetime(_) => true,
			Value::Geometry(_) => true,
			Value::Array(v) => v.is_static(),
			Value::Object(v) => v.is_static(),
			Value::Expression(v) => v.is_static(),
			Value::Function(v) => v.is_static(),
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
			Value::Bool(v) => match other {
				Value::Bool(w) => v == w,
				_ => false,
			},
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
				Value::Strand(w) => v.to_raw().as_str().fuzzy_match(w.as_str()),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => v.as_str().fuzzy_match(w.as_str()),
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

	/// Compare this Value to another Value using natural numerical comparison
	pub fn natural_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::natural_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value lexicographically and using natural numerical comparison
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
			Value::Array(v) => write!(f, "{v}"),
			Value::Block(v) => write!(f, "{v}"),
			Value::Bool(v) => write!(f, "{v}"),
			Value::Bytes(v) => write!(f, "{v}"),
			Value::Cast(v) => write!(f, "{v}"),
			Value::Constant(v) => write!(f, "{v}"),
			Value::Datetime(v) => write!(f, "{v}"),
			Value::Duration(v) => write!(f, "{v}"),
			Value::Edges(v) => write!(f, "{v}"),
			Value::Expression(v) => write!(f, "{v}"),
			Value::Function(v) => write!(f, "{v}"),
			Value::Model(v) => write!(f, "{v}"),
			Value::Future(v) => write!(f, "{v}"),
			Value::Geometry(v) => write!(f, "{v}"),
			Value::Idiom(v) => write!(f, "{v}"),
			Value::Mock(v) => write!(f, "{v}"),
			Value::Number(v) => write!(f, "{v}"),
			Value::Object(v) => write!(f, "{v}"),
			Value::Param(v) => write!(f, "{v}"),
			Value::Range(v) => write!(f, "{v}"),
			Value::Regex(v) => write!(f, "{v}"),
			Value::Strand(v) => write!(f, "{v}"),
			Value::Query(v) => write!(f, "{v}"),
			Value::Subquery(v) => write!(f, "{v}"),
			Value::Table(v) => write!(f, "{v}"),
			Value::Thing(v) => write!(f, "{v}"),
			Value::Uuid(v) => write!(f, "{v}"),
		}
	}
}

impl InfoStructure for Value {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

impl Value {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Value::Block(v) => v.writeable(),
			Value::Idiom(v) => v.writeable(),
			Value::Array(v) => v.iter().any(Value::writeable),
			Value::Object(v) => v.iter().any(|(_, v)| v.writeable()),
			Value::Function(v) => {
				v.is_custom() || v.is_script() || v.args().iter().any(Value::writeable)
			}
			Value::Model(m) => m.args.iter().any(Value::writeable),
			Value::Subquery(v) => v.writeable(),
			Value::Expression(v) => v.writeable(),
			_ => false,
		}
	}
	/// Process this type returning a computed simple Value
	///
	/// Is used recursively.
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Prevent infinite recursion due to casting, expressions, etc.
		let opt = &opt.dive(1)?;

		match self {
			Value::Cast(v) => v.compute(stk, ctx, opt, doc).await,
			Value::Thing(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Block(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Range(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Param(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Idiom(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Array(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Object(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Future(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Constant(v) => v.compute(ctx, opt, doc).await,
			Value::Function(v) => v.compute(stk, ctx, opt, doc).await,
			Value::Model(v) => v.compute(stk, ctx, opt, doc).await,
			Value::Subquery(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Expression(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
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
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_add(w)?),
			(Self::Strand(v), Self::Strand(w)) => Self::Strand(v + w),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w + v),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v + w),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v + w),
			(v, w) => return Err(Error::TryAdd(v.to_raw_string(), w.to_raw_string())),
		})
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
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_sub(w)?),
			(Self::Datetime(v), Self::Datetime(w)) => Self::Duration(v - w),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w - v),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v - w),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v - w),
			(v, w) => return Err(Error::TrySub(v.to_raw_string(), w.to_raw_string())),
		})
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
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_mul(w)?),
			(v, w) => return Err(Error::TryMul(v.to_raw_string(), w.to_raw_string())),
		})
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
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_div(w)?),
			(v, w) => return Err(Error::TryDiv(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryRem<Rhs = Self> {
	type Output;
	fn try_rem(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryRem for Value {
	type Output = Self;
	fn try_rem(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_rem(w)?),
			(v, w) => return Err(Error::TryRem(v.to_raw_string(), w.to_raw_string())),
		})
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
		Ok(match (self, other) {
			(Value::Number(v), Value::Number(w)) => Self::Number(v.try_pow(w)?),
			(v, w) => return Err(Error::TryPow(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryNeg<Rhs = Self> {
	type Output;
	fn try_neg(self) -> Result<Self::Output, Error>;
}

impl TryNeg for Value {
	type Output = Self;
	fn try_neg(self) -> Result<Self, Error> {
		Ok(match self {
			Self::Number(n) => Self::Number(n.try_neg()?),
			v => return Err(Error::TryNeg(v.to_string())),
		})
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[test]
	fn check_none() {
		assert!(Value::None.is_none());
		assert!(!Value::Null.is_none());
		assert!(!Value::from(1).is_none());
	}

	#[test]
	fn check_null() {
		assert!(Value::Null.is_null());
		assert!(!Value::None.is_null());
		assert!(!Value::from(1).is_null());
	}

	#[test]
	fn check_true() {
		assert!(!Value::None.is_true());
		assert!(Value::Bool(true).is_true());
		assert!(!Value::Bool(false).is_true());
		assert!(!Value::from(1).is_true());
		assert!(!Value::from("something").is_true());
	}

	#[test]
	fn check_false() {
		assert!(!Value::None.is_false());
		assert!(!Value::Bool(true).is_false());
		assert!(Value::Bool(false).is_false());
		assert!(!Value::from(1).is_false());
		assert!(!Value::from("something").is_false());
	}

	#[test]
	fn convert_truthy() {
		assert!(!Value::None.is_truthy());
		assert!(!Value::Null.is_truthy());
		assert!(Value::Bool(true).is_truthy());
		assert!(!Value::Bool(false).is_truthy());
		assert!(!Value::from(0).is_truthy());
		assert!(Value::from(1).is_truthy());
		assert!(Value::from(-1).is_truthy());
		assert!(Value::from(1.1).is_truthy());
		assert!(Value::from(-1.1).is_truthy());
		assert!(Value::from("true").is_truthy());
		assert!(!Value::from("false").is_truthy());
		assert!(Value::from("falsey").is_truthy());
		assert!(Value::from("something").is_truthy());
		assert!(Value::from(Uuid::new()).is_truthy());
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
		assert_eq!(String::from("1.1f"), Value::from(1.1).as_string());
		assert_eq!(String::from("-1.1f"), Value::from(-1.1).as_string());
		assert_eq!(String::from("3"), Value::from("3").as_string());
		assert_eq!(String::from("true"), Value::from("true").as_string());
		assert_eq!(String::from("false"), Value::from("false").as_string());
		assert_eq!(String::from("something"), Value::from("something").as_string());
	}

	#[test]
	fn check_size() {
		assert!(64 >= std::mem::size_of::<Value>(), "size of value too big");
		assert_eq!(112, std::mem::size_of::<Error>());
		assert_eq!(112, std::mem::size_of::<Result<Value, Error>>());
		assert_eq!(24, std::mem::size_of::<crate::sql::number::Number>());
		assert_eq!(24, std::mem::size_of::<crate::sql::strand::Strand>());
		assert_eq!(16, std::mem::size_of::<crate::sql::duration::Duration>());
		assert_eq!(12, std::mem::size_of::<crate::sql::datetime::Datetime>());
		assert_eq!(24, std::mem::size_of::<crate::sql::array::Array>());
		assert_eq!(24, std::mem::size_of::<crate::sql::object::Object>());
		assert_eq!(48, std::mem::size_of::<crate::sql::geometry::Geometry>());
		assert_eq!(24, std::mem::size_of::<crate::sql::param::Param>());
		assert_eq!(24, std::mem::size_of::<crate::sql::idiom::Idiom>());
		assert_eq!(24, std::mem::size_of::<crate::sql::table::Table>());
		assert_eq!(56, std::mem::size_of::<crate::sql::thing::Thing>());
		assert_eq!(40, std::mem::size_of::<crate::sql::mock::Mock>());
		assert_eq!(32, std::mem::size_of::<crate::sql::regex::Regex>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::range::Range>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::edges::Edges>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::function::Function>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::subquery::Subquery>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::expression::Expression>>());
	}

	#[test]
	fn check_serialize() {
		let enc: Vec<u8> = Value::None.into();
		assert_eq!(2, enc.len());
		let enc: Vec<u8> = Value::Null.into();
		assert_eq!(2, enc.len());
		let enc: Vec<u8> = Value::Bool(true).into();
		assert_eq!(3, enc.len());
		let enc: Vec<u8> = Value::Bool(false).into();
		assert_eq!(3, enc.len());
		let enc: Vec<u8> = Value::from("test").into();
		assert_eq!(8, enc.len());
		let enc: Vec<u8> = Value::parse("{ hello: 'world' }").into();
		assert_eq!(19, enc.len());
		let enc: Vec<u8> = Value::parse("{ compact: true, schema: 0 }").into();
		assert_eq!(27, enc.len());
	}

	#[test]
	fn serialize_deserialize() {
		let val = Value::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: nulll }] } }",
		);
		let res = Value::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: nulll }] } }",
		);
		let enc: Vec<u8> = val.into();
		let dec: Value = enc.into();
		assert_eq!(res, dec);
	}
}
