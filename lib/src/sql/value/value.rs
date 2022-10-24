#![allow(clippy::derive_ord_xor_partial_ord)]

use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::array::{array, Array};
use crate::sql::common::commas;
use crate::sql::constant::{constant, Constant};
use crate::sql::datetime::{datetime, Datetime};
use crate::sql::duration::{duration, Duration};
use crate::sql::edges::{edges, Edges};
use crate::sql::error::IResult;
use crate::sql::expression::{expression, Expression};
use crate::sql::fmt::Fmt;
use crate::sql::function::{function, Function};
use crate::sql::geometry::{geometry, Geometry};
use crate::sql::id::Id;
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::kind::Kind;
use crate::sql::model::{model, Model};
use crate::sql::number::{number, Number};
use crate::sql::object::{object, Object};
use crate::sql::operation::Operation;
use crate::sql::param::{param, Param};
use crate::sql::part::Part;
use crate::sql::range::{range, Range};
use crate::sql::regex::{regex, Regex};
use crate::sql::serde::is_internal_serialization;
use crate::sql::strand::{strand, Strand};
use crate::sql::subquery::{subquery, Subquery};
use crate::sql::table::{table, Table};
use crate::sql::thing::{thing, Thing};
use crate::sql::uuid::{uuid as unique, Uuid};
use async_recursion::async_recursion;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use derive::Store;
use fuzzy_matcher::skim::SkimMatcherV2;
use fuzzy_matcher::FuzzyMatcher;
use geo::Point;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::multi::separated_list1;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::iter::FromIterator;
use std::ops;
use std::ops::Deref;
use std::str::FromStr;

static MATCHER: Lazy<SkimMatcherV2> = Lazy::new(|| SkimMatcherV2::default().ignore_case());

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Store)]
pub enum Value {
	None,
	Null,
	False,
	True,
	Number(Number),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Geometry(Geometry),
	// ---
	Param(Param),
	Idiom(Idiom),
	Table(Table),
	Thing(Thing),
	Model(Model),
	Regex(Regex),
	Range(Box<Range>),
	Edges(Box<Edges>),
	Constant(Constant),
	Function(Box<Function>),
	Subquery(Box<Subquery>),
	Expression(Box<Expression>),
}

impl Eq for Value {}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}

impl Default for Value {
	fn default() -> Value {
		Self::None
	}
}

impl From<bool> for Value {
	#[inline]
	fn from(v: bool) -> Self {
		match v {
			true => Self::True,
			false => Self::False,
		}
	}
}

impl From<Uuid> for Value {
	fn from(v: Uuid) -> Self {
		Self::Uuid(v)
	}
}

impl From<Param> for Value {
	fn from(v: Param) -> Self {
		Self::Param(v)
	}
}

impl From<Idiom> for Value {
	fn from(v: Idiom) -> Self {
		Self::Idiom(v)
	}
}

impl From<Model> for Value {
	fn from(v: Model) -> Self {
		Self::Model(v)
	}
}

impl From<Table> for Value {
	fn from(v: Table) -> Self {
		Self::Table(v)
	}
}

impl From<Thing> for Value {
	fn from(v: Thing) -> Self {
		Self::Thing(v)
	}
}

impl From<Regex> for Value {
	fn from(v: Regex) -> Self {
		Self::Regex(v)
	}
}

impl From<Array> for Value {
	fn from(v: Array) -> Self {
		Self::Array(v)
	}
}

impl From<Object> for Value {
	fn from(v: Object) -> Self {
		Self::Object(v)
	}
}

impl From<Number> for Value {
	fn from(v: Number) -> Self {
		Self::Number(v)
	}
}

impl From<Strand> for Value {
	fn from(v: Strand) -> Self {
		Self::Strand(v)
	}
}

impl From<Geometry> for Value {
	fn from(v: Geometry) -> Self {
		Self::Geometry(v)
	}
}

impl From<Datetime> for Value {
	fn from(v: Datetime) -> Self {
		Self::Datetime(v)
	}
}

impl From<Duration> for Value {
	fn from(v: Duration) -> Self {
		Self::Duration(v)
	}
}

impl From<Constant> for Value {
	fn from(v: Constant) -> Self {
		Self::Constant(v)
	}
}

impl From<Range> for Value {
	fn from(v: Range) -> Self {
		Self::Range(Box::new(v))
	}
}

impl From<Edges> for Value {
	fn from(v: Edges) -> Self {
		Self::Edges(Box::new(v))
	}
}

impl From<Function> for Value {
	fn from(v: Function) -> Self {
		Self::Function(Box::new(v))
	}
}

impl From<Subquery> for Value {
	fn from(v: Subquery) -> Self {
		Self::Subquery(Box::new(v))
	}
}

impl From<Expression> for Value {
	fn from(v: Expression) -> Self {
		Self::Expression(Box::new(v))
	}
}

impl From<i8> for Value {
	fn from(v: i8) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<i16> for Value {
	fn from(v: i16) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<i32> for Value {
	fn from(v: i32) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<i64> for Value {
	fn from(v: i64) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<isize> for Value {
	fn from(v: isize) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<u8> for Value {
	fn from(v: u8) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<u16> for Value {
	fn from(v: u16) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<u32> for Value {
	fn from(v: u32) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<u64> for Value {
	fn from(v: u64) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<usize> for Value {
	fn from(v: usize) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<f32> for Value {
	fn from(v: f32) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<f64> for Value {
	fn from(v: f64) -> Self {
		Self::Number(Number::from(v))
	}
}

impl From<BigDecimal> for Value {
	fn from(v: BigDecimal) -> Self {
		Self::Number(Number::from(v))
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
		Self::Datetime(Datetime::from(v))
	}
}

impl From<(f64, f64)> for Value {
	fn from(v: (f64, f64)) -> Self {
		Self::Geometry(Geometry::from(v))
	}
}

impl From<[f64; 2]> for Value {
	fn from(v: [f64; 2]) -> Self {
		Self::Geometry(Geometry::from(v))
	}
}

impl From<Point<f64>> for Value {
	fn from(v: Point<f64>) -> Self {
		Self::Geometry(Geometry::from(v))
	}
}

impl From<Operation> for Value {
	fn from(v: Operation) -> Self {
		Self::Object(Object::from(v))
	}
}

impl From<Vec<&str>> for Value {
	fn from(v: Vec<&str>) -> Self {
		Self::Array(Array::from(v))
	}
}

impl From<Vec<i32>> for Value {
	fn from(v: Vec<i32>) -> Self {
		Self::Array(Array::from(v))
	}
}

impl From<Vec<Self>> for Value {
	fn from(v: Vec<Self>) -> Self {
		Self::Array(Array::from(v))
	}
}

impl From<Vec<Number>> for Value {
	fn from(v: Vec<Number>) -> Self {
		Self::Array(Array::from(v))
	}
}

impl From<Vec<Operation>> for Value {
	fn from(v: Vec<Operation>) -> Self {
		Self::Array(Array::from(v))
	}
}

impl From<HashMap<String, Self>> for Value {
	fn from(v: HashMap<String, Self>) -> Self {
		Self::Object(Object::from(v))
	}
}

impl From<BTreeMap<String, Self>> for Value {
	fn from(v: BTreeMap<String, Self>) -> Self {
		Self::Object(Object::from(v))
	}
}

impl From<Option<Self>> for Value {
	fn from(v: Option<Self>) -> Self {
		v.unwrap_or(Self::None)
	}
}

impl From<Option<String>> for Value {
	fn from(v: Option<String>) -> Self {
		v.map(Self::from).unwrap_or(Self::None)
	}
}

impl From<Id> for Value {
	fn from(v: Id) -> Self {
		match v {
			Id::Number(v) => v.into(),
			Id::String(v) => Strand::from(v).into(),
			Id::Object(v) => v.into(),
			Id::Array(v) => v.into(),
		}
	}
}

impl FromIterator<Response> for Vec<Value> {
	fn from_iter<I: IntoIterator<Item = Response>>(iter: I) -> Self {
		iter.into_iter().map(Into::<Value>::into).collect::<Self>()
	}
}

impl Value {
	// -----------------------------------
	// Initial record value
	// -----------------------------------

	pub fn base() -> Self {
		Self::Object(Object::default())
	}

	// -----------------------------------
	// Builtin types
	// -----------------------------------

	pub fn ok(self) -> Result<Value, Error> {
		Ok(self)
	}

	pub fn output(self) -> Option<Value> {
		match self {
			Self::None => None,
			_ => Some(self),
		}
	}

	// -----------------------------------
	// Simple value detection
	// -----------------------------------

	pub fn is_none(&self) -> bool {
		matches!(self, Self::None | Self::Null)
	}

	pub fn is_null(&self) -> bool {
		matches!(self, Self::None | Self::Null)
	}

	pub fn is_some(&self) -> bool {
		!self.is_none()
	}

	pub fn is_true(&self) -> bool {
		match self {
			Self::True => true,
			Self::Strand(v) => v.eq_ignore_ascii_case("true"),
			_ => false,
		}
	}

	pub fn is_false(&self) -> bool {
		match self {
			Self::False => true,
			Self::Strand(v) => v.eq_ignore_ascii_case("false"),
			_ => false,
		}
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Self::True => true,
			Self::False => false,
			Self::Uuid(_) => true,
			Self::Thing(_) => true,
			Self::Geometry(_) => true,
			Self::Array(v) => !v.is_empty(),
			Self::Object(v) => !v.is_empty(),
			Self::Strand(v) => !v.is_empty() && !v.eq_ignore_ascii_case("false"),
			Self::Number(v) => v.is_truthy(),
			Self::Duration(v) => !v.is_zero(),
			Self::Datetime(v) => v.timestamp() > 0,
			_ => false,
		}
	}

	pub fn is_uuid(&self) -> bool {
		matches!(self, Self::Uuid(_))
	}

	pub fn is_thing(&self) -> bool {
		matches!(self, Self::Thing(_))
	}

	pub fn is_model(&self) -> bool {
		matches!(self, Self::Model(_))
	}

	pub fn is_range(&self) -> bool {
		matches!(self, Self::Range(_))
	}

	pub fn is_strand(&self) -> bool {
		matches!(self, Self::Strand(_))
	}

	pub fn is_array(&self) -> bool {
		matches!(self, Self::Array(_))
	}

	pub fn is_object(&self) -> bool {
		matches!(self, Self::Object(_))
	}

	pub fn is_int(&self) -> bool {
		matches!(self, Value::Number(Number::Int(_)))
	}

	pub fn is_float(&self) -> bool {
		matches!(self, Value::Number(Number::Float(_)))
	}

	pub fn is_decimal(&self) -> bool {
		matches!(self, Value::Number(Number::Decimal(_)))
	}

	pub fn is_integer(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_integer())
	}

	pub fn is_positive(&self) -> bool {
		matches!(self, Value::Number(v) if v.is_positive())
	}

	pub fn is_type_record(&self, types: &[Table]) -> bool {
		match self {
			Self::Thing(v) => types.iter().any(|tb| tb.0 == v.tb),
			_ => false,
		}
	}

	pub fn is_type_geometry(&self, types: &[String]) -> bool {
		match self {
			Self::Geometry(Geometry::Point(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "point"))
			}
			Self::Geometry(Geometry::Line(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "line"))
			}
			Self::Geometry(Geometry::Polygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "polygon"))
			}
			Self::Geometry(Geometry::MultiPoint(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipoint"))
			}
			Self::Geometry(Geometry::MultiLine(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multiline"))
			}
			Self::Geometry(Geometry::MultiPolygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipolygon"))
			}
			Self::Geometry(Geometry::Collection(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "collection"))
			}
			_ => false,
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	pub fn as_int(self) -> i64 {
		match self {
			Self::True => 1,
			Self::Strand(v) => v.parse::<i64>().unwrap_or(0),
			Self::Number(v) => v.as_int(),
			Self::Duration(v) => v.as_secs() as i64,
			Self::Datetime(v) => v.timestamp(),
			_ => 0,
		}
	}

	pub fn as_float(self) -> f64 {
		match self {
			Self::True => 1.0,
			Self::Strand(v) => v.parse::<f64>().unwrap_or(0.0),
			Self::Number(v) => v.as_float(),
			Self::Duration(v) => v.as_secs() as f64,
			Self::Datetime(v) => v.timestamp() as f64,
			_ => 0.0,
		}
	}

	pub fn as_decimal(self) -> BigDecimal {
		match self {
			Self::True => BigDecimal::from(1),
			Self::Number(v) => v.as_decimal(),
			Self::Strand(v) => BigDecimal::from_str(v.as_str()).unwrap_or_default(),
			Self::Duration(v) => v.as_secs().into(),
			Self::Datetime(v) => v.timestamp().into(),
			_ => BigDecimal::default(),
		}
	}

	pub fn as_number(self) -> Number {
		match self {
			Self::True => Number::from(1),
			Self::Number(v) => v,
			Self::Strand(v) => Number::from(v.as_str()),
			Self::Duration(v) => v.as_secs().into(),
			Self::Datetime(v) => v.timestamp().into(),
			_ => Number::default(),
		}
	}

	pub fn as_strand(self) -> Strand {
		match self {
			Self::Strand(v) => v,
			Self::Uuid(v) => v.to_raw().into(),
			Self::Datetime(v) => v.to_raw().into(),
			_ => self.to_string().into(),
		}
	}

	pub fn as_datetime(self) -> Datetime {
		match self {
			Self::Strand(v) => Datetime::from(v.as_str()),
			Self::Datetime(v) => v,
			_ => Datetime::default(),
		}
	}

	pub fn as_duration(self) -> Duration {
		match self {
			Self::Strand(v) => Duration::from(v.as_str()),
			Self::Duration(v) => v,
			_ => Duration::default(),
		}
	}

	pub fn as_string(self) -> String {
		match self {
			Self::Strand(v) => v.as_string(),
			_ => self.to_string(),
		}
	}

	pub fn as_usize(self) -> usize {
		match self {
			Value::Number(v) => v.as_usize(),
			_ => 0,
		}
	}

	// -----------------------------------
	// Expensive conversion of value
	// -----------------------------------

	pub fn to_number(&self) -> Number {
		match self {
			Self::True => Number::from(1),
			Self::Number(v) => v.clone(),
			Self::Strand(v) => Number::from(v.as_str()),
			Self::Duration(v) => v.as_secs().into(),
			Self::Datetime(v) => v.timestamp().into(),
			_ => Number::default(),
		}
	}

	pub fn to_strand(&self) -> Strand {
		match self {
			Self::Strand(v) => v.clone(),
			Self::Uuid(v) => v.to_raw().into(),
			Self::Datetime(v) => v.to_raw().into(),
			_ => self.to_string().into(),
		}
	}

	pub fn to_datetime(&self) -> Datetime {
		match self {
			Self::Strand(v) => Datetime::from(v.as_str()),
			Self::Datetime(v) => v.clone(),
			_ => Datetime::default(),
		}
	}

	pub fn to_duration(&self) -> Duration {
		match self {
			Self::Strand(v) => Duration::from(v.as_str()),
			Self::Duration(v) => v.clone(),
			_ => Duration::default(),
		}
	}

	pub fn to_idiom(&self) -> Idiom {
		match self {
			Self::Param(v) => v.simplify(),
			Self::Idiom(v) => v.simplify(),
			Self::Strand(v) => v.0.to_string().into(),
			Self::Datetime(v) => v.0.to_string().into(),
			Self::Function(v) => match v.as_ref() {
				Function::Future(_) => "fn::future".to_string().into(),
				Function::Script(_, _) => "fn::script".to_string().into(),
				Function::Normal(f, _) => f.to_string().into(),
				Function::Cast(_, v) => v.to_idiom(),
			},
			_ => self.to_string().into(),
		}
	}

	pub fn to_operations(&self) -> Result<Vec<Operation>, Error> {
		match self {
			Self::Array(v) => v
				.iter()
				.map(|v| match v {
					Self::Object(v) => v.to_operation(),
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

	pub fn make_bool(self) -> Value {
		match self {
			Self::True | Self::False => self,
			_ => self.is_truthy().into(),
		}
	}

	pub fn make_int(self) -> Value {
		match self {
			Self::Number(Number::Int(_)) => self,
			_ => self.as_int().into(),
		}
	}

	pub fn make_float(self) -> Value {
		match self {
			Self::Number(Number::Float(_)) => self,
			_ => self.as_float().into(),
		}
	}

	pub fn make_decimal(self) -> Value {
		match self {
			Self::Number(Number::Decimal(_)) => self,
			_ => self.as_decimal().into(),
		}
	}

	pub fn make_number(self) -> Value {
		match self {
			Self::Number(_) => self,
			_ => self.as_number().into(),
		}
	}

	pub fn make_strand(self) -> Value {
		match self {
			Self::Strand(_) => self,
			_ => self.as_strand().into(),
		}
	}

	pub fn make_datetime(self) -> Value {
		match self {
			Self::Datetime(_) => self,
			_ => self.as_datetime().into(),
		}
	}

	pub fn make_duration(self) -> Value {
		match self {
			Self::Duration(_) => self,
			_ => self.as_duration().into(),
		}
	}

	pub fn make_table(self) -> Value {
		match self {
			Self::Table(_) => self,
			Self::Strand(v) => Self::Table(Table(v.0)),
			_ => Self::Table(Table(self.as_strand().0)),
		}
	}

	pub fn make_table_or_thing(self) -> Value {
		match self {
			Self::Table(_) => self,
			Self::Thing(_) => self,
			Self::Strand(v) => Self::Table(Table(v.0)),
			_ => Self::Table(Table(self.as_strand().0)),
		}
	}

	pub fn convert_to(self, kind: &Kind) -> Value {
		match kind {
			Kind::Any => self,
			Kind::Bool => self.make_bool(),
			Kind::Int => self.make_int(),
			Kind::Float => self.make_float(),
			Kind::Decimal => self.make_decimal(),
			Kind::Number => self.make_number(),
			Kind::String => self.make_strand(),
			Kind::Datetime => self.make_datetime(),
			Kind::Duration => self.make_duration(),
			Kind::Array => match self {
				Self::Array(_) => self,
				_ => Self::None,
			},
			Kind::Object => match self {
				Self::Object(_) => self,
				_ => Self::None,
			},
			Kind::Record(t) => match self.is_type_record(t) {
				true => self,
				_ => Self::None,
			},
			Kind::Geometry(t) => match self.is_type_geometry(t) {
				true => self,
				_ => Self::None,
			},
		}
	}

	// -----------------------------------
	// Record ID extraction
	// -----------------------------------

	/// Fetch the record id if there is one
	pub fn record(self) -> Option<Thing> {
		match self {
			Self::Object(mut v) => match v.remove("id") {
				Some(Self::Thing(v)) => Some(v),
				_ => None,
			},
			Self::Array(mut v) => match v.len() {
				1 => v.remove(0).record(),
				_ => None,
			},
			Self::Thing(v) => Some(v),
			_ => None,
		}
	}

	// -----------------------------------
	// JSON Path conversion
	// -----------------------------------

	pub fn jsonpath(&self) -> Idiom {
		self.to_strand()
			.as_str()
			.trim_start_matches('/')
			.split(&['.', '/'][..])
			.map(Part::from)
			.collect::<Vec<Part>>()
			.into()
	}

	// -----------------------------------
	// Value operations
	// -----------------------------------

	pub fn equal(&self, other: &Value) -> bool {
		match self {
			Self::None => other.is_none(),
			Self::Null => other.is_null(),
			Self::True => other.is_true(),
			Self::False => other.is_false(),
			Self::Thing(v) => match other {
				Self::Thing(w) => v == w,
				Self::Regex(w) => match w.regex() {
					Some(ref r) => r.is_match(v.to_string().as_str()),
					None => false,
				},
				_ => false,
			},
			Self::Regex(v) => match other {
				Self::Regex(w) => v == w,
				Self::Number(w) => match v.regex() {
					Some(ref r) => r.is_match(w.to_string().as_str()),
					None => false,
				},
				Self::Strand(w) => match v.regex() {
					Some(ref r) => r.is_match(w.as_str()),
					None => false,
				},
				_ => false,
			},
			Self::Uuid(v) => match other {
				Self::Uuid(w) => v == w,
				_ => false,
			},
			Self::Array(v) => match other {
				Self::Array(w) => v == w,
				_ => false,
			},
			Self::Object(v) => match other {
				Self::Object(w) => v == w,
				_ => false,
			},
			Self::Strand(v) => match other {
				Self::Strand(w) => v == w,
				Self::Regex(w) => match w.regex() {
					Some(ref r) => r.is_match(v.as_str()),
					None => false,
				},
				_ => v == &other.to_strand(),
			},
			Self::Number(v) => match other {
				Self::Number(w) => v == w,
				Self::Strand(_) => v == &other.to_number(),
				Self::Regex(w) => match w.regex() {
					Some(ref r) => r.is_match(v.to_string().as_str()),
					None => false,
				},
				_ => false,
			},
			Self::Geometry(v) => match other {
				Self::Geometry(w) => v == w,
				_ => false,
			},
			Self::Duration(v) => match other {
				Self::Duration(w) => v == w,
				Self::Strand(_) => v == &other.to_duration(),
				_ => false,
			},
			Self::Datetime(v) => match other {
				Self::Datetime(w) => v == w,
				Self::Strand(_) => v == &other.to_datetime(),
				_ => false,
			},
			_ => unreachable!(),
		}
	}

	pub fn all_equal(&self, other: &Value) -> bool {
		match self {
			Self::Array(v) => v.iter().all(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	pub fn any_equal(&self, other: &Value) -> bool {
		match self {
			Self::Array(v) => v.iter().any(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	pub fn fuzzy(&self, other: &Value) -> bool {
		match self {
			Self::Strand(v) => match other {
				Self::Strand(w) => MATCHER.fuzzy_match(v.as_str(), w.as_str()).is_some(),
				_ => MATCHER.fuzzy_match(v.as_str(), other.to_string().as_str()).is_some(),
			},
			_ => self.equal(other),
		}
	}

	pub fn all_fuzzy(&self, other: &Value) -> bool {
		match self {
			Self::Array(v) => v.iter().all(|v| v.fuzzy(other)),
			_ => self.fuzzy(other),
		}
	}

	pub fn any_fuzzy(&self, other: &Value) -> bool {
		match self {
			Self::Array(v) => v.iter().any(|v| v.fuzzy(other)),
			_ => self.fuzzy(other),
		}
	}

	pub fn contains(&self, other: &Value) -> bool {
		match self {
			Self::Array(v) => v.iter().any(|v| v.equal(other)),
			Self::Strand(v) => match other {
				Self::Strand(w) => v.contains(w.as_str()),
				_ => v.contains(&other.to_string().as_str()),
			},
			Self::Geometry(v) => match other {
				Self::Geometry(w) => v.contains(w),
				_ => false,
			},
			_ => false,
		}
	}

	pub fn contains_all(&self, other: &Value) -> bool {
		match other {
			Self::Array(v) => v.iter().all(|v| match self {
				Self::Array(w) => w.iter().any(|w| v.equal(w)),
				Self::Geometry(_) => self.contains(v),
				_ => false,
			}),
			_ => false,
		}
	}

	pub fn contains_any(&self, other: &Value) -> bool {
		match other {
			Self::Array(v) => v.iter().any(|v| match self {
				Self::Array(w) => w.iter().any(|w| v.equal(w)),
				Self::Geometry(_) => self.contains(v),
				_ => false,
			}),
			_ => false,
		}
	}

	pub fn intersects(&self, other: &Value) -> bool {
		match self {
			Self::Geometry(v) => match other {
				Self::Geometry(w) => v.intersects(w),
				_ => false,
			},
			_ => false,
		}
	}

	// -----------------------------------
	// Sorting operations
	// -----------------------------------

	pub fn lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Self::Strand(a), Self::Strand(b)) => Some(lexical_sort::lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	pub fn natural_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Self::Strand(a), Self::Strand(b)) => Some(lexical_sort::natural_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	pub fn natural_lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Self::Strand(a), Self::Strand(b)) => Some(lexical_sort::natural_lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}
}

impl Display for Value {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::None => f.write_str("NONE"),
			Self::Null => f.write_str("NULL"),
			Self::True => f.write_str("true"),
			Self::False => f.write_str("false"),
			Self::Number(v) => Display::fmt(v, f),
			Self::Strand(v) => Display::fmt(v, f),
			Self::Duration(v) => Display::fmt(v, f),
			Self::Datetime(v) => Display::fmt(v, f),
			Self::Uuid(v) => Display::fmt(v, f),
			Self::Array(v) => Display::fmt(v, f),
			Self::Object(v) => Display::fmt(v, f),
			Self::Geometry(v) => Display::fmt(v, f),
			Self::Param(v) => Display::fmt(v, f),
			Self::Idiom(v) => Display::fmt(v, f),
			Self::Table(v) => Display::fmt(v, f),
			Self::Thing(v) => Display::fmt(v, f),
			Self::Model(v) => Display::fmt(v, f),
			Self::Regex(v) => Display::fmt(v, f),
			Self::Range(v) => Display::fmt(v, f),
			Self::Edges(v) => Display::fmt(v, f),
			Self::Constant(v) => Display::fmt(v, f),
			Self::Function(v) => Display::fmt(v, f),
			Self::Subquery(v) => Display::fmt(v, f),
			Self::Expression(v) => Display::fmt(v, f),
		}
	}
}

impl Value {
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Array(v) => v.iter().any(Self::writeable),
			Self::Object(v) => v.iter().any(|(_, v)| v.writeable()),
			Self::Function(v) => v.args().iter().any(Self::writeable),
			Self::Subquery(v) => v.writeable(),
			Self::Expression(v) => v.l.writeable() || v.r.writeable(),
			_ => false,
		}
	}

	#[cfg_attr(feature = "parallel", async_recursion)]
	#[cfg_attr(not(feature = "parallel"), async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion Value>,
	) -> Result<Value, Error> {
		match self {
			Self::None => Ok(Self::None),
			Self::Null => Ok(Self::Null),
			Self::True => Ok(Self::True),
			Self::False => Ok(Self::False),
			Self::Thing(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Param(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Idiom(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Array(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Object(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Constant(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Function(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Subquery(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Expression(v) => v.compute(ctx, opt, txn, doc).await,
			_ => Ok(self.to_owned()),
		}
	}
}

impl Serialize for Value {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			match self {
				Self::None => s.serialize_unit_variant("Value", 0, "None"),
				Self::Null => s.serialize_unit_variant("Value", 1, "Null"),
				Self::False => s.serialize_unit_variant("Value", 2, "False"),
				Self::True => s.serialize_unit_variant("Value", 3, "True"),
				Self::Number(v) => s.serialize_newtype_variant("Value", 4, "Number", v),
				Self::Strand(v) => s.serialize_newtype_variant("Value", 5, "Strand", v),
				Self::Duration(v) => s.serialize_newtype_variant("Value", 6, "Duration", v),
				Self::Datetime(v) => s.serialize_newtype_variant("Value", 7, "Datetime", v),
				Self::Uuid(v) => s.serialize_newtype_variant("Value", 8, "Uuid", v),
				Self::Array(v) => s.serialize_newtype_variant("Value", 9, "Array", v),
				Self::Object(v) => s.serialize_newtype_variant("Value", 10, "Object", v),
				Self::Geometry(v) => s.serialize_newtype_variant("Value", 11, "Geometry", v),
				Self::Param(v) => s.serialize_newtype_variant("Value", 12, "Param", v),
				Self::Idiom(v) => s.serialize_newtype_variant("Value", 13, "Idiom", v),
				Self::Table(v) => s.serialize_newtype_variant("Value", 14, "Table", v),
				Self::Thing(v) => s.serialize_newtype_variant("Value", 15, "Thing", v),
				Self::Model(v) => s.serialize_newtype_variant("Value", 16, "Model", v),
				Self::Regex(v) => s.serialize_newtype_variant("Value", 17, "Regex", v),
				Self::Range(v) => s.serialize_newtype_variant("Value", 18, "Range", v),
				Self::Edges(v) => s.serialize_newtype_variant("Value", 19, "Edges", v),
				Self::Constant(v) => s.serialize_newtype_variant("Value", 20, "Constant", v),
				Self::Function(v) => s.serialize_newtype_variant("Value", 21, "Function", v),
				Self::Subquery(v) => s.serialize_newtype_variant("Value", 22, "Subquery", v),
				Self::Expression(v) => s.serialize_newtype_variant("Value", 23, "Expression", v),
			}
		} else {
			match self {
				Self::None => s.serialize_none(),
				Self::Null => s.serialize_none(),
				Self::True => s.serialize_bool(true),
				Self::False => s.serialize_bool(false),
				Self::Thing(v) => s.serialize_some(v),
				Self::Uuid(v) => s.serialize_some(v),
				Self::Array(v) => s.serialize_some(v),
				Self::Object(v) => s.serialize_some(v),
				Self::Number(v) => s.serialize_some(v),
				Self::Strand(v) => s.serialize_some(v),
				Self::Geometry(v) => s.serialize_some(v),
				Self::Duration(v) => s.serialize_some(v),
				Self::Datetime(v) => s.serialize_some(v),
				Self::Constant(v) => s.serialize_some(v),
				_ => s.serialize_none(),
			}
		}
	}
}

impl ops::Add for Value {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v + w),
			(Self::Strand(v), Self::Strand(w)) => Self::Strand(v + w),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w + v),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v + w),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v + w),
			(v, w) => Self::from(v.as_number() + w.as_number()),
		}
	}
}

impl ops::Sub for Value {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v - w),
			(Self::Datetime(v), Self::Datetime(w)) => Self::Duration(v - w),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w - v),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v - w),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v - w),
			(v, w) => Self::from(v.as_number() - w.as_number()),
		}
	}
}

impl ops::Mul for Value {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v * w),
			(v, w) => Self::from(v.as_number() * w.as_number()),
		}
	}
}

impl ops::Div for Value {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		match (self.as_number(), other.as_number()) {
			(_, w) if w == Number::Int(0) => Self::None,
			(v, w) => Self::Number(v / w),
		}
	}
}

pub fn value(i: &str) -> IResult<&str, Value> {
	alt((double, single))(i)
}

pub fn double(i: &str) -> IResult<&str, Value> {
	map(expression, Value::from)(i)
}

pub fn single(i: &str) -> IResult<&str, Value> {
	alt((
		alt((
			map(tag_no_case("NONE"), |_| Value::None),
			map(tag_no_case("NULL"), |_| Value::Null),
			map(tag_no_case("true"), |_| Value::True),
			map(tag_no_case("false"), |_| Value::False),
		)),
		alt((
			map(subquery, Value::from),
			map(function, Value::from),
			map(constant, Value::from),
			map(datetime, Value::from),
			map(duration, Value::from),
			map(geometry, Value::from),
			map(unique, Value::from),
			map(number, Value::from),
			map(object, Value::from),
			map(array, Value::from),
			map(param, Value::from),
			map(regex, Value::from),
			map(model, Value::from),
			map(idiom, Value::from),
			map(thing, Value::from),
			map(strand, Value::from),
		)),
	))(i)
}

pub fn select(i: &str) -> IResult<&str, Value> {
	alt((
		alt((
			map(tag_no_case("NONE"), |_| Value::None),
			map(tag_no_case("NULL"), |_| Value::Null),
			map(tag_no_case("true"), |_| Value::True),
			map(tag_no_case("false"), |_| Value::False),
		)),
		alt((
			map(expression, Value::from),
			map(subquery, Value::from),
			map(function, Value::from),
			map(constant, Value::from),
			map(datetime, Value::from),
			map(duration, Value::from),
			map(geometry, Value::from),
			map(unique, Value::from),
			map(number, Value::from),
			map(object, Value::from),
			map(array, Value::from),
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

pub fn what(i: &str) -> IResult<&str, Value> {
	alt((
		map(subquery, Value::from),
		map(function, Value::from),
		map(constant, Value::from),
		map(param, Value::from),
		map(model, Value::from),
		map(edges, Value::from),
		map(range, Value::from),
		map(thing, Value::from),
		map(table, Value::from),
	))(i)
}

pub fn json(i: &str) -> IResult<&str, Value> {
	alt((
		map(tag_no_case("NULL"), |_| Value::Null),
		map(tag_no_case("true"), |_| Value::True),
		map(tag_no_case("false"), |_| Value::False),
		map(datetime, Value::from),
		map(duration, Value::from),
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
		assert_eq!(true, Value::Null.is_none());
		assert_eq!(false, Value::from(1).is_none());
	}

	#[test]
	fn check_null() {
		assert_eq!(true, Value::None.is_null());
		assert_eq!(true, Value::Null.is_null());
		assert_eq!(false, Value::from(1).is_null());
	}

	#[test]
	fn check_true() {
		assert_eq!(false, Value::None.is_true());
		assert_eq!(true, Value::True.is_true());
		assert_eq!(false, Value::False.is_true());
		assert_eq!(false, Value::from(1).is_true());
		assert_eq!(true, Value::from("true").is_true());
		assert_eq!(true, Value::from("TrUe").is_true());
		assert_eq!(false, Value::from("false").is_true());
		assert_eq!(false, Value::from("something").is_true());
	}

	#[test]
	fn check_false() {
		assert_eq!(false, Value::None.is_false());
		assert_eq!(false, Value::True.is_false());
		assert_eq!(true, Value::False.is_false());
		assert_eq!(false, Value::from(1).is_false());
		assert_eq!(false, Value::from("true").is_false());
		assert_eq!(true, Value::from("false").is_false());
		assert_eq!(true, Value::from("FaLsE").is_false());
		assert_eq!(false, Value::from("something").is_false());
	}

	#[test]
	fn convert_bool() {
		assert_eq!(false, Value::None.is_truthy());
		assert_eq!(false, Value::Null.is_truthy());
		assert_eq!(true, Value::True.is_truthy());
		assert_eq!(false, Value::False.is_truthy());
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
	fn convert_int() {
		assert_eq!(0, Value::None.as_int());
		assert_eq!(0, Value::Null.as_int());
		assert_eq!(1, Value::True.as_int());
		assert_eq!(0, Value::False.as_int());
		assert_eq!(0, Value::from(0).as_int());
		assert_eq!(1, Value::from(1).as_int());
		assert_eq!(-1, Value::from(-1).as_int());
		assert_eq!(1, Value::from(1.1).as_int());
		assert_eq!(-1, Value::from(-1.1).as_int());
		assert_eq!(3, Value::from("3").as_int());
		assert_eq!(0, Value::from("true").as_int());
		assert_eq!(0, Value::from("false").as_int());
		assert_eq!(0, Value::from("something").as_int());
	}

	#[test]
	fn convert_float() {
		assert_eq!(0.0, Value::None.as_float());
		assert_eq!(0.0, Value::Null.as_float());
		assert_eq!(1.0, Value::True.as_float());
		assert_eq!(0.0, Value::False.as_float());
		assert_eq!(0.0, Value::from(0).as_float());
		assert_eq!(1.0, Value::from(1).as_float());
		assert_eq!(-1.0, Value::from(-1).as_float());
		assert_eq!(1.1, Value::from(1.1).as_float());
		assert_eq!(-1.1, Value::from(-1.1).as_float());
		assert_eq!(3.0, Value::from("3").as_float());
		assert_eq!(0.0, Value::from("true").as_float());
		assert_eq!(0.0, Value::from("false").as_float());
		assert_eq!(0.0, Value::from("something").as_float());
	}

	#[test]
	fn convert_number() {
		assert_eq!(Number::from(0), Value::None.as_number());
		assert_eq!(Number::from(0), Value::Null.as_number());
		assert_eq!(Number::from(1), Value::True.as_number());
		assert_eq!(Number::from(0), Value::False.as_number());
		assert_eq!(Number::from(0), Value::from(0).as_number());
		assert_eq!(Number::from(1), Value::from(1).as_number());
		assert_eq!(Number::from(-1), Value::from(-1).as_number());
		assert_eq!(Number::from(1.1), Value::from(1.1).as_number());
		assert_eq!(Number::from(-1.1), Value::from(-1.1).as_number());
		assert_eq!(Number::from(3), Value::from("3").as_number());
		assert_eq!(Number::from(0), Value::from("true").as_number());
		assert_eq!(Number::from(0), Value::from("false").as_number());
		assert_eq!(Number::from(0), Value::from("something").as_number());
	}

	#[test]
	fn convert_strand() {
		assert_eq!(Strand::from("NONE"), Value::None.as_strand());
		assert_eq!(Strand::from("NULL"), Value::Null.as_strand());
		assert_eq!(Strand::from("true"), Value::True.as_strand());
		assert_eq!(Strand::from("false"), Value::False.as_strand());
		assert_eq!(Strand::from("0"), Value::from(0).as_strand());
		assert_eq!(Strand::from("1"), Value::from(1).as_strand());
		assert_eq!(Strand::from("-1"), Value::from(-1).as_strand());
		assert_eq!(Strand::from("1.1"), Value::from(1.1).as_strand());
		assert_eq!(Strand::from("-1.1"), Value::from(-1.1).as_strand());
		assert_eq!(Strand::from("3"), Value::from("3").as_strand());
		assert_eq!(Strand::from("true"), Value::from("true").as_strand());
		assert_eq!(Strand::from("false"), Value::from("false").as_strand());
		assert_eq!(Strand::from("something"), Value::from("something").as_strand());
	}

	#[test]
	fn convert_string() {
		assert_eq!(String::from("NONE"), Value::None.as_string());
		assert_eq!(String::from("NULL"), Value::Null.as_string());
		assert_eq!(String::from("true"), Value::True.as_string());
		assert_eq!(String::from("false"), Value::False.as_string());
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
		assert_eq!(112, std::mem::size_of::<Result<Value, Error>>());
		assert_eq!(48, std::mem::size_of::<crate::sql::number::Number>());
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
		assert_eq!(24, std::mem::size_of::<crate::sql::regex::Regex>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::range::Range>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::edges::Edges>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::function::Function>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::subquery::Subquery>>());
		assert_eq!(8, std::mem::size_of::<Box<crate::sql::expression::Expression>>());
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
