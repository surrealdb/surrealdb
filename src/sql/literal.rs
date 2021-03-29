use crate::ctx::Parent;
use crate::dbs;
use crate::dbs::Executor;
use crate::doc::Document;
use crate::err::Error;
use crate::fnc;
use crate::sql::array::{array, Array};
use crate::sql::common::commas;
use crate::sql::datetime::{datetime, datetime_raw, Datetime};
use crate::sql::duration::{duration, duration_raw, Duration};
use crate::sql::function::{function, Function};
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::model::{model, Model};
use crate::sql::number::{number, Number};
use crate::sql::object::{object, Object};
use crate::sql::param::{param, Param};
use crate::sql::point::{point, Point};
use crate::sql::polygon::{polygon, Polygon};
use crate::sql::regex::{regex, Regex};
use crate::sql::strand::{strand, Strand};
use crate::sql::subquery::{subquery, Subquery};
use crate::sql::table::{table, Table};
use crate::sql::thing::{thing, Thing};
use dec::prelude::ToPrimitive;
use dec::Decimal;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::combinator::rest;
use nom::multi::separated_list1;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;

const NAME: &'static str = "Literal";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Literals(pub Vec<Literal>);

impl fmt::Display for Literals {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

impl IntoIterator for Literals {
	type Item = Literal;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

pub fn literals(i: &str) -> IResult<&str, Literals> {
	let (i, v) = separated_list1(commas, literal)(i)?;
	Ok((i, Literals(v)))
}

pub fn whats(i: &str) -> IResult<&str, Literals> {
	let (i, v) = separated_list1(commas, what)(i)?;
	Ok((i, Literals(v)))
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize)]
pub enum Literal {
	None,
	Void,
	Null,
	False,
	True,
	Int(i64),
	Float(f64),
	Param(Param),
	Idiom(Idiom),
	Table(Table),
	Thing(Thing),
	Model(Model),
	Regex(Regex),
	Point(Point),
	Array(Array),
	Object(Object),
	Number(Number),
	Strand(Strand),
	Polygon(Polygon),
	Duration(Duration),
	Datetime(Datetime),
	Function(Function),
	Subquery(Subquery),
}

impl Eq for Literal {}

impl Default for Literal {
	fn default() -> Literal {
		Literal::None
	}
}

impl From<bool> for Literal {
	fn from(v: bool) -> Self {
		match v {
			true => Literal::True,
			false => Literal::False,
		}
	}
}

impl From<i8> for Literal {
	fn from(v: i8) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<i16> for Literal {
	fn from(v: i16) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<i32> for Literal {
	fn from(v: i32) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<i64> for Literal {
	fn from(v: i64) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<u8> for Literal {
	fn from(v: u8) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<u16> for Literal {
	fn from(v: u16) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<u32> for Literal {
	fn from(v: u32) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<u64> for Literal {
	fn from(v: u64) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<f32> for Literal {
	fn from(v: f32) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<f64> for Literal {
	fn from(v: f64) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<Number> for Literal {
	fn from(v: Number) -> Self {
		Literal::Number(v)
	}
}

impl From<Decimal> for Literal {
	fn from(v: Decimal) -> Self {
		Literal::Number(Number::from(v))
	}
}

impl From<Strand> for Literal {
	fn from(v: Strand) -> Self {
		Literal::Strand(v)
	}
}

impl From<String> for Literal {
	fn from(v: String) -> Self {
		Literal::Strand(Strand::from(v))
	}
}

impl<'a> From<&'a str> for Literal {
	fn from(v: &str) -> Self {
		Literal::Strand(Strand::from(v))
	}
}

impl Literal {
	pub fn is_none(&self) -> bool {
		match self {
			Literal::None => true,
			Literal::Void => true,
			Literal::Null => true,
			_ => false,
		}
	}

	pub fn is_void(&self) -> bool {
		match self {
			Literal::None => true,
			Literal::Void => true,
			_ => false,
		}
	}

	pub fn is_null(&self) -> bool {
		match self {
			Literal::None => true,
			Literal::Null => true,
			_ => false,
		}
	}

	pub fn is_true(&self) -> bool {
		match self {
			Literal::True => true,
			Literal::Strand(ref v) => v.value.to_ascii_lowercase() == "true",
			_ => false,
		}
	}

	pub fn is_false(&self) -> bool {
		match self {
			Literal::False => true,
			Literal::Strand(ref v) => v.value.to_ascii_lowercase() == "false",
			_ => false,
		}
	}

	pub fn as_bool(&self) -> bool {
		match self {
			Literal::True => true,
			Literal::False => false,
			Literal::Int(v) => v > &0,
			Literal::Float(v) => v > &0.0,
			Literal::Thing(_) => true,
			Literal::Point(_) => true,
			Literal::Polygon(_) => true,
			Literal::Array(ref v) => v.value.len() > 0,
			Literal::Object(ref v) => v.value.len() > 0,
			Literal::Strand(ref v) => v.value.to_ascii_lowercase() != "false",
			Literal::Number(ref v) => v.value > Decimal::new(0, 0),
			Literal::Duration(ref v) => v.value.as_nanos() > 0,
			Literal::Datetime(ref v) => v.value.timestamp() > 0,
			_ => false,
		}
	}

	pub fn as_int(&self) -> i64 {
		match self {
			Literal::True => 1,
			Literal::Int(ref v) => v.clone(),
			Literal::Float(ref v) => *v as i64,
			Literal::Strand(ref v) => v.value.parse::<i64>().unwrap_or(0),
			Literal::Number(ref v) => v.value.to_i64().unwrap_or(0),
			_ => 0,
		}
	}

	pub fn as_float(&self) -> f64 {
		match self {
			Literal::True => 1.0,
			Literal::Int(ref v) => *v as f64,
			Literal::Float(ref v) => v.clone(),
			Literal::Strand(ref v) => v.value.parse::<f64>().unwrap_or(0.0),
			Literal::Number(ref v) => v.value.to_f64().unwrap_or(0.0),
			_ => 0.0,
		}
	}

	pub fn as_strand(&self) -> Strand {
		match self {
			Literal::Strand(ref v) => v.clone(),
			_ => Strand::from(self.to_string()),
		}
	}

	pub fn as_number(&self) -> Number {
		match self {
			Literal::True => Number::from(1),
			Literal::Int(ref v) => Number::from(*v),
			Literal::Float(ref v) => Number::from(*v),
			Literal::Number(ref v) => v.clone(),
			Literal::Strand(ref v) => Number::from(v.value.as_str()),
			Literal::Duration(ref v) => v.value.as_secs().into(),
			Literal::Datetime(ref v) => v.value.timestamp().into(),
			_ => Number::from(0),
		}
	}

	pub fn as_datetime(&self) -> Datetime {
		match self {
			Literal::Strand(ref v) => Datetime::from(v.value.as_str()),
			Literal::Datetime(ref v) => v.clone(),
			_ => Datetime::default(),
		}
	}

	pub fn as_duration(&self) -> Duration {
		match self {
			Literal::Strand(ref v) => Duration::from(v.value.as_str()),
			Literal::Duration(ref v) => v.clone(),
			_ => Duration::default(),
		}
	}
}

impl fmt::Display for Literal {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Literal::None => write!(f, "NONE"),
			Literal::Void => write!(f, "VOID"),
			Literal::Null => write!(f, "NULL"),
			Literal::True => write!(f, "true"),
			Literal::False => write!(f, "false"),
			Literal::Int(v) => write!(f, "{}", v),
			Literal::Float(v) => write!(f, "{}", v),
			Literal::Param(ref v) => write!(f, "{}", v),
			Literal::Idiom(ref v) => write!(f, "{}", v),
			Literal::Table(ref v) => write!(f, "{}", v),
			Literal::Thing(ref v) => write!(f, "{}", v),
			Literal::Model(ref v) => write!(f, "{}", v),
			Literal::Regex(ref v) => write!(f, "{}", v),
			Literal::Point(ref v) => write!(f, "{}", v),
			Literal::Array(ref v) => write!(f, "{}", v),
			Literal::Object(ref v) => write!(f, "{}", v),
			Literal::Number(ref v) => write!(f, "{}", v),
			Literal::Strand(ref v) => write!(f, "{}", v),
			Literal::Polygon(ref v) => write!(f, "{}", v),
			Literal::Duration(ref v) => write!(f, "{}", v),
			Literal::Datetime(ref v) => write!(f, "{}", v),
			Literal::Function(ref v) => write!(f, "{}", v),
			Literal::Subquery(ref v) => write!(f, "{}", v),
		}
	}
}

impl dbs::Process for Literal {
	fn process(
		&self,
		ctx: &Parent,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		match self {
			Literal::None => Ok(Literal::None),
			Literal::Void => Ok(Literal::Void),
			Literal::Null => Ok(Literal::Null),
			Literal::True => Ok(Literal::True),
			Literal::False => Ok(Literal::False),
			Literal::Param(ref v) => v.process(ctx, exe, doc),
			Literal::Idiom(ref v) => v.process(ctx, exe, doc),
			Literal::Array(ref v) => v.process(ctx, exe, doc),
			Literal::Object(ref v) => v.process(ctx, exe, doc),
			Literal::Function(ref v) => v.process(ctx, exe, doc),
			Literal::Subquery(ref v) => v.process(ctx, exe, doc),
			_ => Ok(self.to_owned()),
		}
	}
}

impl Serialize for Literal {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if s.is_human_readable() {
			match self {
				Literal::None => s.serialize_none(),
				Literal::Void => s.serialize_none(),
				Literal::Null => s.serialize_none(),
				Literal::True => s.serialize_bool(true),
				Literal::False => s.serialize_bool(false),
				Literal::Int(v) => s.serialize_i64(*v),
				Literal::Float(v) => s.serialize_f64(*v),
				Literal::Thing(ref v) => s.serialize_some(v),
				Literal::Point(ref v) => s.serialize_some(v),
				Literal::Array(ref v) => s.serialize_some(v),
				Literal::Object(ref v) => s.serialize_some(v),
				Literal::Number(ref v) => s.serialize_some(v),
				Literal::Strand(ref v) => s.serialize_some(v),
				Literal::Polygon(ref v) => s.serialize_some(v),
				Literal::Duration(ref v) => s.serialize_some(v),
				Literal::Datetime(ref v) => s.serialize_some(v),
				_ => s.serialize_none(),
			}
		} else {
			match self {
				Literal::None => s.serialize_unit_variant(NAME, 0, "None"),
				Literal::Void => s.serialize_unit_variant(NAME, 2, "Void"),
				Literal::Null => s.serialize_unit_variant(NAME, 1, "Null"),
				Literal::True => s.serialize_unit_variant(NAME, 3, "True"),
				Literal::False => s.serialize_unit_variant(NAME, 4, "False"),
				Literal::Int(ref v) => s.serialize_newtype_variant(NAME, 5, "Int", v),
				Literal::Float(ref v) => s.serialize_newtype_variant(NAME, 6, "Float", v),
				Literal::Param(ref v) => s.serialize_newtype_variant(NAME, 7, "Param", v),
				Literal::Idiom(ref v) => s.serialize_newtype_variant(NAME, 8, "Idiom", v),
				Literal::Table(ref v) => s.serialize_newtype_variant(NAME, 9, "Table", v),
				Literal::Thing(ref v) => s.serialize_newtype_variant(NAME, 10, "Thing", v),
				Literal::Model(ref v) => s.serialize_newtype_variant(NAME, 11, "Model", v),
				Literal::Regex(ref v) => s.serialize_newtype_variant(NAME, 12, "Regex", v),
				Literal::Point(ref v) => s.serialize_newtype_variant(NAME, 13, "Point", v),
				Literal::Array(ref v) => s.serialize_newtype_variant(NAME, 14, "Array", v),
				Literal::Object(ref v) => s.serialize_newtype_variant(NAME, 15, "Object", v),
				Literal::Number(ref v) => s.serialize_newtype_variant(NAME, 16, "Number", v),
				Literal::Strand(ref v) => s.serialize_newtype_variant(NAME, 17, "Strand", v),
				Literal::Polygon(ref v) => s.serialize_newtype_variant(NAME, 18, "Polygon", v),
				Literal::Duration(ref v) => s.serialize_newtype_variant(NAME, 19, "Duration", v),
				Literal::Datetime(ref v) => s.serialize_newtype_variant(NAME, 20, "Datetime", v),
				Literal::Function(ref v) => s.serialize_newtype_variant(NAME, 21, "Function", v),
				Literal::Subquery(ref v) => s.serialize_newtype_variant(NAME, 22, "Subquery", v),
			}
		}
	}
}

impl ops::Add for Literal {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		fnc::operate::add(&self, &other).unwrap_or(Literal::Null)
	}
}

impl ops::Sub for Literal {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		fnc::operate::sub(&self, &other).unwrap_or(Literal::Null)
	}
}

impl ops::Mul for Literal {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		fnc::operate::mul(&self, &other).unwrap_or(Literal::Null)
	}
}

impl ops::Div for Literal {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		fnc::operate::div(&self, &other).unwrap_or(Literal::Null)
	}
}

pub fn literal(i: &str) -> IResult<&str, Literal> {
	alt((
		map(tag_no_case("NONE"), |_| Literal::None),
		map(tag_no_case("VOID"), |_| Literal::Void),
		map(tag_no_case("NULL"), |_| Literal::Null),
		map(tag_no_case("true"), |_| Literal::True),
		map(tag_no_case("false"), |_| Literal::False),
		map(subquery, |v| Literal::Subquery(v)),
		map(function, |v| Literal::Function(v)),
		map(datetime, |v| Literal::Datetime(v)),
		map(duration, |v| Literal::Duration(v)),
		map(polygon, |v| Literal::Polygon(v)),
		map(number, |v| Literal::Number(v)),
		map(strand, |v| Literal::Strand(v)),
		map(object, |v| Literal::Object(v)),
		map(array, |v| Literal::Array(v)),
		map(point, |v| Literal::Point(v)),
		map(param, |v| Literal::Param(v)),
		map(thing, |v| Literal::Thing(v)),
		map(model, |v| Literal::Model(v)),
		map(idiom, |v| Literal::Idiom(v)),
	))(i)
}

pub fn what(i: &str) -> IResult<&str, Literal> {
	alt((
		map(param, |v| Literal::Param(v)),
		map(model, |v| Literal::Model(v)),
		map(regex, |v| Literal::Regex(v)),
		map(thing, |v| Literal::Thing(v)),
		map(table, |v| Literal::Table(v)),
	))(i)
}

pub fn json(i: &str) -> IResult<&str, Literal> {
	alt((
		map(tag_no_case("NULL"), |_| Literal::Null),
		map(tag_no_case("true"), |_| Literal::True),
		map(tag_no_case("false"), |_| Literal::False),
		map(datetime_raw, |v| Literal::Datetime(v)),
		map(duration_raw, |v| Literal::Duration(v)),
		map(number, |v| Literal::Number(v)),
		map(object, |v| Literal::Object(v)),
		map(array, |v| Literal::Array(v)),
		map(rest, |v| Literal::Strand(Strand::from(v))),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn check_none() {
		assert_eq!(true, Literal::None.is_none());
		assert_eq!(true, Literal::Void.is_none());
		assert_eq!(true, Literal::Null.is_none());
		assert_eq!(false, Literal::from(1).is_none());
	}

	#[test]
	fn check_void() {
		assert_eq!(true, Literal::None.is_void());
		assert_eq!(true, Literal::Void.is_void());
		assert_eq!(false, Literal::Null.is_void());
		assert_eq!(false, Literal::from(1).is_void());
	}

	#[test]
	fn check_null() {
		assert_eq!(true, Literal::None.is_null());
		assert_eq!(false, Literal::Void.is_null());
		assert_eq!(true, Literal::Null.is_null());
		assert_eq!(false, Literal::from(1).is_null());
	}

	#[test]
	fn check_true() {
		assert_eq!(false, Literal::None.is_true());
		assert_eq!(true, Literal::True.is_true());
		assert_eq!(false, Literal::False.is_true());
		assert_eq!(false, Literal::from(1).is_true());
		assert_eq!(true, Literal::from("true").is_true());
		assert_eq!(false, Literal::from("false").is_true());
		assert_eq!(false, Literal::from("something").is_true());
	}

	#[test]
	fn check_false() {
		assert_eq!(false, Literal::None.is_false());
		assert_eq!(false, Literal::True.is_false());
		assert_eq!(true, Literal::False.is_false());
		assert_eq!(false, Literal::from(1).is_false());
		assert_eq!(false, Literal::from("true").is_false());
		assert_eq!(true, Literal::from("false").is_false());
		assert_eq!(false, Literal::from("something").is_false());
	}

	#[test]
	fn convert_bool() {
		assert_eq!(false, Literal::None.as_bool());
		assert_eq!(false, Literal::Null.as_bool());
		assert_eq!(false, Literal::Void.as_bool());
		assert_eq!(true, Literal::True.as_bool());
		assert_eq!(false, Literal::False.as_bool());
		assert_eq!(false, Literal::from(0).as_bool());
		assert_eq!(true, Literal::from(1).as_bool());
		assert_eq!(false, Literal::from(-1).as_bool());
		assert_eq!(true, Literal::from(1.1).as_bool());
		assert_eq!(false, Literal::from(-1.1).as_bool());
		assert_eq!(true, Literal::from("true").as_bool());
		assert_eq!(false, Literal::from("false").as_bool());
		assert_eq!(true, Literal::from("falsey").as_bool());
		assert_eq!(true, Literal::from("something").as_bool());
	}

	#[test]
	fn convert_int() {
		assert_eq!(0, Literal::None.as_int());
		assert_eq!(0, Literal::Null.as_int());
		assert_eq!(0, Literal::Void.as_int());
		assert_eq!(1, Literal::True.as_int());
		assert_eq!(0, Literal::False.as_int());
		assert_eq!(0, Literal::from(0).as_int());
		assert_eq!(1, Literal::from(1).as_int());
		assert_eq!(-1, Literal::from(-1).as_int());
		assert_eq!(1, Literal::from(1.1).as_int());
		assert_eq!(-1, Literal::from(-1.1).as_int());
		assert_eq!(3, Literal::from("3").as_int());
		assert_eq!(0, Literal::from("true").as_int());
		assert_eq!(0, Literal::from("false").as_int());
		assert_eq!(0, Literal::from("something").as_int());
	}

	#[test]
	fn convert_float() {
		assert_eq!(0.0, Literal::None.as_float());
		assert_eq!(0.0, Literal::Null.as_float());
		assert_eq!(0.0, Literal::Void.as_float());
		assert_eq!(1.0, Literal::True.as_float());
		assert_eq!(0.0, Literal::False.as_float());
		assert_eq!(0.0, Literal::from(0).as_float());
		assert_eq!(1.0, Literal::from(1).as_float());
		assert_eq!(-1.0, Literal::from(-1).as_float());
		assert_eq!(1.1, Literal::from(1.1).as_float());
		assert_eq!(-1.1, Literal::from(-1.1).as_float());
		assert_eq!(3.0, Literal::from("3").as_float());
		assert_eq!(0.0, Literal::from("true").as_float());
		assert_eq!(0.0, Literal::from("false").as_float());
		assert_eq!(0.0, Literal::from("something").as_float());
	}

	#[test]
	fn convert_number() {
		assert_eq!(Number::from(0), Literal::None.as_number());
		assert_eq!(Number::from(0), Literal::Null.as_number());
		assert_eq!(Number::from(0), Literal::Void.as_number());
		assert_eq!(Number::from(1), Literal::True.as_number());
		assert_eq!(Number::from(0), Literal::False.as_number());
		assert_eq!(Number::from(0), Literal::from(0).as_number());
		assert_eq!(Number::from(1), Literal::from(1).as_number());
		assert_eq!(Number::from(-1), Literal::from(-1).as_number());
		assert_eq!(Number::from(1.1), Literal::from(1.1).as_number());
		assert_eq!(Number::from(-1.1), Literal::from(-1.1).as_number());
		assert_eq!(Number::from(3), Literal::from("3").as_number());
		assert_eq!(Number::from(0), Literal::from("true").as_number());
		assert_eq!(Number::from(0), Literal::from("false").as_number());
		assert_eq!(Number::from(0), Literal::from("something").as_number());
	}

	#[test]
	fn convert_strand() {
		assert_eq!(Strand::from("NONE"), Literal::None.as_strand());
		assert_eq!(Strand::from("NULL"), Literal::Null.as_strand());
		assert_eq!(Strand::from("VOID"), Literal::Void.as_strand());
		assert_eq!(Strand::from("true"), Literal::True.as_strand());
		assert_eq!(Strand::from("false"), Literal::False.as_strand());
		assert_eq!(Strand::from("0"), Literal::from(0).as_strand());
		assert_eq!(Strand::from("1"), Literal::from(1).as_strand());
		assert_eq!(Strand::from("-1"), Literal::from(-1).as_strand());
		assert_eq!(Strand::from("1.1"), Literal::from(1.1).as_strand());
		assert_eq!(Strand::from("-1.1"), Literal::from(-1.1).as_strand());
		assert_eq!(Strand::from("3"), Literal::from("3").as_strand());
		assert_eq!(Strand::from("true"), Literal::from("true").as_strand());
		assert_eq!(Strand::from("false"), Literal::from("false").as_strand());
		assert_eq!(Strand::from("something"), Literal::from("something").as_strand());
	}
}
