use crate::sql::comment::comment;
use crate::sql::error::IResult;
use crate::sql::operator::{assigner, operator};
use dec::prelude::FromPrimitive;
use dec::prelude::ToPrimitive;
use dec::Decimal;
use dec::MathematicalOps;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::multispace1;
use nom::combinator::eof;
use nom::combinator::map;
use nom::combinator::peek;
use nom::number::complete::recognize_float;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::iter::Product;
use std::iter::Sum;
use std::ops;
use std::str::FromStr;

#[derive(Clone, Debug, Deserialize)]
pub enum Number {
	Int(i64),
	Float(f64),
	Decimal(Decimal),
}

impl Default for Number {
	fn default() -> Self {
		Number::Decimal(Decimal::from(0))
	}
}

impl From<i8> for Number {
	fn from(i: i8) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<i16> for Number {
	fn from(i: i16) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<i32> for Number {
	fn from(i: i32) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<i64> for Number {
	fn from(i: i64) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<i128> for Number {
	fn from(i: i128) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<isize> for Number {
	fn from(i: isize) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<u8> for Number {
	fn from(i: u8) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<u16> for Number {
	fn from(i: u16) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<u32> for Number {
	fn from(i: u32) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<u64> for Number {
	fn from(i: u64) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<u128> for Number {
	fn from(i: u128) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<usize> for Number {
	fn from(i: usize) -> Self {
		Number::Decimal(Decimal::from(i))
	}
}

impl From<f32> for Number {
	fn from(f: f32) -> Self {
		Number::Decimal(Decimal::from_f32(f).unwrap_or_default())
	}
}

impl From<f64> for Number {
	fn from(f: f64) -> Self {
		Number::Decimal(Decimal::from_f64(f).unwrap_or_default())
	}
}

impl<'a> From<&'a str> for Number {
	fn from(s: &str) -> Self {
		match s.contains(&['e', 'E'][..]) {
			true => Number::Decimal(Decimal::from_scientific(s).unwrap_or_default()),
			false => Number::Decimal(Decimal::from_str(s).unwrap_or_default()),
		}
	}
}

impl From<String> for Number {
	fn from(s: String) -> Self {
		match s.contains(&['e', 'E'][..]) {
			true => Number::Decimal(Decimal::from_scientific(&s).unwrap_or_default()),
			false => Number::Decimal(Decimal::from_str(&s).unwrap_or_default()),
		}
	}
}

impl From<Decimal> for Number {
	fn from(v: Decimal) -> Self {
		Number::Decimal(v)
	}
}

impl fmt::Display for Number {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Number::Int(v) => write!(f, "{}", v),
			Number::Float(v) => write!(f, "{}", v),
			Number::Decimal(v) => write!(f, "{}", v),
		}
	}
}

impl Serialize for Number {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if s.is_human_readable() {
			match self {
				Number::Int(v) => s.serialize_i64(*v),
				Number::Float(v) => s.serialize_f64(*v),
				Number::Decimal(v) => s.serialize_some(v),
			}
		} else {
			match self {
				Number::Int(v) => s.serialize_newtype_variant("Number", 0, "Int", v),
				Number::Float(v) => s.serialize_newtype_variant("Number", 1, "Float", v),
				Number::Decimal(v) => s.serialize_newtype_variant("Number", 2, "Decimal", v),
			}
		}
	}
}

impl Number {
	// -----------------------------------
	// Simple number detection
	// -----------------------------------

	pub fn is_truthy(&self) -> bool {
		match self {
			Number::Int(v) => v != &0,
			Number::Float(v) => v != &0.0,
			Number::Decimal(v) => v != &Decimal::default(),
		}
	}

	// -----------------------------------
	// Simple conversion of number
	// -----------------------------------

	pub fn as_int(self) -> i64 {
		match self {
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or(0),
		}
	}

	pub fn as_float(self) -> f64 {
		match self {
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.to_f64().unwrap_or(0.0),
		}
	}

	pub fn as_decimal(self) -> Decimal {
		match self {
			Number::Int(v) => Decimal::from(v),
			Number::Float(v) => Decimal::from_f64(v).unwrap_or_default(),
			Number::Decimal(v) => v,
		}
	}

	// -----------------------------------
	//
	// -----------------------------------

	pub fn to_usize(&self) -> usize {
		match self {
			Number::Int(v) => *v as usize,
			Number::Float(v) => *v as usize,
			Number::Decimal(v) => v.to_usize().unwrap_or(0),
		}
	}

	pub fn to_int(&self) -> i64 {
		match self {
			Number::Int(v) => *v,
			Number::Float(v) => *v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or(0),
		}
	}

	pub fn to_float(&self) -> f64 {
		match self {
			Number::Int(v) => *v as f64,
			Number::Float(v) => *v,
			Number::Decimal(v) => v.to_f64().unwrap_or(0.0),
		}
	}

	pub fn to_decimal(&self) -> Decimal {
		match self {
			Number::Int(v) => Decimal::from(*v),
			Number::Float(v) => Decimal::from_f64(*v).unwrap_or_default(),
			Number::Decimal(v) => *v,
		}
	}

	// -----------------------------------
	//
	// -----------------------------------

	pub fn abs(self) -> Self {
		match self {
			Number::Int(v) => v.abs().into(),
			Number::Float(v) => v.abs().into(),
			Number::Decimal(v) => v.abs().into(),
		}
	}

	pub fn ceil(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.ceil().into(),
			Number::Decimal(v) => v.ceil().into(),
		}
	}

	pub fn floor(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.floor().into(),
			Number::Decimal(v) => v.floor().into(),
		}
	}

	pub fn round(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.round().into(),
			Number::Decimal(v) => v.round().into(),
		}
	}

	pub fn sqrt(self) -> Self {
		match self {
			Number::Int(v) => (v as f64).sqrt().into(),
			Number::Float(v) => v.sqrt().into(),
			Number::Decimal(v) => v.sqrt().unwrap_or_default().into(),
		}
	}

	// -----------------------------------
	//
	// -----------------------------------

	pub fn fixed(self, precision: usize) -> Number {
		match self {
			Number::Int(v) => format!("{:.1$}", v, precision).into(),
			Number::Float(v) => format!("{:.1$}", v, precision).into(),
			Number::Decimal(v) => v.round_dp(precision as u32).into(),
		}
	}
}

impl Eq for Number {}

impl Ord for Number {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}

impl PartialEq for Number {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.eq(w),
			(Number::Float(v), Number::Float(w)) => v.eq(w),
			(Number::Decimal(v), Number::Decimal(w)) => v.eq(w),
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => (*v as f64).eq(w),
			(Number::Float(v), Number::Int(w)) => v.eq(&(*w as f64)),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).eq(w),
			(Number::Decimal(v), Number::Int(w)) => v.eq(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				Decimal::from_f64(*v).unwrap_or_default().eq(w)
			}
			(Number::Decimal(v), Number::Float(w)) => {
				v.eq(&Decimal::from_f64(*w).unwrap_or_default())
			}
		}
	}
}

impl PartialOrd for Number {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.partial_cmp(w),
			(Number::Float(v), Number::Float(w)) => v.partial_cmp(w),
			(Number::Decimal(v), Number::Decimal(w)) => v.partial_cmp(w),
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => (*v as f64).partial_cmp(w),
			(Number::Float(v), Number::Int(w)) => v.partial_cmp(&(*w as f64)),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).partial_cmp(w),
			(Number::Decimal(v), Number::Int(w)) => v.partial_cmp(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				Decimal::from_f64(*v).unwrap_or_default().partial_cmp(w)
			}
			(Number::Decimal(v), Number::Float(w)) => {
				v.partial_cmp(&Decimal::from_f64(*w).unwrap_or_default())
			}
		}
	}
}

impl ops::Add for Number {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v + w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v + w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v + w),
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 + w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v + w as f64),
			(v, w) => Number::from(v.as_decimal() + w.as_decimal()),
		}
	}
}

impl<'a, 'b> ops::Add<&'b Number> for &'a Number {
	type Output = Number;
	fn add(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v + w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v + w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v + w),
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 + w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v + *w as f64),
			(v, w) => Number::from(v.to_decimal() + w.to_decimal()),
		}
	}
}

impl ops::Sub for Number {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v - w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v - w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v - w),
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 - w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v - w as f64),
			(v, w) => Number::from(v.as_decimal() - w.as_decimal()),
		}
	}
}

impl<'a, 'b> ops::Sub<&'b Number> for &'a Number {
	type Output = Number;
	fn sub(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v - w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v - w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v - w),
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 - w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v - *w as f64),
			(v, w) => Number::from(v.to_decimal() - w.to_decimal()),
		}
	}
}

impl ops::Mul for Number {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v * w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v * w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v * w),
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 * w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v * w as f64),
			(v, w) => Number::from(v.as_decimal() * w.as_decimal()),
		}
	}
}

impl<'a, 'b> ops::Mul<&'b Number> for &'a Number {
	type Output = Number;
	fn mul(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v * w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v * w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v * w),
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 * w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v * *w as f64),
			(v, w) => Number::from(v.to_decimal() * w.to_decimal()),
		}
	}
}

impl ops::Div for Number {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v / w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v / w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v / w),
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 / w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v / w as f64),
			(v, w) => Number::from(v.as_decimal() / w.as_decimal()),
		}
	}
}

impl<'a, 'b> ops::Div<&'b Number> for &'a Number {
	type Output = Number;
	fn div(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v / w),
			(Number::Float(v), Number::Float(w)) => Number::Float(v / w),
			(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(v / w),
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 / w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v / *w as f64),
			(v, w) => Number::from(v.to_decimal() / w.to_decimal()),
		}
	}
}

// ------------------------------

impl Sum<Self> for Number {
	fn sum<I>(iter: I) -> Number
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(Number::Int(0), |a, b| a + b)
	}
}

impl<'a> Sum<&'a Self> for Number {
	fn sum<I>(iter: I) -> Number
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(Number::Int(0), |a, b| &a + b)
	}
}

impl Product<Self> for Number {
	fn product<I>(iter: I) -> Number
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(Number::Int(1), |a, b| a * b)
	}
}

impl<'a> Product<&'a Self> for Number {
	fn product<I>(iter: I) -> Number
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(Number::Int(1), |a, b| &a * b)
	}
}

pub fn number(i: &str) -> IResult<&str, Number> {
	let (i, v) = recognize_float(i)?;
	let (i, _) = peek(alt((
		map(multispace1, |_| ()),
		map(operator, |_| ()),
		map(assigner, |_| ()),
		map(comment, |_| ()),
		map(tag("]"), |_| ()),
		map(tag("}"), |_| ()),
		map(tag(";"), |_| ()),
		map(tag(","), |_| ()),
		map(eof, |_| ()),
	)))(i)?;
	Ok((i, Number::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn number_integer() {
		let sql = "123";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("123", format!("{}", out));
		assert_eq!(out, Number::from(123));
	}

	#[test]
	fn number_integer_neg() {
		let sql = "-123";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-123", format!("{}", out));
		assert_eq!(out, Number::from(-123));
	}

	#[test]
	fn number_decimal() {
		let sql = "123.45";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("123.45", format!("{}", out));
		assert_eq!(out, Number::from(123.45));
	}

	#[test]
	fn number_decimal_neg() {
		let sql = "-123.45";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-123.45", format!("{}", out));
		assert_eq!(out, Number::from(-123.45));
	}

	#[test]
	fn number_scientific_lower() {
		let sql = "12345e-1";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1234.5", format!("{}", out));
		assert_eq!(out, Number::from(1234.5));
	}

	#[test]
	fn number_scientific_lower_neg() {
		let sql = "-12345e-1";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-1234.5", format!("{}", out));
		assert_eq!(out, Number::from(-1234.5));
	}

	#[test]
	fn number_scientific_upper() {
		let sql = "12345E-02";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("123.45", format!("{}", out));
		assert_eq!(out, Number::from(123.45));
	}

	#[test]
	fn number_scientific_upper_neg() {
		let sql = "-12345E-02";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-123.45", format!("{}", out));
		assert_eq!(out, Number::from(-123.45));
	}
}
