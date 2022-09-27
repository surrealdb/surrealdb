use crate::sql::ending::number as ending;
use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use bigdecimal::BigDecimal;
use bigdecimal::FromPrimitive;
use bigdecimal::ToPrimitive;
use nom::branch::alt;
use nom::character::complete::i64;
use nom::combinator::map;
use nom::number::complete::recognize_float;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;
use std::iter::Product;
use std::iter::Sum;
use std::ops;
use std::str::FromStr;

#[derive(Clone, Debug, Deserialize)]
pub enum Number {
	Int(i64),
	Float(f64),
	Decimal(BigDecimal),
}

impl Default for Number {
	fn default() -> Self {
		Number::Int(0)
	}
}

impl From<i8> for Number {
	fn from(i: i8) -> Self {
		Number::Int(i as i64)
	}
}

impl From<i16> for Number {
	fn from(i: i16) -> Self {
		Number::Int(i as i64)
	}
}

impl From<i32> for Number {
	fn from(i: i32) -> Self {
		Number::Int(i as i64)
	}
}

impl From<i64> for Number {
	fn from(i: i64) -> Self {
		Number::Int(i)
	}
}

impl From<isize> for Number {
	fn from(i: isize) -> Self {
		Number::Int(i as i64)
	}
}

impl From<u8> for Number {
	fn from(i: u8) -> Self {
		Number::Int(i as i64)
	}
}

impl From<u16> for Number {
	fn from(i: u16) -> Self {
		Number::Int(i as i64)
	}
}

impl From<u32> for Number {
	fn from(i: u32) -> Self {
		Number::Int(i as i64)
	}
}

impl From<u64> for Number {
	fn from(i: u64) -> Self {
		Number::Int(i as i64)
	}
}

impl From<usize> for Number {
	fn from(i: usize) -> Self {
		Number::Int(i as i64)
	}
}

impl From<f32> for Number {
	fn from(f: f32) -> Self {
		Number::Float(f as f64)
	}
}

impl From<f64> for Number {
	fn from(f: f64) -> Self {
		Number::Float(f as f64)
	}
}

impl From<&str> for Number {
	fn from(s: &str) -> Self {
		Number::Decimal(BigDecimal::from_str(s).unwrap_or_default())
	}
}

impl From<String> for Number {
	fn from(s: String) -> Self {
		Number::Decimal(BigDecimal::from_str(&s).unwrap_or_default())
	}
}

impl From<BigDecimal> for Number {
	fn from(v: BigDecimal) -> Self {
		Number::Decimal(v)
	}
}

impl fmt::Display for Number {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Number::Int(v) => Display::fmt(v, f),
			Number::Float(v) => Display::fmt(v, f),
			Number::Decimal(v) => Display::fmt(v, f),
		}
	}
}

impl Serialize for Number {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			match self {
				Number::Int(v) => s.serialize_newtype_variant("Number", 0, "Int", v),
				Number::Float(v) => s.serialize_newtype_variant("Number", 1, "Float", v),
				Number::Decimal(v) => s.serialize_newtype_variant("Number", 2, "Decimal", v),
			}
		} else {
			match self {
				Number::Int(v) => s.serialize_i64(*v),
				Number::Float(v) => s.serialize_f64(*v),
				Number::Decimal(v) => s.serialize_some(v),
			}
		}
	}
}

impl Number {
	// -----------------------------------
	// Constants
	// -----------------------------------

	pub const NAN: Number = Number::Float(f64::NAN);

	// -----------------------------------
	// Simple number detection
	// -----------------------------------

	pub fn is_int(&self) -> bool {
		matches!(self, Number::Int(_))
	}

	pub fn is_float(&self) -> bool {
		matches!(self, Number::Float(_))
	}

	pub fn is_decimal(&self) -> bool {
		matches!(self, Number::Decimal(_))
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Number::Int(v) => v != &0,
			Number::Float(v) => v != &0.0,
			Number::Decimal(v) => v != &BigDecimal::default(),
		}
	}

	// -----------------------------------
	// Simple conversion of number
	// -----------------------------------

	pub fn as_usize(self) -> usize {
		match self {
			Number::Int(v) => v as usize,
			Number::Float(v) => v as usize,
			Number::Decimal(v) => v.to_usize().unwrap_or_default(),
		}
	}

	pub fn as_int(self) -> i64 {
		match self {
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or_default(),
		}
	}

	pub fn as_float(self) -> f64 {
		match self {
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.to_f64().unwrap_or_default(),
		}
	}

	pub fn as_decimal(self) -> BigDecimal {
		match self {
			Number::Int(v) => BigDecimal::from_i64(v).unwrap_or_default(),
			Number::Float(v) => BigDecimal::from_f64(v).unwrap_or_default(),
			Number::Decimal(v) => v,
		}
	}

	// -----------------------------------
	// Complex conversion of number
	// -----------------------------------

	pub fn to_usize(&self) -> usize {
		match self {
			Number::Int(v) => *v as usize,
			Number::Float(v) => *v as usize,
			Number::Decimal(v) => v.to_usize().unwrap_or_default(),
		}
	}

	pub fn to_int(&self) -> i64 {
		match self {
			Number::Int(v) => *v,
			Number::Float(v) => *v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or_default(),
		}
	}

	pub fn to_float(&self) -> f64 {
		match self {
			Number::Int(v) => *v as f64,
			Number::Float(v) => *v,
			Number::Decimal(v) => v.to_f64().unwrap_or_default(),
		}
	}

	pub fn to_decimal(&self) -> BigDecimal {
		match self {
			Number::Int(v) => BigDecimal::from_i64(*v).unwrap_or_default(),
			Number::Float(v) => BigDecimal::from_f64(*v).unwrap_or_default(),
			Number::Decimal(v) => v.clone(),
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
			Number::Decimal(v) => (v + BigDecimal::from_f32(0.5).unwrap()).round(0).into(),
		}
	}

	pub fn floor(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.floor().into(),
			Number::Decimal(v) => (v - BigDecimal::from_f32(0.5).unwrap()).round(0).into(),
		}
	}

	pub fn round(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.round().into(),
			Number::Decimal(v) => v.round(0).into(),
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
			Number::Decimal(v) => v.round(precision as i64).into(),
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
			(Number::Int(v), Number::Decimal(w)) => BigDecimal::from(*v).eq(w),
			(Number::Decimal(v), Number::Int(w)) => v.eq(&BigDecimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				BigDecimal::from_f64(*v).unwrap_or_default().eq(w)
			}
			(Number::Decimal(v), Number::Float(w)) => {
				v.eq(&BigDecimal::from_f64(*w).unwrap_or_default())
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
			(Number::Int(v), Number::Decimal(w)) => BigDecimal::from(*v).partial_cmp(w),
			(Number::Decimal(v), Number::Int(w)) => v.partial_cmp(&BigDecimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				BigDecimal::from_f64(*v).unwrap_or_default().partial_cmp(w)
			}
			(Number::Decimal(v), Number::Float(w)) => {
				v.partial_cmp(&BigDecimal::from_f64(*w).unwrap_or_default())
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

pub struct Sorted<T>(pub T);

pub trait Sort {
	fn sorted(&mut self) -> Sorted<&Self>
	where
		Self: Sized;
}

impl Sort for Vec<Number> {
	fn sorted(&mut self) -> Sorted<&Vec<Number>> {
		self.sort();
		Sorted(self)
	}
}

pub fn number(i: &str) -> IResult<&str, Number> {
	alt((map(integer, Number::from), map(decimal, Number::from)))(i)
}

pub fn integer(i: &str) -> IResult<&str, i64> {
	let (i, v) = i64(i)?;
	let (i, _) = ending(i)?;
	Ok((i, v))
}

pub fn decimal(i: &str) -> IResult<&str, &str> {
	let (i, v) = recognize_float(i)?;
	let (i, _) = ending(i)?;
	Ok((i, v))
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
