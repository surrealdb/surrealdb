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
use std::fmt::{self, Display, Formatter};
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
		Self::Int(0)
	}
}

impl From<i8> for Number {
	fn from(i: i8) -> Self {
		Self::Int(i as i64)
	}
}

impl From<i16> for Number {
	fn from(i: i16) -> Self {
		Self::Int(i as i64)
	}
}

impl From<i32> for Number {
	fn from(i: i32) -> Self {
		Self::Int(i as i64)
	}
}

impl From<i64> for Number {
	fn from(i: i64) -> Self {
		Self::Int(i)
	}
}

impl From<isize> for Number {
	fn from(i: isize) -> Self {
		Self::Int(i as i64)
	}
}

impl From<u8> for Number {
	fn from(i: u8) -> Self {
		Self::Int(i as i64)
	}
}

impl From<u16> for Number {
	fn from(i: u16) -> Self {
		Self::Int(i as i64)
	}
}

impl From<u32> for Number {
	fn from(i: u32) -> Self {
		Self::Int(i as i64)
	}
}

impl From<u64> for Number {
	fn from(i: u64) -> Self {
		Self::Int(i as i64)
	}
}

impl From<usize> for Number {
	fn from(i: usize) -> Self {
		Self::Int(i as i64)
	}
}

impl From<f32> for Number {
	fn from(f: f32) -> Self {
		Self::Float(f as f64)
	}
}

impl From<f64> for Number {
	fn from(f: f64) -> Self {
		Self::Float(f)
	}
}

impl From<&str> for Number {
	fn from(s: &str) -> Self {
		Self::Decimal(BigDecimal::from_str(s).unwrap_or_default())
	}
}

impl From<String> for Number {
	fn from(s: String) -> Self {
		Self::from(s.as_str())
	}
}

impl From<BigDecimal> for Number {
	fn from(v: BigDecimal) -> Self {
		Self::Decimal(v)
	}
}

impl Display for Number {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Int(v) => Display::fmt(v, f),
			Self::Float(v) => Display::fmt(v, f),
			Self::Decimal(v) => Display::fmt(v, f),
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
				Self::Int(v) => s.serialize_newtype_variant("Number", 0, "Int", v),
				Self::Float(v) => s.serialize_newtype_variant("Number", 1, "Float", v),
				Self::Decimal(v) => s.serialize_newtype_variant("Number", 2, "Decimal", v),
			}
		} else {
			match self {
				Self::Int(v) => s.serialize_i64(*v),
				Self::Float(v) => s.serialize_f64(*v),
				Self::Decimal(v) => s.serialize_some(v),
			}
		}
	}
}

impl Number {
	// -----------------------------------
	// Constants
	// -----------------------------------

	pub const NAN: Self = Number::Float(f64::NAN);

	// -----------------------------------
	// Simple number detection
	// -----------------------------------

	pub fn is_int(&self) -> bool {
		matches!(self, Self::Int(_))
	}

	pub fn is_float(&self) -> bool {
		matches!(self, Self::Float(_))
	}

	pub fn is_decimal(&self) -> bool {
		matches!(self, Self::Decimal(_))
	}

	pub fn is_integer(&self) -> bool {
		match self {
			Number::Int(_) => true,
			Number::Float(_) => false,
			Number::Decimal(v) => v.is_integer(),
		}
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Self::Int(v) => v != &0,
			Self::Float(v) => v != &0.0,
			Self::Decimal(v) => v != &BigDecimal::default(),
		}
	}

	pub fn is_positive(&self) -> bool {
		match self {
			Number::Int(v) => v > &0,
			Number::Float(v) => v > &0.0,
			Number::Decimal(v) => v > &BigDecimal::from(0),
		}
	}

	// -----------------------------------
	// Simple conversion of number
	// -----------------------------------

	pub fn as_usize(self) -> usize {
		match self {
			Self::Int(v) => v as usize,
			Self::Float(v) => v as usize,
			Self::Decimal(v) => v.to_usize().unwrap_or_default(),
		}
	}

	pub fn as_int(self) -> i64 {
		match self {
			Self::Int(v) => v,
			Self::Float(v) => v as i64,
			Self::Decimal(v) => v.to_i64().unwrap_or_default(),
		}
	}

	pub fn as_float(self) -> f64 {
		match self {
			Self::Int(v) => v as f64,
			Self::Float(v) => v,
			Self::Decimal(v) => v.to_f64().unwrap_or_default(),
		}
	}

	pub fn as_decimal(self) -> BigDecimal {
		match self {
			Self::Int(v) => BigDecimal::from_i64(v).unwrap_or_default(),
			Self::Float(v) => BigDecimal::from_f64(v).unwrap_or_default(),
			Self::Decimal(v) => v,
		}
	}

	// -----------------------------------
	// Complex conversion of number
	// -----------------------------------

	pub fn to_usize(&self) -> usize {
		match self {
			Self::Int(v) => *v as usize,
			Self::Float(v) => *v as usize,
			Self::Decimal(v) => v.to_usize().unwrap_or_default(),
		}
	}

	pub fn to_int(&self) -> i64 {
		match self {
			Self::Int(v) => *v,
			Self::Float(v) => *v as i64,
			Self::Decimal(v) => v.to_i64().unwrap_or_default(),
		}
	}

	pub fn to_float(&self) -> f64 {
		match self {
			Self::Int(v) => *v as f64,
			Self::Float(v) => *v,
			Self::Decimal(v) => v.to_f64().unwrap_or_default(),
		}
	}

	pub fn to_decimal(&self) -> BigDecimal {
		match self {
			Self::Int(v) => BigDecimal::from_i64(*v).unwrap_or_default(),
			Self::Float(v) => BigDecimal::from_f64(*v).unwrap_or_default(),
			Self::Decimal(v) => v.clone(),
		}
	}

	// -----------------------------------
	//
	// -----------------------------------

	pub fn abs(self) -> Self {
		match self {
			Self::Int(v) => v.abs().into(),
			Self::Float(v) => v.abs().into(),
			Self::Decimal(v) => v.abs().into(),
		}
	}

	pub fn ceil(self) -> Self {
		match self {
			Self::Int(v) => v.into(),
			Self::Float(v) => v.ceil().into(),
			Self::Decimal(v) => {
				if v.digits() > 16 {
					let v = (v.to_f64().unwrap_or_default() + 0.5).round();
					BigDecimal::from_f64(v).unwrap_or_default().into()
				} else {
					(v + BigDecimal::from_f32(0.5).unwrap()).round(0).into()
				}
			}
		}
	}

	pub fn floor(self) -> Self {
		match self {
			Self::Int(v) => v.into(),
			Self::Float(v) => v.floor().into(),
			Self::Decimal(v) => {
				if v.digits() > 16 {
					let v = (v.to_f64().unwrap_or_default() - 0.5).round();
					BigDecimal::from_f64(v).unwrap_or_default().into()
				} else {
					(v - BigDecimal::from_f32(0.5).unwrap()).round(0).into()
				}
			}
		}
	}

	pub fn round(self) -> Self {
		match self {
			Self::Int(v) => v.into(),
			Self::Float(v) => v.round().into(),
			Self::Decimal(v) => {
				if v.digits() > 16 {
					let v = v.to_f64().unwrap_or_default().round();
					BigDecimal::from_f64(v).unwrap_or_default().into()
				} else {
					v.round(0).into()
				}
			}
		}
	}

	pub fn sqrt(self) -> Self {
		match self {
			Self::Int(v) => (v as f64).sqrt().into(),
			Self::Float(v) => v.sqrt().into(),
			Self::Decimal(v) => v.sqrt().unwrap_or_default().into(),
		}
	}

	// -----------------------------------
	//
	// -----------------------------------

	pub fn fixed(self, precision: usize) -> Number {
		match self {
			Self::Int(v) => format!("{:.1$}", v, precision).into(),
			Self::Float(v) => format!("{:.1$}", v, precision).into(),
			Self::Decimal(v) => v.round(precision as i64).into(),
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
			(Self::Int(v), Self::Int(w)) => v.eq(w),
			(Self::Float(v), Self::Float(w)) => v.eq(w),
			(Self::Decimal(v), Self::Decimal(w)) => v.eq(w),
			// ------------------------------
			(Self::Int(v), Self::Float(w)) => (*v as f64).eq(w),
			(Self::Float(v), Self::Int(w)) => v.eq(&(*w as f64)),
			// ------------------------------
			(Self::Int(v), Self::Decimal(w)) => BigDecimal::from(*v).eq(w),
			(Self::Decimal(v), Self::Int(w)) => v.eq(&BigDecimal::from(*w)),
			// ------------------------------
			(Self::Float(v), Self::Decimal(w)) => {
				BigDecimal::from_f64(*v).unwrap_or_default().eq(w)
			}
			(Self::Decimal(v), Self::Float(w)) => {
				v.eq(&BigDecimal::from_f64(*w).unwrap_or_default())
			}
		}
	}
}

impl PartialOrd for Number {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match (self, other) {
			(Self::Int(v), Self::Int(w)) => v.partial_cmp(w),
			(Self::Float(v), Self::Float(w)) => v.partial_cmp(w),
			(Self::Decimal(v), Self::Decimal(w)) => v.partial_cmp(w),
			// ------------------------------
			(Self::Int(v), Self::Float(w)) => (*v as f64).partial_cmp(w),
			(Self::Float(v), Self::Int(w)) => v.partial_cmp(&(*w as f64)),
			// ------------------------------
			(Self::Int(v), Self::Decimal(w)) => BigDecimal::from(*v).partial_cmp(w),
			(Self::Decimal(v), Self::Int(w)) => v.partial_cmp(&BigDecimal::from(*w)),
			// ------------------------------
			(Self::Float(v), Self::Decimal(w)) => {
				BigDecimal::from_f64(*v).unwrap_or_default().partial_cmp(w)
			}
			(Self::Decimal(v), Self::Float(w)) => {
				v.partial_cmp(&BigDecimal::from_f64(*w).unwrap_or_default())
			}
		}
	}
}

impl ops::Add for Number {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		match (self, other) {
			(Self::Int(v), Self::Int(w)) => Self::Int(v + w),
			(Self::Float(v), Self::Float(w)) => Self::Float(v + w),
			(Self::Decimal(v), Self::Decimal(w)) => Self::Decimal(v + w),
			(Self::Int(v), Self::Float(w)) => Self::Float(v as f64 + w),
			(Self::Float(v), Self::Int(w)) => Self::Float(v + w as f64),
			(v, w) => Self::from(v.as_decimal() + w.as_decimal()),
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
			(Self::Int(v), Self::Int(w)) => Self::Int(v - w),
			(Self::Float(v), Self::Float(w)) => Self::Float(v - w),
			(Self::Decimal(v), Self::Decimal(w)) => Self::Decimal(v - w),
			(Self::Int(v), Self::Float(w)) => Self::Float(v as f64 - w),
			(Self::Float(v), Self::Int(w)) => Self::Float(v - w as f64),
			(v, w) => Self::from(v.as_decimal() - w.as_decimal()),
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
			(Self::Int(v), Self::Int(w)) => Self::Int(v * w),
			(Self::Float(v), Self::Float(w)) => Self::Float(v * w),
			(Self::Decimal(v), Self::Decimal(w)) => Self::Decimal(v * w),
			(Self::Int(v), Self::Float(w)) => Self::Float(v as f64 * w),
			(Self::Float(v), Self::Int(w)) => Self::Float(v * w as f64),
			(v, w) => Self::from(v.as_decimal() * w.as_decimal()),
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
			(Self::Float(v), Self::Float(w)) => Self::Float(v / w),
			(Self::Decimal(v), Self::Decimal(w)) => Self::Decimal(v / w),
			(Self::Int(v), Self::Float(w)) => Self::Float(v as f64 / w),
			(Self::Float(v), Self::Int(w)) => Self::Float(v / w as f64),
			(v, w) => Self::from(v.as_decimal() / w.as_decimal()),
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
	fn sum<I>(iter: I) -> Self
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(Self::Int(0), |a, b| a + b)
	}
}

impl<'a> Sum<&'a Self> for Number {
	fn sum<I>(iter: I) -> Self
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(Self::Int(0), |a, b| &a + b)
	}
}

impl Product<Self> for Number {
	fn product<I>(iter: I) -> Number
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(Self::Int(1), |a, b| a * b)
	}
}

impl<'a> Product<&'a Self> for Number {
	fn product<I>(iter: I) -> Number
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(Self::Int(1), |a, b| &a * b)
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
		self.sort_unstable();
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
