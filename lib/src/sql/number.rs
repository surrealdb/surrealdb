pub use super::value::serde::I256;
use super::value::{TryAdd, TryDiv, TryMul, TryNeg, TryPow, TryRem, TrySub};
use crate::err::Error;
use crate::sql::strand::Strand;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash;
use std::iter::Product;
use std::iter::Sum;
use std::ops::{self, Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Number";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Number")]
#[revisioned(revision = 1)]
pub enum Number {
	Int(i64),
	Float(f64),
	Decimal(Decimal),
	// Add new variants here
	BigInt(I256),
}

impl Default for Number {
	fn default() -> Self {
		Self::Int(0)
	}
}

macro_rules! from_prim_ints {
	($($int: ty),*) => {
		$(
			impl From<$int> for Number {
				fn from(i: $int) -> Self {
					Self::Int(i as i64)
				}
			}
		)*
	};
}

from_prim_ints!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);

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

impl From<Decimal> for Number {
	fn from(v: Decimal) -> Self {
		Self::Decimal(v)
	}
}

impl From<I256> for Number {
	fn from(v: I256) -> Self {
		Self::BigInt(v)
	}
}

impl FromStr for Number {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Number {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Number {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Number {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		// Attempt to parse as i64
		match v.parse::<i64>() {
			// Store it as an i64
			Ok(v) => Ok(Self::Int(v)),
			// It wasn't parsed as a i64 so parse as a float
			_ => match f64::from_str(v) {
				// Store it as a float
				Ok(v) => Ok(Self::Float(v)),
				// It wasn't parsed as a number
				_ => match I256::from_str(v) {
					Ok(v) => Ok(Self::BigInt(v)),
					_ => Err(()),
				},
			},
		}
	}
}

macro_rules! try_into_prim {
	// TODO: switch to one argument per int once https://github.com/rust-lang/rust/issues/29599 is stable
	($($int: ty => $to_int: ident),*) => {
		$(
			impl TryFrom<Number> for $int {
				type Error = Error;
				fn try_from(value: Number) -> Result<Self, Self::Error> {
					match value {
						Number::Int(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_string(), stringify!($int))),
						},
						Number::Float(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_string(), stringify!($int))),
						},
						Number::Decimal(ref v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_string(), stringify!($int))),
						},
						Number::BigInt(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_string(), stringify!($int))),
						},
					}
				}
			}
		)*
	};
}

try_into_prim!(
	i8 => to_i8, i16 => to_i16, i32 => to_i32, i64 => to_i64, i128 => to_i128,
	u8 => to_u8, u16 => to_u16, u32 => to_u32, u64 => to_u64, u128 => to_u128,
	f32 => to_f32, f64 => to_f64
);

impl TryFrom<Number> for Decimal {
	type Error = Error;
	fn try_from(value: Number) -> Result<Self, Self::Error> {
		match value {
			Number::Int(v) => match Decimal::from_i64(v) {
				Some(v) => Ok(v),
				None => Err(Error::TryFrom(value.to_string(), "Decimal")),
			},
			Number::Float(v) => match Decimal::try_from(v) {
				Ok(v) => Ok(v),
				_ => Err(Error::TryFrom(value.to_string(), "Decimal")),
			},
			Number::Decimal(x) => Ok(x),
			Number::BigInt(x) => match Decimal::try_from(x.to_i128().unwrap_or_default()) {
				Ok(x) => Ok(x),
				_ => Err(Error::TryFrom(value.to_string(), "Decimal")),
			},
		}
	}
}

impl TryFrom<Number> for I256 {
	type Error = Error;
	fn try_from(value: Number) -> Result<Self, Self::Error> {
		match value {
			Number::Int(v) => Ok(I256::from(v)),
			Number::Float(v) => match I256::try_from(v) {
				Ok(v) => Ok(v),
				Err(e) => Err(e),
			},
			Number::Decimal(x) => match I256::try_from(x.to_i128().unwrap_or_default()) {
				Ok(x) => Ok(x),
				_ => Err(Error::TryFrom(value.to_string(), "I256")),
			},
			Number::BigInt(x) => Ok(x),
		}
	}
}

impl Display for Number {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Number::Int(v) => Display::fmt(v, f),
			Number::Float(v) => {
				if v.is_finite() {
					// Add suffix to distinguish between int and float
					write!(f, "{v}f")
				} else {
					// Don't add suffix for NaN, inf, -inf
					Display::fmt(v, f)
				}
			}
			Number::Decimal(v) => write!(f, "{v}dec"),
			// todo: convert to hex
			Number::BigInt(v) => write!(f, "{v}bigint"),
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

	pub fn is_nan(&self) -> bool {
		matches!(self, Number::Float(v) if v.is_nan())
	}

	pub fn is_int(&self) -> bool {
		matches!(self, Number::Int(_))
	}

	pub fn is_float(&self) -> bool {
		matches!(self, Number::Float(_))
	}

	pub fn is_decimal(&self) -> bool {
		matches!(self, Number::Decimal(_))
	}

	pub fn is_bigint(&self) -> bool {
		matches!(self, Number::BigInt(_))
	}

	pub fn is_integer(&self) -> bool {
		match self {
			Number::Int(_) => true,
			Number::Float(v) => v.fract() == 0.0,
			Number::Decimal(v) => v.is_integer(),
			Number::BigInt(_) => true,
		}
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Number::Int(v) => v != &0,
			Number::Float(v) => v != &0.0,
			Number::Decimal(v) => v != &Decimal::ZERO,
			Number::BigInt(v) => v.is_zero(),
		}
	}

	pub fn is_positive(&self) -> bool {
		match self {
			Number::Int(v) => v > &0,
			Number::Float(v) => v > &0.0,
			Number::Decimal(v) => v > &Decimal::ZERO,
			Number::BigInt(v) => v.is_positive(),
		}
	}

	pub fn is_negative(&self) -> bool {
		match self {
			Number::Int(v) => v < &0,
			Number::Float(v) => v < &0.0,
			Number::Decimal(v) => v < &Decimal::ZERO,
			Number::BigInt(v) => v.is_negative(),
		}
	}

	pub fn is_zero(&self) -> bool {
		match self {
			Number::Int(v) => v == &0,
			Number::Float(v) => v == &0.0,
			Number::Decimal(v) => v == &Decimal::ZERO,
			Number::BigInt(v) => v.is_zero(),
		}
	}

	pub fn is_zero_or_positive(&self) -> bool {
		match self {
			Number::Int(v) => v >= &0,
			Number::Float(v) => v >= &0.0,
			Number::Decimal(v) => v >= &Decimal::ZERO,
			Number::BigInt(v) => v.is_zero_or_positive(),
		}
	}

	pub fn is_zero_or_negative(&self) -> bool {
		match self {
			Number::Int(v) => v <= &0,
			Number::Float(v) => v <= &0.0,
			Number::Decimal(v) => v <= &Decimal::ZERO,
			Number::BigInt(v) => v.is_zero_or_negative(),
		}
	}

	// -----------------------------------
	// Simple conversion of number
	// -----------------------------------

	pub fn as_usize(self) -> usize {
		match self {
			Number::Int(v) => v as usize,
			Number::Float(v) => v as usize,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::BigInt(v) => v.to_usize().unwrap_or_default(),
		}
	}

	pub fn as_int(self) -> i64 {
		match self {
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::BigInt(v) => v.to_i64().unwrap_or_default(),
		}
	}

	pub fn as_float(self) -> f64 {
		match self {
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::BigInt(v) => v.to_f64().unwrap_or_default(),
		}
	}

	pub fn as_decimal(self) -> Decimal {
		match self {
			Number::Int(v) => Decimal::from(v),
			Number::Float(v) => Decimal::try_from(v).unwrap_or_default(),
			Number::Decimal(v) => v,
			Number::BigInt(v) => Decimal::try_from(v.to_i128().unwrap()).unwrap_or_default(),
		}
	}

	pub fn as_big(self) -> I256 {
		match self {
			Number::Int(v) => I256::from(v),
			Number::Float(v) => I256::try_from(v as i64).unwrap_or_default(),
			Number::Decimal(v) => I256::from(v.to_i128().unwrap_or_default()),
			Number::BigInt(v) => v,
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
			Number::BigInt(v) => v.to_usize().unwrap_or_default(),
		}
	}

	pub fn to_int(&self) -> i64 {
		match self {
			Number::Int(v) => *v,
			Number::Float(v) => *v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or_default(),
			Number::BigInt(v) => v.to_i64().unwrap_or_default(),
		}
	}

	pub fn to_float(&self) -> f64 {
		match self {
			Number::Int(v) => *v as f64,
			Number::Float(v) => *v,
			&Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::BigInt(v) => v.to_f64().unwrap_or_default(),
		}
	}

	pub fn to_decimal(&self) -> Decimal {
		match self {
			Number::Int(v) => Decimal::try_from(*v).unwrap_or_default(),
			Number::Float(v) => Decimal::try_from(*v).unwrap_or_default(),
			Number::Decimal(v) => *v,
			Number::BigInt(v) => {
				Decimal::try_from(v.to_u128().unwrap_or_default()).unwrap_or_default()
			}
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
			Number::BigInt(v) => v.abs().into(),
		}
	}

	pub fn acos(self) -> Self {
		self.to_float().acos().into()
	}

	pub fn ceil(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.ceil().into(),
			Number::Decimal(v) => v.ceil().into(),
			Number::BigInt(v) => v.into(),
		}
	}

	pub fn floor(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.floor().into(),
			Number::Decimal(v) => v.floor().into(),
			Number::BigInt(v) => v.into(),
		}
	}

	pub fn round(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.round().into(),
			Number::Decimal(v) => v.round().into(),
			Number::BigInt(v) => v.into(),
		}
	}

	pub fn fixed(self, precision: usize) -> Number {
		match self {
			Number::Int(v) => format!("{v:.precision$}").try_into().unwrap_or_default(),
			Number::Float(v) => format!("{v:.precision$}").try_into().unwrap_or_default(),
			Number::Decimal(v) => v.round_dp(precision as u32).into(),
			Number::BigInt(v) => format!("{v:.precision$}").try_into().unwrap_or_default(),
		}
	}

	pub fn sqrt(self) -> Self {
		match self {
			Number::Int(v) => (v as f64).sqrt().into(),
			Number::Float(v) => v.sqrt().into(),
			Number::Decimal(v) => v.sqrt().unwrap_or_default().into(),
			Number::BigInt(_) => I256::zero().into(),
		}
	}

	pub fn pow(self, power: Number) -> Number {
		match (self, power) {
			(Number::Int(v), Number::Int(p)) => Number::Int(v.pow(p as u32)),
			(Number::Decimal(v), Number::Int(p)) => v.powi(p).into(),
			// TODO: (Number::Decimal(v), Number::Float(p)) => todo!(),
			// TODO: (Number::Decimal(v), Number::Decimal(p)) => todo!(),
			(Number::Int(v), Number::BigInt(p)) => v.pow(p.to_u32().unwrap_or_default()).into(),
			(Number::BigInt(v), Number::Int(p)) => v.pow(p as u32).into(),
			(Number::BigInt(v), Number::BigInt(p)) => v.pow(p.to_u32().unwrap_or_default()).into(),
			(v, p) => v.as_float().powf(p.as_float()).into(),
		}
	}
}

impl Eq for Number {}

impl Ord for Number {
	fn cmp(&self, other: &Self) -> Ordering {
		fn total_cmp_f64(a: f64, b: f64) -> Ordering {
			if a == 0.0 && b == 0.0 {
				// -0.0 = 0.0
				Ordering::Equal
			} else {
				// Handles NaN's
				a.total_cmp(&b)
			}
		}

		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.cmp(w),
			(Number::Float(v), Number::Float(w)) => total_cmp_f64(*v, *w),
			(Number::Decimal(v), Number::Decimal(w)) => v.cmp(w),
			(Number::BigInt(v), Number::BigInt(w)) => v.cmp(*w),
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => total_cmp_f64(*v as f64, *w),
			(Number::Float(v), Number::Int(w)) => total_cmp_f64(*v, *w as f64),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).cmp(w),
			(Number::Decimal(v), Number::Int(w)) => v.cmp(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				// `rust_decimal::Decimal` code comments indicate that `to_f64` is infallible
				total_cmp_f64(*v, w.to_f64().unwrap())
			}
			(Number::Decimal(v), Number::Float(w)) => total_cmp_f64(v.to_f64().unwrap(), *w),
			// ------------------------------
			(Number::BigInt(v), Number::Int(w)) => v.cmp(I256::from(*w)),
			(Number::Int(v), Number::BigInt(w)) => I256::from(*v).cmp(*w),
			// ------------------------------
			(Number::BigInt(v), Number::Float(w)) => v.cmp(I256::from(*w as u64)),
			(Number::Float(v), Number::BigInt(w)) => I256::from(*v as u64).cmp(*w),
			// ------------------------------
			(Number::BigInt(v), Number::Decimal(w)) => {
				v.cmp(I256::from(w.to_i128().unwrap_or_default()))
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				v.to_i128().map(|v| I256::from(v).cmp(*w)).unwrap()
			}
		}
	}
}

// Warning: Equal numbers may have different hashes, which violates
// the invariants of certain collections!
impl hash::Hash for Number {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		match self {
			Number::Int(v) => v.hash(state),
			Number::Float(v) => v.to_bits().hash(state),
			Number::Decimal(v) => v.hash(state),
			Number::BigInt(v) => v.hash(state),
		}
	}
}

impl PartialEq for Number {
	fn eq(&self, other: &Self) -> bool {
		fn total_eq_f64(a: f64, b: f64) -> bool {
			a.to_bits().eq(&b.to_bits()) || (a == 0.0 && b == 0.0)
		}

		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.eq(w),
			(Number::Float(v), Number::Float(w)) => total_eq_f64(*v, *w),
			(Number::Decimal(v), Number::Decimal(w)) => v.eq(w),
			(Number::BigInt(v), Number::BigInt(w)) => v.eq(w),
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => total_eq_f64(*v as f64, *w),
			(Number::Float(v), Number::Int(w)) => total_eq_f64(*v, *w as f64),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).eq(w),
			(Number::Decimal(v), Number::Int(w)) => v.eq(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => total_eq_f64(*v, w.to_f64().unwrap()),
			(Number::Decimal(v), Number::Float(w)) => total_eq_f64(v.to_f64().unwrap(), *w),
			// ------------------------------
			(Number::BigInt(v), Number::Int(w)) => v.eq(&I256::from(*w)),
			(Number::Int(v), Number::BigInt(w)) => I256::from(*v).eq(w),
			// ------------------------------
			(Number::BigInt(v), Number::Float(w)) => v.eq(&I256::from(*w as u64)),
			(Number::Float(v), Number::BigInt(w)) => I256::from(*v as u64).eq(w),
			// ------------------------------
			(Number::BigInt(v), Number::Decimal(w)) => {
				v.eq(&I256::from(w.to_i128().unwrap_or_default()))
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				v.to_i128().map(|v| I256::from(v).eq(w)).unwrap()
			}
		}
	}
}

impl PartialOrd for Number {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

macro_rules! impl_simple_try_op {
	($trt:ident, $fn:ident, $unchecked:ident, $checked:ident) => {
		impl $trt for Number {
			type Output = Self;
			fn $fn(self, other: Self) -> Result<Self, Error> {
				Ok(match (self, other) {
					(Number::Int(v), Number::Int(w)) => Number::Int(
						v.$checked(w).ok_or_else(|| Error::$trt(v.to_string(), w.to_string()))?,
					),
					(Number::Float(v), Number::Float(w)) => Number::Float(v.$unchecked(w)),
					(Number::Decimal(v), Number::Decimal(w)) => Number::Decimal(
						v.$checked(w).ok_or_else(|| Error::$trt(v.to_string(), w.to_string()))?,
					),
					(Number::Int(v), Number::Float(w)) => Number::Float((v as f64).$unchecked(w)),
					(Number::Float(v), Number::Int(w)) => Number::Float(v.$unchecked(w as f64)),
					(v, w) => Number::Decimal(
						v.to_decimal()
							.$checked(w.to_decimal())
							.ok_or_else(|| Error::$trt(v.to_string(), w.to_string()))?,
					),
				})
			}
		}
	};
}

impl_simple_try_op!(TryAdd, try_add, add, checked_add);
impl_simple_try_op!(TrySub, try_sub, sub, checked_sub);
impl_simple_try_op!(TryMul, try_mul, mul, checked_mul);
impl_simple_try_op!(TryDiv, try_div, div, checked_div);
impl_simple_try_op!(TryRem, try_rem, rem, checked_rem);

impl TryPow for Number {
	type Output = Self;
	fn try_pow(self, power: Self) -> Result<Self, Error> {
		Ok(match (self, power) {
			(Self::Int(v), Self::Int(p)) => Self::Int(match v {
				0 => match p.cmp(&0) {
					// 0^(-x)
					Ordering::Less => return Err(Error::TryPow(v.to_string(), p.to_string())),
					// 0^0
					Ordering::Equal => 1,
					// 0^x
					Ordering::Greater => 0,
				},
				// 1^p
				1 => 1,
				-1 => {
					if p % 2 == 0 {
						// (-1)^even
						1
					} else {
						// (-1)^odd
						-1
					}
				}
				// try_into may cause an error, which would be wrong for the above cases.
				_ => p
					.try_into()
					.ok()
					.and_then(|p| v.checked_pow(p))
					.ok_or_else(|| Error::TryPow(v.to_string(), p.to_string()))?,
			}),
			(Self::Decimal(v), Self::Int(p)) => Self::Decimal(
				v.checked_powi(p).ok_or_else(|| Error::TryPow(v.to_string(), p.to_string()))?,
			),
			(Self::Decimal(v), Self::Float(p)) => Self::Decimal(
				v.checked_powf(p).ok_or_else(|| Error::TryPow(v.to_string(), p.to_string()))?,
			),
			(Self::Decimal(v), Self::Decimal(p)) => Self::Decimal(
				v.checked_powd(p).ok_or_else(|| Error::TryPow(v.to_string(), p.to_string()))?,
			),
			(v, p) => v.as_float().powf(p.as_float()).into(),
		})
	}
}

impl TryNeg for Number {
	type Output = Self;

	fn try_neg(self) -> Result<Self::Output, Error> {
		Ok(match self {
			Self::Int(n) => {
				Number::Int(n.checked_neg().ok_or_else(|| Error::TryNeg(n.to_string()))?)
			}
			Self::Float(n) => Number::Float(-n),
			Self::Decimal(n) => Number::Decimal(-n),
			Self::BigInt(n) => Number::BigInt(n.neg()),
		})
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.add(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.add(I256::from(w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(w.add(I256::from(v))),
			(Number::BigInt(v), Number::Float(w)) => Number::BigInt(v.add(I256::from(w as u64))),
			(Number::Float(v), Number::BigInt(w)) => Number::BigInt(w.add(I256::from(v as u64))),
			(Number::BigInt(v), Number::Decimal(w)) => {
				Number::BigInt(w.to_i128().map(|w| v.add(I256::from(w))).unwrap_or_default())
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				Number::BigInt(v.to_i128().map(|v| w.add(I256::from(v))).unwrap_or_default())
			}
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.add(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.add(&I256::from(*w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(w.add(&I256::from(*v))),
			(Number::BigInt(v), Number::Float(w)) => Number::BigInt(v.add(&I256::from(*w as u64))),
			(Number::Float(v), Number::BigInt(w)) => Number::BigInt(w.add(&I256::from(*v as u64))),
			(Number::BigInt(v), Number::Decimal(w)) => {
				Number::BigInt(w.to_i128().map(|w| v.add(&I256::from(w))).unwrap())
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				Number::BigInt(v.to_i128().map(|v| w.add(&I256::from(v))).unwrap())
			}
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.sub(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.sub(I256::from(w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(I256::from(v).sub(w)),
			(Number::BigInt(v), Number::Float(w)) => Number::BigInt(v.sub(I256::from(w as u64))),
			(Number::Float(v), Number::BigInt(w)) => Number::BigInt(I256::from(v as u64).sub(w)),
			(Number::BigInt(v), Number::Decimal(w)) => {
				Number::BigInt(w.to_i128().map(|w| v.sub(I256::from(w))).unwrap_or_default())
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				Number::BigInt(v.to_i128().map(|v| I256::from(v).sub(w)).unwrap_or_default())
			}
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.sub(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.sub(&I256::from(*w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(I256::from(*v).sub(*w)),
			(Number::BigInt(v), Number::Float(w)) => Number::BigInt(v.sub(&I256::from(*w as u64))),
			(Number::Float(v), Number::BigInt(w)) => Number::BigInt(I256::from(*v as u64).sub(*w)),
			(Number::BigInt(v), Number::Decimal(w)) => {
				Number::BigInt(w.to_i128().map(|w| v.sub(&I256::from(w))).unwrap())
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				Number::BigInt(v.to_i128().map(|v| I256::from(v).sub(*w)).unwrap())
			}
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.mul(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.mul(I256::from(w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(w.mul(I256::from(v))),
			(Number::BigInt(v), Number::Float(w)) => Number::BigInt(v.mul(I256::from(w as u64))),
			(Number::Float(v), Number::BigInt(w)) => Number::BigInt(w.mul(I256::from(v as u64))),
			(Number::BigInt(v), Number::Decimal(w)) => {
				Number::BigInt(w.to_i128().map(|w| v.mul(I256::from(w))).unwrap_or_default())
			}
			(Number::Decimal(v), Number::BigInt(w)) => {
				Number::BigInt(v.to_i128().map(|v| w.mul(I256::from(v))).unwrap_or_default())
			}
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.mul(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.mul(&I256::from(*w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(I256::from(*v).mul(*w)),
			// (Number::Big(v), Number::Float(w)) => Number::Big(v.mul(I256::from(*w as u64))),
			// (Number::Float(v), Number::Big(w)) => Number::Big(w.mul(I256::from(*v as u64))),
			// (Number::Big(v), Number::Decimal(w)) => Number::Big(w.to_i128().map(|w| v.mul(I256::from(w))).unwrap_or_default()),
			// (Number::Decimal(v), Number::Big(w)) => Number::Big(v.to_i128().map(|v| w.mul(I256::from(v))).unwrap_or_default()),
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.div(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.div(I256::from(w))),
			(Number::Int(v), Number::BigInt(w)) => {
				Number::BigInt(I256::from(v / w.to_i64().unwrap_or_default()))
			}
			// (Number::Big(v), Number::Float(w)) => Number::Big(v.div(I256::from(w as u64))),
			// (Number::Float(v), Number::Big(w)) => Number::Big(w.div(I256::from(v as u64))),
			// (Number::Big(v), Number::Decimal(w)) => {
			// 	Number::Big(w.to_i128().map(|w| v.div(I256::from(w))).unwrap())
			// }
			// (Number::Decimal(v), Number::Big(w)) => {
			// 	Number::Big(v.to_i128().map(|v| w.div(I256::from(v))).unwrap())
			// }
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
			(Number::BigInt(v), Number::BigInt(w)) => Number::BigInt(v.div(w)),
			(Number::BigInt(v), Number::Int(w)) => Number::BigInt(v.div(&I256::from(*w))),
			(Number::Int(v), Number::BigInt(w)) => Number::BigInt(I256::from(*v).div(*w)),

			// (Number::Big(v), Number::Float(w)) => Number::Big(v.div(I256::from(*w as u64))),
			// (Number::Float(v), Number::Big(w)) => Number::Big(w.div(I256::from(*v as u64))),
			// (Number::Big(v), Number::Decimal(w)) => {
			// 	Number::Big(w.to_i128().map(|w| v.div(I256::from(w))).unwrap_or_default())
			// }
			// (Number::Decimal(v), Number::Big(w)) => {
			// 	Number::Big(v.to_i128().map(|v| w.div(I256::from(v))).unwrap_or_default())
			// }
			(v, w) => Number::from(v.to_decimal() / w.to_decimal()),
		}
	}
}

impl Neg for Number {
	type Output = Self;

	fn neg(self) -> Self::Output {
		match self {
			Self::Int(n) => Number::Int(-n),
			Self::Float(n) => Number::Float(-n),
			Self::Decimal(n) => Number::Decimal(-n),
			Self::BigInt(n) => Number::BigInt(-n),
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
