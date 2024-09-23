use super::value::{TryAdd, TryDiv, TryFloatDiv, TryMul, TryNeg, TryPow, TryRem, TrySub};
use crate::err::Error;
use crate::fnc::util::math::ToFloat;
use crate::sql::strand::Strand;
use crate::sql::Value;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::f64::consts::PI;
use std::fmt::Debug;
use std::fmt::{self, Display, Formatter};
use std::hash;
use std::iter::Product;
use std::iter::Sum;
use std::ops::{self, Add, Div, Mul, Neg, Rem, Sub};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Number";

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
#[serde(rename = "$surrealdb::private::sql::Number")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Number {
	Int(i64),
	Float(f64),
	Decimal(Decimal),
	// Add new variants here
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
				_ => Err(()),
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
		}
	}
}

impl TryFrom<&Number> for f64 {
	type Error = Error;

	fn try_from(n: &Number) -> Result<Self, Self::Error> {
		Ok(n.to_float())
	}
}

impl TryFrom<&Number> for f32 {
	type Error = Error;

	fn try_from(n: &Number) -> Result<Self, Self::Error> {
		n.to_float().to_f32().ok_or_else(|| Error::ConvertTo {
			from: Value::Number(*n),
			into: "f32".to_string(),
		})
	}
}

impl TryFrom<&Number> for i64 {
	type Error = Error;

	fn try_from(n: &Number) -> Result<Self, Self::Error> {
		Ok(n.to_int())
	}
}
impl TryFrom<&Number> for i32 {
	type Error = Error;

	fn try_from(n: &Number) -> Result<Self, Self::Error> {
		n.to_int().to_i32().ok_or_else(|| Error::ConvertTo {
			from: Value::Number(*n),
			into: "i32".to_string(),
		})
	}
}

impl TryFrom<&Number> for i16 {
	type Error = Error;

	fn try_from(n: &Number) -> Result<Self, Self::Error> {
		n.to_int().to_i16().ok_or_else(|| Error::ConvertTo {
			from: Value::Number(*n),
			into: "i16".to_string(),
		})
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

	pub fn is_integer(&self) -> bool {
		match self {
			Number::Int(_) => true,
			Number::Float(v) => v.fract() == 0.0,
			Number::Decimal(v) => v.is_integer(),
		}
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Number::Int(v) => v != &0,
			Number::Float(v) => v != &0.0,
			Number::Decimal(v) => v != &Decimal::ZERO,
		}
	}

	pub fn is_positive(&self) -> bool {
		match self {
			Number::Int(v) => v > &0,
			Number::Float(v) => v > &0.0,
			Number::Decimal(v) => v > &Decimal::ZERO,
		}
	}

	pub fn is_negative(&self) -> bool {
		match self {
			Number::Int(v) => v < &0,
			Number::Float(v) => v < &0.0,
			Number::Decimal(v) => v < &Decimal::ZERO,
		}
	}

	pub fn is_zero(&self) -> bool {
		match self {
			Number::Int(v) => v == &0,
			Number::Float(v) => v == &0.0,
			Number::Decimal(v) => v == &Decimal::ZERO,
		}
	}

	pub fn is_zero_or_positive(&self) -> bool {
		match self {
			Number::Int(v) => v >= &0,
			Number::Float(v) => v >= &0.0,
			Number::Decimal(v) => v >= &Decimal::ZERO,
		}
	}

	pub fn is_zero_or_negative(&self) -> bool {
		match self {
			Number::Int(v) => v <= &0,
			Number::Float(v) => v <= &0.0,
			Number::Decimal(v) => v <= &Decimal::ZERO,
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
		}
	}

	pub fn as_int(self) -> i64 {
		match self {
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
		}
	}

	pub fn as_float(self) -> f64 {
		match self {
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
		}
	}

	pub fn as_decimal(self) -> Decimal {
		match self {
			Number::Int(v) => Decimal::from(v),
			Number::Float(v) => Decimal::try_from(v).unwrap_or_default(),
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
			&Number::Decimal(v) => v.try_into().unwrap_or_default(),
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

	pub fn acos(self) -> Self {
		self.to_float().acos().into()
	}

	pub fn asin(self) -> Self {
		self.to_float().asin().into()
	}

	pub fn atan(self) -> Self {
		self.to_float().atan().into()
	}

	pub fn acot(self) -> Self {
		(PI / 2.0 - self.atan().to_float()).into()
	}

	pub fn ceil(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.ceil().into(),
			Number::Decimal(v) => v.ceil().into(),
		}
	}

	pub fn clamp(self, min: Self, max: Self) -> Self {
		match (self, min, max) {
			(Number::Int(n), Number::Int(min), Number::Int(max)) => n.clamp(min, max).into(),
			(Number::Decimal(n), min, max) => n.clamp(min.to_decimal(), max.to_decimal()).into(),
			(Number::Float(n), min, max) => n.clamp(min.to_float(), max.to_float()).into(),
			(Number::Int(n), min, max) => n.to_float().clamp(min.to_float(), max.to_float()).into(),
		}
	}

	pub fn cos(self) -> Self {
		self.to_float().cos().into()
	}

	pub fn cot(self) -> Self {
		(1.0 / self.to_float().tan()).into()
	}

	pub fn deg2rad(self) -> Self {
		self.to_float().to_radians().into()
	}

	pub fn floor(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.floor().into(),
			Number::Decimal(v) => v.floor().into(),
		}
	}

	fn lerp_f64(from: f64, to: f64, factor: f64) -> f64 {
		from + factor * (to - from)
	}

	fn lerp_decimal(from: Decimal, to: Decimal, factor: Decimal) -> Decimal {
		from + factor * (to - from)
	}

	pub fn lerp(self, from: Self, to: Self) -> Self {
		match (self, from, to) {
			(Number::Decimal(val), from, to) => {
				Self::lerp_decimal(from.to_decimal(), to.to_decimal(), val).into()
			}
			(val, from, to) => {
				Self::lerp_f64(from.to_float(), to.to_float(), val.to_float()).into()
			}
		}
	}

	fn repeat_f64(t: f64, m: f64) -> f64 {
		(t - (t / m).floor() * m).clamp(0.0, m)
	}

	fn repeat_decimal(t: Decimal, m: Decimal) -> Decimal {
		(t - (t / m).floor() * m).clamp(Decimal::ZERO, m)
	}

	pub fn lerp_angle(self, from: Self, to: Self) -> Self {
		match (self, from, to) {
			(Number::Decimal(val), from, to) => {
				let from = from.to_decimal();
				let to = to.to_decimal();
				let mut dt = Self::repeat_decimal(to - from, Decimal::from(360));
				if dt > Decimal::from(180) {
					dt = Decimal::from(360) - dt;
				}
				Self::lerp_decimal(from, from + dt, val).into()
			}
			(val, from, to) => {
				let val = val.to_float();
				let from = from.to_float();
				let to = to.to_float();
				let mut dt = Self::repeat_f64(to - from, 360.0);
				if dt > 180.0 {
					dt = 360.0 - dt;
				}
				Self::lerp_f64(from, from + dt, val).into()
			}
		}
	}

	pub fn ln(self) -> Self {
		self.to_float().ln().into()
	}

	pub fn log(self, base: Self) -> Self {
		self.to_float().log(base.to_float()).into()
	}

	pub fn log2(self) -> Self {
		self.to_float().log2().into()
	}

	pub fn log10(self) -> Self {
		self.to_float().log10().into()
	}

	pub fn rad2deg(self) -> Self {
		self.to_float().to_degrees().into()
	}

	pub fn round(self) -> Self {
		match self {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.round().into(),
			Number::Decimal(v) => v.round().into(),
		}
	}

	pub fn fixed(self, precision: usize) -> Number {
		match self {
			Number::Int(v) => format!("{v:.precision$}").try_into().unwrap_or_default(),
			Number::Float(v) => format!("{v:.precision$}").try_into().unwrap_or_default(),
			Number::Decimal(v) => v.round_dp(precision as u32).into(),
		}
	}

	pub fn sign(self) -> Self {
		match self {
			Number::Int(n) => n.signum().into(),
			Number::Float(n) => n.signum().into(),
			Number::Decimal(n) => n.signum().into(),
		}
	}

	pub fn sin(self) -> Self {
		self.to_float().sin().into()
	}

	pub fn tan(self) -> Self {
		self.to_float().tan().into()
	}

	pub fn sqrt(self) -> Self {
		match self {
			Number::Int(v) => (v as f64).sqrt().into(),
			Number::Float(v) => v.sqrt().into(),
			Number::Decimal(v) => v.sqrt().unwrap_or_default().into(),
		}
	}

	pub fn pow(self, power: Number) -> Number {
		match (self, power) {
			(Number::Int(v), Number::Int(p)) => Number::Int(v.pow(p as u32)),
			(Number::Decimal(v), Number::Int(p)) => v.powi(p).into(),
			// TODO: (Number::Decimal(v), Number::Float(p)) => todo!(),
			// TODO: (Number::Decimal(v), Number::Decimal(p)) => todo!(),
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
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => {
				let l = *v as i128;
				let r = *w as i128;
				match l.cmp(&r) {
					Ordering::Equal => f64::default().total_cmp(&w.fract()),
					ordering => ordering,
				}
			}
			(v @ Number::Float(_), w @ Number::Int(_)) => w.cmp(v).reverse(),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).cmp(w),
			(Number::Decimal(v), Number::Int(w)) => v.cmp(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				if v > &Decimal::MAX
					.to_f64()
					.expect("Decimal::MAX should be able to be converted to f64")
				{
					Ordering::Greater
				} else if v < &Decimal::MIN
					.to_f64()
					.expect("Decimal::MIN should be able to be converted to f64")
				{
					Ordering::Less
				} else if let Some(vd) = Decimal::from_f64_retain(*v) {
					match (vd.cmp(w), v.fract() == 0.0, w.fract() == Decimal::ZERO) {
						// both non-integers so we order float first
						(Ordering::Equal, false, false) => Ordering::Less,
						// both have equal int parts, but w has larger magnitude
						(Ordering::Equal, false, true) => match v.is_sign_positive() {
							true => Ordering::Greater,
							false => Ordering::Less,
						},
						(Ordering::Equal, true, false) => match v.is_sign_positive() {
							true => Ordering::Less,
							false => Ordering::Greater,
						},
						// Both are integers and equal
						(Ordering::Equal, true, true) => Ordering::Equal,
						(o @ Ordering::Less | o @ Ordering::Greater, _, _) => o,
					}
				} else if v.is_sign_positive() {
					Ordering::Greater // inf, +NaN, pos overflow
				} else {
					Ordering::Less // -inf, -NaN, neg overflow
				}
			}
			(Number::Decimal(v), Number::Float(w)) => {
				Number::cmp(&Number::Float(*w), &Number::Decimal(*v)).reverse()
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
			// ------------------------------
			(v @ Number::Int(_), w @ Number::Float(_)) => v.cmp(w) == Ordering::Equal,
			(v @ Number::Float(_), w @ Number::Int(_)) => v.cmp(w) == Ordering::Equal,
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).eq(w),
			(Number::Decimal(v), Number::Int(w)) => v.eq(&Decimal::from(*w)),
			// ------------------------------
			(v @ Number::Float(_), w @ Number::Decimal(_)) => v.cmp(w) == Ordering::Equal,
			(v @ Number::Decimal(_), w @ Number::Float(_)) => v.cmp(w) == Ordering::Equal,
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
		})
	}
}

impl TryFloatDiv for Number {
	type Output = Self;
	fn try_float_div(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Number::Int(v), Number::Int(w)) => {
				let quotient = (v as f64).div(w as f64);
				if quotient.fract() != 0.0 {
					return Ok(Number::Float(quotient));
				}
				Number::Int(
					v.checked_div(w).ok_or_else(|| Error::TryDiv(v.to_string(), w.to_string()))?,
				)
			}
			(v, w) => v.try_div(w)?,
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

impl Neg for Number {
	type Output = Self;

	fn neg(self) -> Self::Output {
		match self {
			Self::Int(n) => Number::Int(-n),
			Self::Float(n) => Number::Float(-n),
			Self::Decimal(n) => Number::Decimal(-n),
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

#[non_exhaustive]
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

impl ToFloat for Number {
	fn to_float(&self) -> f64 {
		self.to_float()
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;

	use rand::seq::SliceRandom;
	use rand::thread_rng;
	use rand::Rng;
	use rust_decimal::Decimal;

	use super::Number;
	use super::TryFloatDiv;
	#[test]
	fn test_try_float_div() {
		let (sum_one, count_one) = (Number::Int(5), Number::Int(2));
		assert_eq!(sum_one.try_float_div(count_one).unwrap(), Number::Float(2.5));
		// i64::MIN

		let (sum_two, count_two) = (Number::Int(10), Number::Int(5));
		assert_eq!(sum_two.try_float_div(count_two).unwrap(), Number::Int(2));

		let (sum_three, count_three) = (Number::Float(6.3), Number::Int(3));
		assert_eq!(sum_three.try_float_div(count_three).unwrap(), Number::Float(2.1));
	}

	#[test]
	fn ord_test() {
		let a = Number::Float(-f64::NAN);
		let b = Number::Float(-f64::INFINITY);
		let c = Number::Float(1f64);
		let d = Number::Decimal(Decimal::from_str_exact("1.0000000000000000000000000002").unwrap());
		let e = Number::Decimal(Decimal::from_str_exact("1.1").unwrap());
		let f = Number::Float(1.1f64);
		let g = Number::Float(1.5f64);
		let h = Number::Decimal(Decimal::from_str_exact("1.5").unwrap());
		let i = Number::Float(f64::INFINITY);
		let j = Number::Float(f64::NAN);
		let original = vec![a, b, c, d, e, f, g, h, i, j];
		let mut copy = original.clone();
		let mut rng = thread_rng();
		copy.shuffle(&mut rng);
		copy.sort();
		assert_eq!(original, copy);
	}

	#[test]
	fn ord_fuzz() {
		fn random_number() -> Number {
			let mut rng = thread_rng();
			match rng.gen_range(0..3) {
				0 => Number::Int(rng.gen()),
				1 => Number::Float(f64::from_bits(rng.gen())),
				_ => Number::Decimal(Number::Float(f64::from_bits(rng.gen())).as_decimal()),
			}
		}

		// TODO: Use std library once stable https://doc.rust-lang.org/std/primitive.f64.html#method.next_down
		fn next_down(n: f64) -> f64 {
			const TINY_BITS: u64 = 0x1; // Smallest positive f64.
			const CLEAR_SIGN_MASK: u64 = 0x7fff_ffff_ffff_ffff;

			let bits = n.to_bits();
			if n.is_nan() || bits == f64::INFINITY.to_bits() {
				return n;
			}

			let abs = bits & CLEAR_SIGN_MASK;
			let next_bits = if abs == 0 {
				TINY_BITS
			} else if bits == abs {
				bits + 1
			} else {
				bits - 1
			};
			f64::from_bits(next_bits)
		}

		fn random_permutation(number: Number) -> Number {
			let mut rng = thread_rng();
			let value = match rng.gen_range(0..4) {
				0 => number + Number::from(rng.gen::<f64>()),
				1 if !matches!(number, Number::Int(i64::MIN)) => number * Number::from(-1),
				2 => Number::Float(next_down(number.as_float())),
				_ => number,
			};
			match rng.gen_range(0..3) {
				0 => Number::Int(value.as_int()),
				1 => Number::Float(value.as_float()),
				_ => Number::Decimal(value.as_decimal()),
			}
		}

		fn assert_partial_ord(x: Number, y: Number) {
			// PartialOrd requirements
			assert_eq!(x == y, x.partial_cmp(&y) == Some(Ordering::Equal), "{x:?} {y:?}");

			// Ord consistent with PartialOrd
			assert_eq!(x.partial_cmp(&y), Some(x.cmp(&y)), "{x:?} {y:?}");
		}

		fn assert_consistent(a: Number, b: Number, c: Number) {
			assert_partial_ord(a, b);
			assert_partial_ord(b, c);
			assert_partial_ord(c, a);

			// Transitive property (without the fix, these can fail)
			if a == b && b == c {
				assert_eq!(a, c, "{a:?} {b:?} {c:?}");
			}
			if a != b && b == c {
				assert_ne!(a, c, "{a:?} {b:?} {c:?}");
			}
			if a < b && b < c {
				assert!(a < c, "{a:?} {b:?} {c:?}");
			}
			if a > b && b > c {
				assert!(a > c, "{a:?} {b:?} {c:?}");
			}

			// Duality
			assert_eq!(a == b, b == a, "{a:?} {b:?}");
			assert_eq!(a < b, b > a, "{a:?} {b:?}");
		}

		for _ in 0..100000 {
			let base = random_number();
			let a = random_permutation(base);
			let b = random_permutation(a.clone());
			let c = random_permutation(b.clone());
			assert_consistent(a, b, c);
		}
	}
}
