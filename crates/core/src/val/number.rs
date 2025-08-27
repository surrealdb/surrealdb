//! Numeric value type used throughout SurrealDB.
//!
//! This module defines Number, a discriminated union over Int (i64), Float (f64),
//! and Decimal (rust_decimal::Decimal), and implements arithmetic, comparison,
//! and conversions. For storage in index keys, Numbers are serialized with a
//! canonical, lexicographic encoding (via expr::decimal::DecimalLexEncoder)
//! so that byte-wise ordering matches numeric ordering and numerically-equal
//! values across variants normalize to identical bytes.
//!
//! Key points:
//! - Ordering: PartialOrd/Ord behavior aims to reflect mathematical ordering across variants; for
//!   index keys we rely on DecimalLexEncoder to preserve ordering at the byte level.
//! - Normalization in keys: 0 (Int), 0.0 (Float) and 0dec (Decimal) encode to the same byte
//!   sequence for keys, so UNIQUE indexes treat them as equal.
//! - Special float values: NaN, +∞ and −∞ are given fixed encodings that fit in the total ordering
//!   used by keys (see DecimalLexEncoder docs).
//! - Stream-friendly: the numeric encoding contains an in-band terminator and appends a 0x00 byte,
//!   allowing concatenation in composite keys without ambiguity during decoding.
use std::cmp::Ordering;
use std::f64::consts::PI;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash;
use std::iter::{Product, Sum};
use std::ops::{self, Add, Div, Mul, Neg, Rem, Sub};

use anyhow::{Result, bail};
use fastnum::D128;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::fnc::util::math::ToFloat;
use crate::val::{Strand, TryAdd, TryDiv, TryFloatDiv, TryMul, TryNeg, TryPow, TryRem, TrySub};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
#[serde(rename = "$surrealdb::private::Number")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

	/// Converts this Number to a lexicographically ordered byte buffer.
	///
	/// This serializes the Number using DecimalLexEncoder so that byte-wise
	/// comparison preserves numeric ordering. This is essential for database
	/// indexes where key bytes must sort the same way as their numeric values.
	///
	/// Ordering guarantees:
	/// - If `a < b` numerically, then `a.as_decimal_buf() < b.as_decimal_buf()` lexicographically.
	///
	/// Encoding format:
	/// - A leading class/marker byte indicates zero, finite negative, finite positive, negative
	///   infinity, positive infinity, or NaN.
	/// - Two bytes encode a biased scale for finite values.
	/// - Packed base-10 digits follow (2 digits per byte), with an in-band terminator ensured by
	///   the packing scheme; the encoder also appends a trailing 0x00 terminator byte for
	///   stream-friendly decoding.
	///
	/// Notes:
	/// - There is no extra "type marker" for Int/Float/Decimal variants; all variants are
	///   normalized through D128 for ordering.
	/// - Special float values (NaN/±∞) are mapped to fixed encodings at the extremes to preserve a
	///   total order.
	///
	/// Returns an ordered byte buffer or an error if Decimal conversion fails
	/// for Decimal variant values.
	pub(crate) fn as_decimal_buf(&self) -> Result<Vec<u8>> {
		let b = match self {
			Self::Int(v) => {
				// Convert integer to decimal for consistent encoding across all numeric types
				DecimalLexEncoder::encode(D128::from(*v))
			}
			Self::Float(v) => {
				// Convert float to decimal for lexicographic encoding
				DecimalLexEncoder::encode(D128::from_f64(*v))
			}
			Self::Decimal(v) => {
				// Direct encoding of decimal values using lexicographic encoder
				DecimalLexEncoder::encode(DecimalLexEncoder::to_d128(*v)?)
			}
		};
		Ok(b)
	}

	/// Reconstructs a Number from a lexicographically ordered byte buffer.
	///
	/// This deserializes a buffer produced by `as_decimal_buf()` using
	/// DecimalLexEncoder, recovering the numeric value. All Number variants are
	/// normalized through the same encoding, so the original variant (Int/Float/
	/// Decimal) is not preserved; only the value (and special cases like NaN/±∞)
	/// matters for ordering and equality in keys.
	///
	/// The decoder recognizes:
	/// - Zero, finite negatives, finite positives (via marker and biased scale)
	/// - Negative/positive infinity, NaN (fixed encodings)
	/// - An explicit in-band terminator added by the encoder, which ensures the mantissa decoder
	///   stops before any following data in the stream.
	///
	/// Returns the reconstructed Number or an error if the buffer is empty or
	/// cannot be decoded.
	pub(crate) fn from_decimal_buf(b: &[u8]) -> Result<Self> {
		let dec = DecimalLexEncoder::decode(b)?;
		if dec.is_finite() {
			match DecimalLexEncoder::to_decimal(dec) {
				Ok(dec) => Ok(Number::Decimal(dec)),
				Err(_) => Ok(Number::Float(dec.to_f64())),
			}
		} else if dec.is_nan() {
			Ok(Number::Float(f64::NAN))
		} else if dec.is_infinite() {
			if dec.is_negative() {
				Ok(Number::Float(f64::NEG_INFINITY))
			} else {
				Ok(Number::Float(f64::INFINITY))
			}
		} else {
			bail!(Error::Serialization(format!("Invalid decimal value: {dec}")))
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

	pub fn checked_abs(self) -> Option<Self> {
		match self {
			Number::Int(v) => v.checked_abs().map(|x| x.into()),
			Number::Float(v) => Some(v.abs().into()),
			Number::Decimal(v) => Some(v.abs().into()),
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

		// Pick the greater number depending on whether it's positive.
		macro_rules! greater {
			($f:ident) => {
				if $f.is_sign_positive() {
					Ordering::Greater
				} else {
					Ordering::Less
				}
			};
		}

		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.cmp(w),
			(Number::Float(v), Number::Float(w)) => total_cmp_f64(*v, *w),
			(Number::Decimal(v), Number::Decimal(w)) => v.cmp(w),
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => {
				// If the float is not finite, we don't need to compare it to the integer.
				if !w.is_finite() {
					return greater!(w).reverse();
				}
				// Cast int to i128 to avoid saturating.
				let l = *v as i128;
				// Cast the integer-part of the float to i128 to avoid saturating.
				let r = *w as i128;
				// Compare both integer parts.
				match l.cmp(&r) {
					// If the integer parts are equal then we need to compare the mantissa.
					Ordering::Equal => total_cmp_f64(0.0, w.fract()),
					// If the integer parts are not equal then we already know the correct ordering.
					ordering => ordering,
				}
			}
			(v @ Number::Float(_), w @ Number::Int(_)) => w.cmp(v).reverse(),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).cmp(w),
			(Number::Decimal(v), Number::Int(w)) => v.cmp(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				// Compare fractional parts of the float and decimal.
				macro_rules! compare_fractions {
					($l:ident, $r:ident) => {
						match ($l == 0.0, $r == Decimal::ZERO) {
							// If both numbers are zero, these are equal.
							(true, true) => {
								return Ordering::Equal;
							}
							// If only the float is zero, check the decimal's sign.
							(true, false) => {
								return greater!($r).reverse();
							}
							// If only the decimal is zero, check the float's sign.
							(false, true) => {
								return greater!($l);
							}
							// If neither is zero, continue checking the rest of the digits.
							(false, false) => {
								continue;
							}
						}
					};
				}
				// If the float is not finite, we don't need to compare it to the decimal
				if !v.is_finite() {
					return greater!(v);
				}
				// Cast int to i128 to avoid saturating.
				let l = *v as i128;
				// Cast the integer-part of the decimal to i128.
				let Ok(r) = i128::try_from(*w) else {
					return greater!(w).reverse();
				};
				// Compare both integer parts.
				match l.cmp(&r) {
					// If the integer parts are equal then we need to compare the fractional parts.
					Ordering::Equal => {
						// We can't compare the fractional parts of floats with decimals reliably.
						// Instead, we need to compare them as integers. To do this, we need to
						// multiply the fraction with a number large enough to move some digits
						// to the integer part of the float or decimal. The number should fit in
						// 52 bits and be able to multiply f64 fractions between -1 and 1 without
						// losing precision. Since we may need to do this repeatedly it helps if
						// the number is as big as possible to reduce the number of
						// iterations needed.
						//
						// This number is roughly 2 ^ 53 with the last digits truncated in order
						// to make sure the fraction converges to 0 every time we multiply it.
						// This is a magic number I found through my experiments so don't ask me
						// the logic behind it :) Before changing this number, please make sure
						// that the relevant tests aren't flaky after changing it.
						const SAFE_MULTIPLIER: i64 = 9_007_199_254_740_000;
						// Get the fractional part of the float.
						let mut l = v.fract();
						// Get the fractional part of the decimal.
						let mut r = w.fract();
						// Move the digits and compare them.
						// This is very generous. For example, for our tests to pass we only need
						// 3 iterations. This should be at least 6 to make sure we cover all
						// possible decimals and floats.
						for _ in 0..12 {
							l *= SAFE_MULTIPLIER as f64;
							r *= Decimal::new(SAFE_MULTIPLIER, 0);
							// Cast the integer part of the decimal to i64. The fractions are always
							// less than 1 so we know this will always be less than SAFE_MULTIPLIER.
							match r.to_i64() {
								Some(ref right) => match (l as i64).cmp(right) {
									// If the integer parts are equal, we need to check the
									// remaining fractional parts.
									Ordering::Equal => {
										// Drop the integer parts we already compared.
										l = l.fract();
										r = r.fract();
										// Compare the fractional parts and decide whether to return
										// or continue checking the next digits.
										compare_fractions!(l, r);
									}
									ordering => {
										// If the integer parts are not equal then we already know
										// the correct ordering.
										return ordering;
									}
								},
								// This is technically unreachable. Reaching this part likely
								// indicates a bug in `rust-decimal`'s `to_f64`'s
								// implementation.
								None => {
									// We will assume the decimal is bigger or smaller depending on
									// its sign.
									return greater!(w).reverse();
								}
							}
						}
						// After our iterations, if we still haven't exhausted both fractions we
						// will just treat them as equal. It should be impossible to reach
						// this point after at least 6 iterations. We could use an infinite
						// loop instead but this way we make sure the loop always exits.
						Ordering::Equal
					}
					// If the integer parts are not equal then we already know the correct ordering.
					ordering => ordering,
				}
			}
			(v @ Number::Decimal(..), w @ Number::Float(..)) => w.cmp(v).reverse(),
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
			fn $fn(self, other: Self) -> Result<Self> {
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
	fn try_pow(self, power: Self) -> Result<Self> {
		Ok(match (self, power) {
			(Self::Int(v), Self::Int(p)) => Self::Int(match v {
				0 => match p.cmp(&0) {
					// 0^(-x)
					Ordering::Less => bail!(Error::TryPow(v.to_string(), p.to_string())),
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

	fn try_neg(self) -> Result<Self::Output> {
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
	fn try_float_div(self, other: Self) -> Result<Self> {
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

impl<'b> ops::Add<&'b Number> for &Number {
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

impl<'b> ops::Sub<&'b Number> for &Number {
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

impl<'b> ops::Mul<&'b Number> for &Number {
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

impl<'b> ops::Div<&'b Number> for &Number {
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

use std::str::FromStr;

use rust_decimal::Decimal;

use crate::expr::decimal::DecimalLexEncoder;

/// A trait to extend the Decimal type with additional functionality.
pub trait DecimalExt {
	/// Converts a string to a Decimal, normalizing it in the process.
	///
	/// This method is a convenience wrapper around
	/// `rust_decimal::Decimal::from_str` which can parse a string into a
	/// Decimal and normalize it. If the value has higher precision than the
	/// Decimal type can handle, it will be rounded to the
	/// nearest representable value.
	fn from_str_normalized(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;

	/// Converts a string to a Decimal, normalizing it in the process.
	///
	/// This method is a convenience wrapper around
	/// `rust_decimal::Decimal::from_str_exact` which can parse a string into a
	/// Decimal and normalize it. If the value has higher precision than the
	/// Decimal type can handle an Underflow error will be returned.
	fn from_str_exact_normalized(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;
}

impl DecimalExt for Decimal {
	fn from_str_normalized(s: &str) -> Result<Decimal, rust_decimal::Error> {
		#[allow(clippy::disallowed_methods)]
		Ok(Decimal::from_str(s)?.normalize())
	}

	fn from_str_exact_normalized(s: &str) -> Result<Decimal, rust_decimal::Error> {
		#[allow(clippy::disallowed_methods)]
		Ok(Decimal::from_str_exact(s)?.normalize())
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;

	use ahash::HashSet;
	use rand::seq::SliceRandom;
	use rand::{Rng, thread_rng};
	use rust_decimal::Decimal;
	use rust_decimal::prelude::ToPrimitive;

	use super::*;

	#[test]
	fn test_decimal_ext_from_str_normalized() {
		let decimal = Decimal::from_str_normalized("0.0").unwrap();
		assert_eq!(decimal.to_string(), "0");
		assert_eq!(decimal.to_i64(), Some(0));
		assert_eq!(decimal.to_f64(), Some(0.0));

		let decimal = Decimal::from_str_normalized("123.456").unwrap();
		assert_eq!(decimal.to_string(), "123.456");
		assert_eq!(decimal.to_i64(), Some(123));
		assert_eq!(decimal.to_f64(), Some(123.456));

		let decimal =
			Decimal::from_str_normalized("13.5719384719384719385639856394139476937756394756")
				.unwrap();
		assert_eq!(decimal.to_string(), "13.571938471938471938563985639");
		assert_eq!(decimal.to_i64(), Some(13));
		assert_eq!(decimal.to_f64(), Some(13.571_938_471_938_472));
	}

	#[test]
	fn test_decimal_ext_from_str_exact_normalized() {
		let decimal = Decimal::from_str_exact_normalized("0.0").unwrap();
		assert_eq!(decimal.to_string(), "0");
		assert_eq!(decimal.to_i64(), Some(0));
		assert_eq!(decimal.to_f64(), Some(0.0));

		let decimal = Decimal::from_str_exact_normalized("123.456").unwrap();
		assert_eq!(decimal.to_string(), "123.456");
		assert_eq!(decimal.to_i64(), Some(123));
		assert_eq!(decimal.to_f64(), Some(123.456));

		let decimal =
			Decimal::from_str_exact_normalized("13.5719384719384719385639856394139476937756394756");
		assert!(decimal.is_err());
		let err = decimal.unwrap_err();
		assert_eq!(err.to_string(), "Number has a high precision that can not be represented.");
	}

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
		let d = Number::Decimal(
			Decimal::from_str_exact_normalized("1.0000000000000000000000000002").unwrap(),
		);
		let e = Number::Decimal(Decimal::from_str_exact_normalized("1.1").unwrap());
		let f = Number::Float(1.1f64);
		let g = Number::Float(1.5f64);
		let h = Number::Decimal(Decimal::from_str_exact_normalized("1.5").unwrap());
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
				0 => Number::Int(rng.r#gen()),
				1 => Number::Float(f64::from_bits(rng.r#gen())),
				_ => Number::Decimal(Number::Float(f64::from_bits(rng.r#gen())).as_decimal()),
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
				0 => number + Number::from(rng.r#gen::<f64>()),
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
			let b = random_permutation(a);
			let c = random_permutation(b);
			assert_consistent(a, b, c);
		}
	}

	#[test]
	fn serialised_ord_test() {
		let ordering = [
			Number::from(f64::NEG_INFINITY),
			Number::from(f64::MIN),
			Number::Int(i64::MIN),
			Number::from(-1000),
			Number::from(-100),
			Number::from(-10),
			Number::from(-1.5),
			Number::from(-1),
			Number::from(0),
			Number::from(1),
			Number::from(1.5),
			Number::from(2),
			Number::from(10),
			Number::from(100),
			Number::from(1000),
			Number::from(i64::MAX),
			Number::from(f64::MAX),
			Number::from(f64::INFINITY),
			Number::from(f64::NAN),
		];
		for window in ordering.windows(2) {
			let n1 = &window[0];
			let n2 = &window[1];
			assert!(n1 < n2, "{n1:?} < {n2:?} (before serialization)");
			let b1 = n1.as_decimal_buf().unwrap();
			let b2 = n2.as_decimal_buf().unwrap();
			assert!(b1 < b2, "{n1:?} < {n2:?} (after serialization) - {b1:?} < {b2:?}");
			let r1 = Number::from_decimal_buf(&b1).unwrap();
			let r2 = Number::from_decimal_buf(&b2).unwrap();
			assert!(r1.eq(n1), "{r1:?} = {n1:?} (after deserialization)");
			assert!(r2.eq(n2), "{r2:?} = {n2:?} (after deserialization)");
		}
	}

	#[test]
	fn serialised_test() {
		let check = |numbers: &[Number]| {
			let mut buffers = HashSet::default();
			for n1 in numbers {
				let b = n1.as_decimal_buf().unwrap();
				let n2 = Number::from_decimal_buf(&b).unwrap();
				buffers.insert(b);
				assert!(n1.eq(&n2), "{n1:?} = {n2:?} (after deserialization)");
			}
			assert_eq!(buffers.len(), 1, "{numbers:?}");
		};
		check(&[Number::Int(0), Number::Float(0.0), Number::Decimal(Decimal::ZERO)]);
		check(&[Number::Int(1), Number::Float(1.0), Number::Decimal(Decimal::ONE)]);
		check(&[Number::Int(-1), Number::Float(-1.0), Number::Decimal(Decimal::NEGATIVE_ONE)]);
		check(&[Number::Float(1.5), Number::Decimal(Decimal::from_str_normalized("1.5").unwrap())]);
	}
}
