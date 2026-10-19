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
//! - Ordering: `Ord`, `Eq` and `Hash` are [`surrealdb_types::Number`]'s, which compares numbers of
//!   every kind by exact value, so `0.1f` sits above `0.1dec`. That is the order of their key
//!   encodings once a decimal is normalised (`Decimal::normalize`), so an index read and a scan
//!   admit the same numbers, and `Hash` agrees with `Eq`.
//! - Decimal scale in keys: the encoding keeps a decimal's trailing zeros, so a computed `2.50dec`
//!   and the literal `2.5dec` encode apart though one value. A B-tree index stamped at
//!   `BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION` or later respells each decimal without them
//!   before encoding, so its keys agree with the order above.
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
use std::str::FromStr;

use anyhow::{Result, bail, ensure};
use revision::revisioned;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, fmt_non_finite_f64, write_sql};

use super::{IndexFormat, IndexIdFormat};
use crate::expr::Error;
use crate::expr::decimal::{DecimalLexEncoder, LexDecimal};
use crate::val::{TryAdd, TryDiv, TryFloatDiv, TryMul, TryNeg, TryPow, TryRem, TrySub};

#[derive(Copy, Clone, Encode, BorrowDecode)]
pub enum NumberKind {
	Int,
	Float,
	Decimal,
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug)]
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

// Only integer types that always fit losslessly into an i64 are converted
// infallibly here. Wider types are handled below so they store the value
// correctly (as a Decimal when it exceeds i64) instead of truncating.
from_prim_ints!(i8, i16, i32, i64, isize, u8, u16, u32);

// `u64`/`usize` can exceed i64::MAX but always fit within Decimal's 96-bit
// mantissa, so the conversion is lossless and infallible: store as an Int when
// it fits, otherwise as a Decimal.
impl From<u64> for Number {
	fn from(i: u64) -> Self {
		match i64::try_from(i) {
			Ok(v) => Self::Int(v),
			Err(_) => Self::Decimal(Decimal::from(i)),
		}
	}
}

impl From<usize> for Number {
	fn from(i: usize) -> Self {
		// usize is at most 64 bits wide on every supported target.
		Self::from(i as u64)
	}
}

// `i128`/`u128` can exceed Decimal's range (2^96), so the conversion is
// fallible: store as an Int when it fits in i64, otherwise as a Decimal, and
// error rather than silently truncate when the value is too large for either.
impl TryFrom<i128> for Number {
	type Error = Error;
	fn try_from(i: i128) -> Result<Self, Self::Error> {
		if let Ok(v) = i64::try_from(i) {
			Ok(Self::Int(v))
		} else if let Some(v) = Decimal::from_i128(i) {
			Ok(Self::Decimal(v))
		} else {
			Err(Error::TryFrom(i.to_string(), "Number"))
		}
	}
}

impl TryFrom<u128> for Number {
	type Error = Error;
	fn try_from(i: u128) -> Result<Self, Self::Error> {
		if let Ok(v) = i64::try_from(i) {
			Ok(Self::Int(v))
		} else if let Some(v) = Decimal::from_u128(i) {
			Ok(Self::Decimal(v))
		} else {
			Err(Error::TryFrom(i.to_string(), "Number"))
		}
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

impl From<Decimal> for Number {
	fn from(v: Decimal) -> Self {
		Self::Decimal(v)
	}
}

impl From<surrealdb_types::Number> for Number {
	fn from(v: surrealdb_types::Number) -> Self {
		match v {
			surrealdb_types::Number::Int(i) => Self::Int(i),
			surrealdb_types::Number::Float(f) => Self::Float(f),
			surrealdb_types::Number::Decimal(d) => Self::Decimal(d),
		}
	}
}

impl From<Number> for surrealdb_types::Number {
	fn from(v: Number) -> Self {
		match v {
			Number::Int(i) => Self::Int(i),
			Number::Float(f) => Self::Float(f),
			Number::Decimal(d) => Self::Decimal(d),
		}
	}
}

impl FromStr for Number {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// Attempt to parse as i64
		match s.parse::<i64>() {
			// Store it as an i64
			Ok(v) => Ok(Self::Int(v)),
			// It wasn't parsed as a i64 so parse as a float
			_ => match s.parse::<f64>() {
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
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::Float(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::Decimal(ref v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
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
				None => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
			Number::Float(v) => match Decimal::try_from(v) {
				Ok(v) => Ok(v),
				_ => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
			Number::Decimal(x) => Ok(x),
		}
	}
}

impl Display for Number {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Number::Int(v) => Display::fmt(v, f),
			Number::Float(v) => Display::fmt(v, f),
			Number::Decimal(v) => Display::fmt(v, f),
		}
	}
}

impl ToSql for Number {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Number::Int(v) => v.fmt_sql(f, sql_fmt),
			Number::Float(v) => {
				match fmt_non_finite_f64(*v) {
					// Special case: Infinity, -Infinity or NaN
					Some(special) => write_sql!(f, sql_fmt, "{}", special),
					// Regular float: add f to distinguish between int and float
					None => write_sql!(f, sql_fmt, "{v}f"),
				}
			}
			Number::Decimal(v) => v.fmt_sql(f, sql_fmt),
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

	/// Adds `other` to a running total that must stay exact.
	///
	/// Two integers whose sum does not fit an `i64` are summed as a `Decimal`
	/// rather than wrapping or failing. A float anywhere makes the total a
	/// float, whichever side holds the decimal that came of widening integers,
	/// so a total does not depend on the order its terms arrive in. Every other
	/// pairing is [`TryAdd`]. Used by aggregates that finish with a division
	/// (`math::mean`), where an intermediate total may leave `i64` range and
	/// come back.
	pub fn add_exact(self, other: Number) -> Result<Number> {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Ok(match v.checked_add(w) {
				Some(sum) => Number::Int(sum),
				None => Number::Decimal(Decimal::from(v) + Decimal::from(w)),
			}),
			(Number::Decimal(d), Number::Float(f)) | (Number::Float(f), Number::Decimal(d)) => {
				Ok(Number::Float(d.to_f64().unwrap_or(f64::NAN) + f))
			}
			(v, w) => v.try_add(w),
		}
	}

	/// Subtracts `other` from a running total kept by [`Self::add_exact`], with
	/// the same widening: two integers whose difference does not fit an `i64`
	/// are subtracted as a `Decimal`, and a float makes the result a float.
	pub fn sub_exact(self, other: Number) -> Result<Number> {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Ok(match v.checked_sub(w) {
				Some(difference) => Number::Int(difference),
				None => Number::Decimal(Decimal::from(v) - Decimal::from(w)),
			}),
			(Number::Decimal(d), Number::Float(f)) => {
				Ok(Number::Float(d.to_f64().unwrap_or(f64::NAN) - f))
			}
			(Number::Float(f), Number::Decimal(d)) => {
				Ok(Number::Float(f - d.to_f64().unwrap_or(f64::NAN)))
			}
			(v, w) => v.try_sub(w),
		}
	}

	pub fn is_float(&self) -> bool {
		matches!(self, Number::Float(_))
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Number::Int(v) => v != &0,
			Number::Float(v) => v != &0.0,
			Number::Decimal(v) => v != &Decimal::ZERO,
		}
	}

	pub fn is_zero(&self) -> bool {
		match self {
			Number::Int(v) => v == &0,
			Number::Float(v) => v == &0.0,
			Number::Decimal(v) => v == &Decimal::ZERO,
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

	/// Convert to an `i64` only when the value round-trips exactly.
	///
	/// Returns `None` for any value that does not represent an exact integer
	/// within `i64` range — including NaN, infinities, fractional values, and
	/// out-of-range magnitudes. Lossy `as` casts (which saturate or truncate
	/// silently) are deliberately avoided so callers can reject the input
	/// rather than write a wrong value.
	pub fn as_int_lossless(self) -> Option<i64> {
		match self {
			Number::Int(v) => Some(v),
			Number::Float(v) => {
				// `i64::MAX as f64` rounds up to 2^63 (the next representable
				// f64), which is *not* in i64 range. `-(i64::MIN as f64)` is
				// exactly 2^63 and is the right exclusive upper bound.
				if !v.is_finite()
					|| v.fract() != 0.0
					|| v < i64::MIN as f64
					|| v >= -(i64::MIN as f64)
				{
					return None;
				}
				Some(v as i64)
			}
			Number::Decimal(v) => {
				if v.fract().is_zero() {
					v.try_into().ok()
				} else {
					None
				}
			}
		}
	}

	/// Convert to a `usize` suitable for array indexing.
	///
	/// Returns `None` for any value that does not represent an exact
	/// non-negative integer within `usize` range — including negatives,
	/// NaN, infinities, fractional values, and out-of-range magnitudes.
	pub fn as_array_index(self) -> Option<usize> {
		match self {
			Number::Int(v) => usize::try_from(v).ok(),
			Number::Float(v) => {
				if !v.is_finite() || v < 0.0 || v.fract() != 0.0 {
					return None;
				}
				// `f64 as usize` saturates above `usize::MAX`; round-trip back
				// through f64 to reject any value that couldn't be represented
				// exactly.
				let idx = v as usize;
				(idx as f64 == v).then_some(idx)
			}
			Number::Decimal(v) => {
				if v.fract().is_zero() {
					v.to_usize()
				} else {
					None
				}
			}
		}
	}

	pub fn to_int(self) -> i64 {
		match self {
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or_default(),
		}
	}

	/// The number as an `f64`. Inlined so that per-component loops in other
	/// crates (the vector functions' norms and distances) compile it into the
	/// loop whatever link-time optimisation decides.
	#[inline]
	pub fn to_float(self) -> f64 {
		match self {
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
		}
	}

	pub fn to_decimal(self) -> Decimal {
		match self {
			Number::Int(v) => Decimal::from(v),
			Number::Float(v) => Decimal::from_f64(v).unwrap_or_default(),
			Number::Decimal(v) => v,
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
	/// - There is no extra "type marker" for Int/Float/Decimal variants; all variants are converted
	///   to a [`LexDecimal`] first, which is what orders them against each other.
	/// - Special float values (NaN/±∞) are mapped to fixed encodings at the extremes to preserve a
	///   total order.
	pub fn as_decimal_buf(&self) -> Vec<u8> {
		DecimalLexEncoder::encode(self.as_lex_decimal())
	}

	/// The value in the form the key encoding works on; see
	/// [`LexDecimal`] for how each variant converts.
	fn as_lex_decimal(&self) -> LexDecimal {
		match self {
			Self::Int(v) => LexDecimal::from_i64(*v),
			Self::Float(v) => LexDecimal::from_f64(*v),
			Self::Decimal(v) => LexDecimal::from_decimal(*v),
		}
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
	/// A finite value decodes as a `Decimal` when it is zero or `10^-6 <= |value| < 2^96`,
	/// rounded to the 28 fractional digits and 96-bit mantissa a `Decimal` holds (see
	/// [`LexDecimal::to_decimal_rounded`]), and as the nearest `f64` otherwise. Which one
	/// depends on the value's magnitude alone, and the decoded value is within rounding of the
	/// encoded one at every magnitude. NaN and the infinities decode as the `f64` they stand for.
	///
	/// Returns the reconstructed Number or an error if the buffer is empty or
	/// cannot be decoded.
	pub fn from_decimal_buf(b: &[u8]) -> Result<Self> {
		let dec = DecimalLexEncoder::decode(b)?;
		Ok(match dec.to_decimal_rounded() {
			Some(dec) => Number::Decimal(dec),
			None => Number::Float(dec.to_f64()),
		})
	}

	/// Reconstructs a Number of the given kind from a buffer produced by
	/// `as_decimal_buf()`.
	///
	/// This is the inverse of the buffer's encoding for the kind it was written
	/// with: an `Int` decodes to the same `i64`, a `Decimal` to a `Decimal` of
	/// the same value, and a `Float` to the same `f64` bit for bit (the sign of a
	/// zero and the payload of a NaN are not stored). Decoding then encoding
	/// therefore reproduces the buffer, which a record id read back out of a key
	/// relies on to address its own record.
	///
	/// A float is decoded from the decimal digits by [`LexDecimal::to_f64`].
	pub fn from_decimal_buf_kind(b: &[u8], kind: NumberKind) -> Result<Self> {
		let dec = DecimalLexEncoder::decode(b)?;
		match kind {
			NumberKind::Int => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				Ok(Number::Int(dec.to_i64_positional()?))
			}
			NumberKind::Float => Ok(Number::Float(dec.to_f64())),
			NumberKind::Decimal => {
				ensure!(dec.is_finite(), format!("Invalid decimal value: {dec}"));
				Ok(Number::Decimal(dec.to_decimal_exact()?))
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

	/// `self` held within `min..=max`, which the caller orders, NaN excluded. A decimal held
	/// between bounds that do not convert to an ordered pair of decimals is clamped as a float.
	/// Never panics: bounds that the conversion to floats leaves out of order hold `self` at the
	/// bound it passes, and a NaN `self` stays NaN.
	pub fn clamp(self, min: Self, max: Self) -> Self {
		fn clamp_f64(n: f64, low: f64, high: f64) -> f64 {
			if n < low {
				low
			} else if n > high {
				high
			} else {
				n
			}
		}
		let exact = |bound: Self| match bound {
			Number::Float(f) => Decimal::from_f64(f),
			other => Some(other.to_decimal()),
		};
		match (self, min, max) {
			(Number::Int(n), Number::Int(min), Number::Int(max)) if min <= max => {
				n.clamp(min, max).into()
			}
			(Number::Decimal(n), min, max) => match (exact(min), exact(max)) {
				(Some(low), Some(high)) if low <= high => n.clamp(low, high).into(),
				_ => {
					clamp_f64(Number::Decimal(n).to_float(), min.to_float(), max.to_float()).into()
				}
			},
			(n, min, max) => clamp_f64(n.to_float(), min.to_float(), max.to_float()).into(),
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
			Number::Int(v) => v.into(),
			// Truncate via the formatter so subnormals and very large
			// magnitudes (which would overflow a `10^precision` multiplier)
			// stay representable; non-finite f64s round-trip too.
			Number::Float(v) => format!("{v:.precision$}")
				.parse::<f64>()
				.expect("formatted f64 always parses back as f64")
				.into(),
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
}

impl Eq for Number {}

/// Compares two floats as a total order: `-0.0` equals `0.0`, and every other pair
/// (NaN included) follows [`f64::total_cmp`].
///
/// Merging the two zeros loses no transitivity, since they are adjacent in `total_cmp`.
///
/// Geometry coordinate streams use this order, where NaNs of different sign and payload stay
/// distinct. [`Number`] orders itself differently: every NaN is one value above `+inf`.
pub(crate) fn cmp_f64(a: f64, b: f64) -> Ordering {
	if a == 0.0 && b == 0.0 {
		Ordering::Equal
	} else {
		a.total_cmp(&b)
	}
}

/// Numbers order, compare and hash as [`surrealdb_types::Number`] does, so a
/// value reads the same in the engine and once returned. Every NaN, whatever its
/// sign and payload, is one value above every other number, `+inf` included;
/// numbers of different kinds are equal exactly when they are the same number,
/// and equal numbers hash alike.
///
/// That is also the order of their index keys, so an indexed range and a scan
/// admit the same numbers. A key spells a float by [`LexDecimal::from_f64`],
/// exactly except when the expansion overflows a `u128` coefficient, and then
/// to at least 38 significant digits, within `10^-37` of the float relatively.
/// A float rounded so has at least 33 fractional bits, `a · 2^-k` with `a` odd
/// and below `2^53`, and differs from any decimal `m · 10^-s` (`s <= 28`) by at
/// least `1 / (a · 5^s) > 1 / (2^53 · 5^28)`, about `3 · 10^-36` relatively, so
/// the rounding never reaches or crosses a decimal.
impl Ord for Number {
	fn cmp(&self, other: &Self) -> Ordering {
		surrealdb_types::Number::from(*self).cmp(&surrealdb_types::Number::from(*other))
	}
}

impl hash::Hash for Number {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		surrealdb_types::Number::from(*self).hash(state);
	}
}

impl PartialEq for Number {
	fn eq(&self, other: &Self) -> bool {
		surrealdb_types::Number::from(*self).eq(&surrealdb_types::Number::from(*other))
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
							.ok_or_else(|| Error::$trt(v.to_sql(), w.to_sql()))?,
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

// `+`, `-`, `*` and `/` never panic. Over two integers `+`, `-` and `*` wrap on overflow in every
// build, and a quotient an integer cannot hold (a division by zero, `MIN / -1`) is the float of the
// division; a decimal result that overflows or divides by zero is the float of the operation. The
// checked forms that raise instead are `TryAdd`, `TrySub`, `TryMul` and `TryDiv`.

/// `exact` of `v` and `w` as a decimal, or `float` of the two as floats where the decimal
/// overflows.
fn decimal_or_float(
	v: Decimal,
	w: Decimal,
	exact: fn(Decimal, Decimal) -> Option<Decimal>,
	float: fn(f64, f64) -> f64,
) -> Number {
	match exact(v, w) {
		Some(result) => Number::Decimal(result),
		None => {
			Number::Float(float(v.to_f64().unwrap_or_default(), w.to_f64().unwrap_or_default()))
		}
	}
}

impl ops::Add for Number {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v.wrapping_add(w)),
			(Number::Float(v), Number::Float(w)) => Number::Float(v + w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(v, w, Decimal::checked_add, |a, b| a + b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 + w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v + w as f64),
			(v, w) => {
				decimal_or_float(v.as_decimal(), w.as_decimal(), Decimal::checked_add, |a, b| a + b)
			}
		}
	}
}

impl<'b> ops::Add<&'b Number> for &Number {
	type Output = Number;
	fn add(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v.wrapping_add(*w)),
			(Number::Float(v), Number::Float(w)) => Number::Float(v + w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(*v, *w, Decimal::checked_add, |a, b| a + b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 + w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v + *w as f64),
			(v, w) => {
				decimal_or_float(v.to_decimal(), w.to_decimal(), Decimal::checked_add, |a, b| a + b)
			}
		}
	}
}

impl ops::Sub for Number {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v.wrapping_sub(w)),
			(Number::Float(v), Number::Float(w)) => Number::Float(v - w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(v, w, Decimal::checked_sub, |a, b| a - b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 - w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v - w as f64),
			(v, w) => {
				decimal_or_float(v.as_decimal(), w.as_decimal(), Decimal::checked_sub, |a, b| a - b)
			}
		}
	}
}

impl<'b> ops::Sub<&'b Number> for &Number {
	type Output = Number;
	fn sub(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v.wrapping_sub(*w)),
			(Number::Float(v), Number::Float(w)) => Number::Float(v - w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(*v, *w, Decimal::checked_sub, |a, b| a - b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 - w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v - *w as f64),
			(v, w) => {
				decimal_or_float(v.to_decimal(), w.to_decimal(), Decimal::checked_sub, |a, b| a - b)
			}
		}
	}
}

impl ops::Mul for Number {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v.wrapping_mul(w)),
			(Number::Float(v), Number::Float(w)) => Number::Float(v * w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(v, w, Decimal::checked_mul, |a, b| a * b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 * w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v * w as f64),
			(v, w) => {
				decimal_or_float(v.as_decimal(), w.as_decimal(), Decimal::checked_mul, |a, b| a * b)
			}
		}
	}
}

impl<'b> ops::Mul<&'b Number> for &Number {
	type Output = Number;
	fn mul(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => Number::Int(v.wrapping_mul(*w)),
			(Number::Float(v), Number::Float(w)) => Number::Float(v * w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(*v, *w, Decimal::checked_mul, |a, b| a * b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 * w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v * *w as f64),
			(v, w) => {
				decimal_or_float(v.to_decimal(), w.to_decimal(), Decimal::checked_mul, |a, b| a * b)
			}
		}
	}
}

impl ops::Div for Number {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => match v.checked_div(w) {
				Some(quotient) => Number::Int(quotient),
				None => Number::Float(v as f64 / w as f64),
			},
			(Number::Float(v), Number::Float(w)) => Number::Float(v / w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(v, w, Decimal::checked_div, |a, b| a / b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(v as f64 / w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v / w as f64),
			(v, w) => {
				decimal_or_float(v.as_decimal(), w.as_decimal(), Decimal::checked_div, |a, b| a / b)
			}
		}
	}
}

impl<'b> ops::Div<&'b Number> for &Number {
	type Output = Number;
	fn div(self, other: &'b Number) -> Number {
		match (self, other) {
			(Number::Int(v), Number::Int(w)) => match v.checked_div(*w) {
				Some(quotient) => Number::Int(quotient),
				None => Number::Float(*v as f64 / *w as f64),
			},
			(Number::Float(v), Number::Float(w)) => Number::Float(v / w),
			(Number::Decimal(v), Number::Decimal(w)) => {
				decimal_or_float(*v, *w, Decimal::checked_div, |a, b| a / b)
			}
			(Number::Int(v), Number::Float(w)) => Number::Float(*v as f64 / w),
			(Number::Float(v), Number::Int(w)) => Number::Float(v / *w as f64),
			(v, w) => {
				decimal_or_float(v.to_decimal(), w.to_decimal(), Decimal::checked_div, |a, b| a / b)
			}
		}
	}
}

impl Neg for Number {
	type Output = Self;

	/// The negation; the integer `MIN` wraps to itself, as `-` wraps in every build.
	fn neg(self) -> Self::Output {
		match self {
			Self::Int(n) => Number::Int(n.wrapping_neg()),
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
	#[inline]
	fn to_float(&self) -> f64 {
		Number::to_float(*self)
	}
}

impl Encode<()> for Number {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		let slice = self.as_decimal_buf();
		w.write_slice(&slice)?;
		let kind = match self {
			Number::Int(_) => NumberKind::Int,
			Number::Float(_) => NumberKind::Float,
			Number::Decimal(_) => NumberKind::Decimal,
		};
		Encode::<()>::encode(&kind, w)?;
		Ok(())
	}
}

impl<'de> BorrowDecode<'de, ()> for Number {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let slice = r.read_cow()?;
		let kind: NumberKind = BorrowDecode::<'de, ()>::borrow_decode(r)?;
		Number::from_decimal_buf_kind(slice.as_ref(), kind)
			.map_err(|_| storekey::DecodeError::InvalidFormat)
	}
}

impl Encode<IndexFormat> for Number {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		let slice = self.as_decimal_buf();
		w.write_slice(&slice)
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Number {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let slice = r.read_cow()?;
		Number::from_decimal_buf(slice.as_ref()).map_err(|_| storekey::DecodeError::InvalidFormat)
	}
}

/// A number inside an index key's record id is spelled as an indexed number is.
impl Encode<IndexIdFormat> for Number {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		Encode::<IndexFormat>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Number {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		BorrowDecode::<'de, IndexFormat>::borrow_decode(r)
	}
}

/// Conversion to `f64`. The implementations are marked `#[inline]` so that the
/// per-component loops of the vector functions, in other crates, compile them
/// into the loop whatever link-time optimisation decides.
pub trait ToFloat {
	fn to_float(&self) -> f64;
}

impl ToFloat for f64 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self
	}
}

impl ToFloat for f32 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for half::f16 {
	#[inline]
	fn to_float(&self) -> f64 {
		f64::from(*self)
	}
}

impl ToFloat for i64 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i32 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i16 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i8 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for u8 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;

	use ahash::HashSet;
	use common::decimal::DecimalExt;
	use rand::Rng;
	use rand::seq::SliceRandom;
	use rust_decimal::Decimal;
	use rust_decimal::prelude::ToPrimitive;

	use super::*;

	/// Whether two numbers agree to `digits` significant digits, to within one
	/// unit of the last. Both are truncated to the same unit, so the comparison
	/// is exact integer arithmetic whatever the magnitudes. Zero agrees only with
	/// zero.
	fn agree(a: LexDecimal, b: LexDecimal, digits: u32) -> bool {
		let (
			LexDecimal::Finite {
				negative: na,
				coefficient: ca,
				exponent: ea,
			},
			LexDecimal::Finite {
				negative: nb,
				coefficient: cb,
				exponent: eb,
			},
		) = (a, b)
		else {
			return false;
		};
		if ca == 0 || cb == 0 {
			return ca == cb;
		}
		if na != nb {
			return false;
		}
		let lead = |c: u128, e: i32| e + c.ilog10() as i32 + 1;
		// The exponent of one unit in the last of `digits` digits, measured from the
		// larger of the two leading digits.
		let unit = lead(ca, ea).max(lead(cb, eb)) - digits as i32;
		let scaled = |c: u128, e: i32| {
			let shift = e - unit;
			if shift >= 0 {
				c * 10u128.pow(shift as u32)
			} else {
				10u128.checked_pow((-shift) as u32).map_or(0, |p| c / p)
			}
		};
		scaled(ca, ea).abs_diff(scaled(cb, eb)) <= 1
	}

	/// Whether a decoded number agrees with the value a key was written from.
	fn agrees(original: LexDecimal, decoded: &Number, digits: u32) -> bool {
		agree(original, decoded.as_lex_decimal(), digits)
	}

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
		let a = Number::Float(-f64::INFINITY);
		let b = Number::Float(-1f64);
		let c = Number::Float(1f64);
		let d = Number::Decimal(
			Decimal::from_str_normalized("1.0000000000000000000000000002").unwrap(),
		);
		let e = Number::Decimal(Decimal::from_str_normalized("1.1").unwrap());
		let f = Number::Float(1.1f64);
		let g = Number::Float(1.5f64);
		let h = Number::Decimal(Decimal::from_str_normalized("1.5").unwrap());
		let i = Number::Float(f64::INFINITY);
		let j = Number::Float(f64::NAN);
		let k = Number::Float(-f64::NAN);
		let original = vec![a, b, c, d, e, f, g, h, i, j, k];
		let mut copy = original.clone();
		let mut rng = rand::rng();
		copy.shuffle(&mut rng);
		copy.sort();
		assert_eq!(original, copy);
	}

	#[test]
	fn every_nan_is_one_value_above_every_number() {
		let nans = [f64::NAN, -f64::NAN, f64::from_bits(0x7ff0_0000_0000_0001)];
		let numbers = [
			Number::Float(f64::INFINITY),
			Number::Float(-f64::INFINITY),
			Number::Float(0.0),
			Number::Int(i64::MAX),
			Number::Int(i64::MIN),
			Number::Decimal(Decimal::MAX),
			Number::Decimal(Decimal::MIN),
		];
		for x in nans {
			let x = Number::Float(x);
			for y in nans {
				assert_eq!(x, Number::Float(y));
				assert_eq!(x.cmp(&Number::Float(y)), Ordering::Equal);
			}
			for n in &numbers {
				assert_eq!(x.cmp(n), Ordering::Greater, "{x:?} against {n:?}");
				assert_eq!(n.cmp(&x), Ordering::Less, "{n:?} against {x:?}");
				assert_ne!(&x, n);
			}
			assert_eq!(x.as_decimal_buf(), Number::Float(f64::NAN).as_decimal_buf());
		}
	}

	#[test]
	fn ord_fuzz() {
		fn random_number() -> Number {
			let mut rng = rand::rng();
			match rng.random_range(0..3) {
				0 => Number::Int(rng.random()),
				1 => Number::Float(f64::from_bits(rng.random())),
				_ => Number::Decimal(Number::Float(f64::from_bits(rng.random())).as_decimal()),
			}
		}

		fn random_permutation(number: Number) -> Number {
			let mut rng = rand::rng();
			let value = match rng.random_range(0..4) {
				0 => number + Number::from(rng.random::<f64>()),
				1 if !matches!(number, Number::Int(i64::MIN)) => number * Number::from(-1),
				2 => Number::Float(number.as_float().next_down()),
				_ => number,
			};
			match rng.random_range(0..3) {
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

	/// A float equals a decimal only when the decimal spells the float's exact
	/// value, so `0.1f`, whose key spelling is
	/// `0.100000000000000005551115123125782702118`, is above `0.1dec`.
	#[test]
	fn a_float_compares_with_a_decimal_by_its_exact_expansion() {
		let dec = |s: &str| Number::Decimal(Decimal::from_str_normalized(s).unwrap());
		for s in ["0.1", "0.3", "0.07", "1.1", "19.99", "0.11111"] {
			let f = Number::Float(s.parse().unwrap());
			assert_ne!(f, dec(s), "{s}");
		}
		assert!(Number::Float(0.1) > dec("0.1"));
		assert!(Number::Float(0.3) < dec("0.3"));
		assert_eq!(Number::Float(2f64.powi(-20)), dec("0.00000095367431640625"));
		for s in ["0", "0.5", "1", "1.5", "-2.25", "1024"] {
			let f = Number::Float(s.parse().unwrap());
			assert_eq!(f, dec(s), "{s}");
		}
		assert_eq!(Number::Float(-0.0), dec("0"));
		assert!(Number::Float(1.0) < dec("1.0000000000000000000000000001"));
		assert!(Number::Float(f64::INFINITY) > dec("79228162514264337593543950335"));
		assert!(Number::Float(f64::NEG_INFINITY) < dec("-79228162514264337593543950335"));
		// A decimal's trailing zeros, and the bounds of an `i64`, change nothing.
		let mut padded = Decimal::from_str_normalized("2.5").unwrap();
		padded.rescale(3);
		assert_eq!(Number::Float(2.5), Number::Decimal(padded));
		assert_eq!(Number::Float(2f64.powi(63)), dec("9223372036854775808"));
		assert_eq!(Number::Float(-(2f64.powi(63))), Number::Int(i64::MIN));
		// A subnormal float lies between zero and the smallest decimals.
		let subnormal = f64::from_bits(1);
		assert!(Number::Float(subnormal) < Number::Decimal(Decimal::new(1, 28)));
		assert!(Number::Float(subnormal) > dec("0"));
		assert!(Number::Float(-subnormal) > Number::Decimal(Decimal::new(-1, 28)));
	}

	/// A float and a decimal order as their index keys whatever the float's
	/// expansion, including a float whose key rounds it: no decimal lies within
	/// that rounding of it.
	#[test]
	fn a_float_and_a_decimal_order_as_their_keys_whatever_the_expansion() {
		fn key(n: Number) -> Vec<u8> {
			match n {
				Number::Decimal(d) => Number::Decimal(d.normalize()).as_decimal_buf(),
				n => n.as_decimal_buf(),
			}
		}
		let mut rng = rand::rng();
		let mut floats =
			vec![0.1, 0.3, 1.0 / 3.0, 2f64.powi(-33), 2f64.powi(-60), f64::from_bits(1)];
		let mut decimals =
			vec![Decimal::new(1, 1), Decimal::new(1, 28), Decimal::MAX, Decimal::MIN];
		for _ in 0..2_000 {
			let scale = rng.random_range(0..=28);
			let mantissa = rng.random::<i64>() as i128 >> rng.random_range(0..40);
			let d = Decimal::from_i128_with_scale(mantissa, scale);
			decimals.push(d);
			if let Some(f) = d.to_f64() {
				// The float nearest the decimal, and its neighbours.
				floats.extend([
					f,
					f64::from_bits(f.to_bits() + 1),
					f64::from_bits(f.to_bits() - 1),
				]);
			}
			let f = f64::from_bits(rng.random());
			if f.is_finite() {
				floats.push(f);
			}
		}
		// A float with a binary fraction finer than `2^-33` is one a key can round.
		let fine = floats.iter().filter(|f| (**f * 2f64.powi(33)).fract() != 0.0).count();
		assert!(fine > 1_000, "the sample holds floats whose keys round: {fine}");
		for &f in &floats {
			let (f, kf) = (Number::Float(f), key(Number::Float(f)));
			for &d in &decimals {
				let d = Number::Decimal(d);
				assert_eq!(f.cmp(&d), kf.cmp(&key(d)), "{f:?} {d:?}");
			}
		}
	}

	/// `Ord` orders every pair of numbers as their key bytes do, and numbers that
	/// compare equal hash alike, whatever their kinds.
	#[test]
	fn ord_and_hash_agree_with_the_key_encoding() {
		fn hash(n: &impl std::hash::Hash) -> u64 {
			use std::hash::{BuildHasher, BuildHasherDefault};
			BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default().hash_one(n)
		}
		fn kinds_of(n: Number) -> [Number; 3] {
			[Number::Int(n.as_int()), Number::Float(n.as_float()), Number::Decimal(n.to_decimal())]
		}
		/// `a` and `b` order as the key bytes of their normalised forms, and hash
		/// alike when equal.
		fn agree((a, ka): &(Number, Vec<u8>), (b, kb): &(Number, Vec<u8>)) {
			assert_eq!(a.cmp(b), ka.cmp(kb), "{a:?} {b:?}");
			if a == b {
				assert_eq!(hash(a), hash(b), "{a:?} {b:?}");
			}
		}
		let mut rng = rand::rng();
		let numbers = vec![
			Number::Float(f64::NAN),
			Number::Float(f64::INFINITY),
			Number::Float(f64::NEG_INFINITY),
			Number::Float(-0.0),
			Number::Float(0.0),
			Number::Float(0.1),
			Number::Float(-1.0),
			Number::Float(f64::MIN_POSITIVE),
			Number::Float(f64::MAX),
			Number::Int(i64::MAX),
			Number::Int(i64::MIN),
			Number::Float(2f64.powi(53)),
			Number::Int(9_007_199_254_740_993),
			Number::Float(2f64.powi(63)),
			Number::Float(-(2f64.powi(63))),
			Number::Float(f64::from_bits(1)),
			Number::Float(-f64::from_bits(1)),
		];
		// Every kind of every special value, checked against every other.
		// Each kind, and a decimal with trailing zeros that compares equal to it.
		let keyed = |ns: &[Number]| -> Vec<(Number, Vec<u8>)> {
			ns.iter()
				.flat_map(|n| kinds_of(*n))
				.flat_map(|n| match n {
					Number::Decimal(d) => {
						let mut padded = d;
						padded.rescale(d.scale() + 2);
						vec![n, Number::Decimal(padded)]
					}
					n => vec![n],
				})
				.map(|n| {
					let key = match n {
						Number::Decimal(d) => Number::Decimal(d.normalize()).as_decimal_buf(),
						n => n.as_decimal_buf(),
					};
					(n, key)
				})
				.collect()
		};
		let specials = keyed(&numbers);
		for a in &specials {
			for b in &specials {
				agree(a, b);
			}
		}
		// Random values next to their neighbours and to decimals near them.
		let mut randoms = Vec::new();
		for _ in 0..300 {
			let f = match rng.random_range(0..3) {
				0 => f64::from_bits(rng.random()),
				1 => rng.random_range(-1e6..1e6),
				_ => rng.random_range(-1.0..1.0),
			};
			if f.is_finite() {
				randoms.push(Number::Float(f));
				randoms.push(Number::Float(f.next_up()));
				randoms.push(Number::Decimal(Number::Float(f).as_decimal()));
			}
		}
		let mut keyed = keyed(&randoms);
		keyed.extend(specials);
		keyed.shuffle(&mut rng);
		for pair in keyed.chunks(2).filter(|c| c.len() == 2).chain(keyed.windows(2)) {
			agree(&pair[0], &pair[1]);
		}
		keyed.sort_by_key(|a| a.0);
		for pair in keyed.windows(2) {
			agree(&pair[0], &pair[1]);
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
			let b1 = n1.as_decimal_buf();
			let b2 = n2.as_decimal_buf();
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
				let b = n1.as_decimal_buf();
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

	/// A number as an index key stores it, and as a read of that key decodes it.
	fn through_index_key(n: &Number) -> Number {
		let key = storekey::encode_vec_format::<IndexFormat, _>(n).unwrap();
		storekey::decode_borrow_format::<IndexFormat, Number>(&key)
			.unwrap_or_else(|e| panic!("the index key of {n:?} does not decode: {e}"))
	}

	fn decimal(text: &str) -> Decimal {
		text.parse().unwrap()
	}

	/// The decoded number is a `Decimal` of exactly this mantissa and scale.
	#[track_caller]
	fn assert_decimal(decoded: Number, mantissa: i128, scale: u32) {
		match decoded {
			Number::Decimal(d) => assert_eq!((d.mantissa(), d.scale()), (mantissa, scale), "{d}"),
			other => panic!("decoded as {other:?}, not the decimal {mantissa}e-{scale}"),
		}
	}

	#[test]
	fn an_index_key_decodes_a_float_below_a_millionth_to_the_float_it_was_written_from() {
		for f in [
			1e-20,
			5.6527459748369866e-8,
			1e-7,
			9.99e-7,
			1e-28,
			1e-29,
			1e-300,
			f64::MIN_POSITIVE,
			5e-324,
		] {
			for f in [f, -f] {
				match through_index_key(&Number::Float(f)) {
					Number::Float(g) => {
						assert_eq!(g.to_bits(), f.to_bits(), "{f:e} decoded as {g:e}")
					}
					other => panic!("{f:e} decoded as {other:?}, not a float"),
				}
			}
		}
	}

	#[test]
	fn an_index_key_decodes_a_float_to_the_decimal_it_has_always_decoded_to() {
		// 0.1 is 0.1000000000000000055511151231257827…, of which a Decimal holds 28 places.
		assert_decimal(through_index_key(&Number::Float(0.1)), 1000000000000000055511151231, 28);
		assert_decimal(through_index_key(&Number::Float(-0.1)), -1000000000000000055511151231, 28);
		assert_decimal(through_index_key(&Number::Float(1.5)), 15, 1);
		assert_decimal(through_index_key(&Number::Float(0.0)), 0, 0);
		assert_decimal(through_index_key(&Number::Float(1e28)), 9999999999999999583119736832, 0);
		// Beyond a Decimal's 96 bits the value is a float.
		for f in [1e30, -1e30, 1e300, f64::MAX, f64::MIN] {
			assert!(
				matches!(through_index_key(&Number::Float(f)), Number::Float(g) if g == f),
				"{f:e}"
			);
		}
		for f in [f64::INFINITY, f64::NEG_INFINITY] {
			assert!(matches!(through_index_key(&Number::Float(f)), Number::Float(g) if g == f));
		}
		assert!(
			matches!(through_index_key(&Number::Float(f64::NAN)), Number::Float(g) if g.is_nan())
		);
	}

	#[test]
	fn an_index_key_decodes_a_number_to_a_decimal_only_between_a_millionth_and_2_pow_96() {
		// A Decimal's own range reads back as itself, scale included.
		for text in [
			"0.000001",
			"-0.000001",
			"0.0000010000000000000000000001",
			"1.50",
			"100.000",
			"79228162514264337593543950335",
			"-79228162514264337593543950335",
			"7.9228162514264337593543950335",
		] {
			let d = decimal(text);
			assert_decimal(through_index_key(&Number::Decimal(d)), d.mantissa(), d.scale());
		}
		// A magnitude below a millionth reads back as the nearest float.
		for (text, float) in [
			("0.0000001", "1e-7"),
			("-0.0000001", "-1e-7"),
			("0.0000009999999999999999999999", "9.999999999999999999999e-7"),
			("0.0000000000000000000000000001", "1e-28"),
		] {
			let nearest: f64 = float.parse().unwrap();
			match through_index_key(&Number::Decimal(decimal(text))) {
				Number::Float(g) => assert_eq!(g.to_bits(), nearest.to_bits(), "{text}: {g:e}"),
				other => panic!("{text} decoded as {other:?}, not a float"),
			}
		}
		// A float is on one side or the other of 2^96 = 79228162514264337593543950336.
		assert!(matches!(through_index_key(&Number::Float(2f64.powi(96))), Number::Float(_)));
		assert!(matches!(
			through_index_key(&Number::Float(2f64.powi(96) * (1.0 - f64::EPSILON))),
			Number::Decimal(_)
		));
	}

	#[test]
	fn an_index_key_decodes_an_int_to_the_decimal_of_that_value() {
		for i in [0, 1, -1, 7, i64::MAX, i64::MIN, 1_000_000, -1_000_000_007] {
			assert_decimal(through_index_key(&Number::Int(i)), i128::from(i), 0);
		}
	}

	/// Whatever a number is written as, an index key reads it back as a number of
	/// about the same value, never one an exponent away.
	#[test]
	fn an_index_key_decodes_to_the_value_it_was_written_from_up_to_rounding() {
		use rand::SeedableRng;
		use rand::rngs::StdRng;

		let mut rng = StdRng::seed_from_u64(0x1DE7);

		// Every f64 bit pattern, so every magnitude and every digit string.
		for _ in 0..60_000 {
			let f = f64::from_bits(rng.random());
			if f.is_nan() {
				continue;
			}
			let original = LexDecimal::from_f64(f);
			let decoded = through_index_key(&Number::Float(f));
			match decoded {
				// A float is the one it was written from.
				Number::Float(g) if f.is_finite() => {
					assert_eq!(g.to_bits(), f.to_bits(), "{f:e} decoded as {g:e}")
				}
				Number::Float(g) => assert_eq!(g, f, "{f:e}"),
				// A Decimal keeps 28 fractional digits, so a value of at least a
				// millionth keeps at least 22 significant ones.
				Number::Decimal(_) => {
					assert!(agrees(original, &decoded, 22), "{f:e} decoded as {decoded:?}");
				}
				Number::Int(_) => panic!("{f:e} decoded as an int"),
			}
		}

		// Floats with a short mantissa at every power of ten, where the digit
		// string is exact rather than a 39-digit expansion.
		for e in -324..=308 {
			for m in ["1", "9.999999999999999", "1.2345678901234567", "5", "7.1"] {
				let Ok(f) = format!("{m}e{e}").parse::<f64>() else {
					continue;
				};
				// A mantissa at the top of the range rounds up past `f64::MAX`.
				if !f.is_finite() {
					continue;
				}
				for f in [f, -f] {
					let original = LexDecimal::from_f64(f);
					let decoded = through_index_key(&Number::Float(f));
					assert!(agrees(original, &decoded, 22), "{f:e} decoded as {decoded:?}");
				}
			}
		}

		// Every Decimal: those in range come back as themselves, and the rest as a
		// float of the same value.
		for _ in 0..60_000 {
			let mantissa = (rng.random::<u128>() >> rng.random_range(32..128)) as i128;
			let mantissa = if rng.random() {
				-mantissa
			} else {
				mantissa
			};
			let d = Decimal::from_i128_with_scale(mantissa, rng.random_range(0..=28));
			let decoded = through_index_key(&Number::Decimal(d));
			if d.is_zero() || d.abs() >= decimal("0.000001") {
				match decoded {
					Number::Decimal(g) if d.is_zero() => assert!(g.is_zero(), "{d} decoded as {g}"),
					Number::Decimal(g) => {
						assert_eq!(g.serialize(), d.serialize(), "{d} decoded as {g}")
					}
					other => panic!("{d} decoded as {other:?}"),
				}
			} else {
				match decoded {
					Number::Float(g) => {
						let original = LexDecimal::from_decimal(d);
						assert!(agrees(original, &decoded, 15), "{d} decoded as {g:e}");
					}
					other => panic!("{d} decoded as {other:?}, not a float"),
				}
			}
		}

		// Every i64.
		for _ in 0..20_000 {
			let i = rng.random::<i64>() >> rng.random_range(0..64);
			assert_decimal(through_index_key(&Number::Int(i)), i128::from(i), 0);
		}
	}

	/// A decoded float that a Decimal parser reads positionally is the Decimal it
	/// reads, to the last digit and the scale, so a decoded value does not move.
	#[test]
	fn an_index_key_decodes_a_float_to_the_decimal_its_text_parses_to() {
		use rand::SeedableRng;
		use rand::rngs::StdRng;

		let mut rng = StdRng::seed_from_u64(0x7E47);
		let mut checked = 0;
		for _ in 0..60_000 {
			// Magnitudes of a millionth up to 2^96, written positionally.
			let f = rng.random::<f64>() * 10f64.powi(rng.random_range(-6..=28));
			let f = if rng.random() {
				-f
			} else {
				f
			};
			let text = LexDecimal::from_f64(f).to_string();
			if text.contains(['e', 'E']) {
				continue;
			}
			let Ok(parsed) = Decimal::from_str_radix(&text, 10) else {
				continue;
			};
			checked += 1;
			match through_index_key(&Number::Float(f)) {
				Number::Decimal(d) => assert_eq!(d.serialize(), parsed.serialize(), "{text}: {d}"),
				other => panic!("{f:e} decoded as {other:?}, not the decimal {parsed}"),
			}
		}
		assert!(checked > 20_000, "only {checked} positional floats were generated");
	}

	/// Reading a Decimal from the text of the decoded digits, and a float where that
	/// text is not one, is a decoder of its own for the values it reads right. The
	/// decoder under test agrees with it wherever it is right, in value and in kind,
	/// so a value is decoded differently only where the text read gets it wrong: each
	/// such case is checked, and none is required, since the text read may get the
	/// whole sample right.
	#[test]
	fn an_index_key_decodes_every_value_a_text_read_gets_right_as_it_does() {
		use rand::SeedableRng;
		use rand::rngs::StdRng;

		fn read_from_text(key: &[u8]) -> Number {
			let d128 = DecimalLexEncoder::decode(key).unwrap();
			match Decimal::from_str_radix(&d128.to_string(), 10) {
				Ok(d) => Number::Decimal(d),
				Err(_) => Number::Float(d128.to_f64()),
			}
		}

		let mut rng = StdRng::seed_from_u64(0x7E47_0002);
		let mut numbers = Vec::new();
		for _ in 0..60_000 {
			numbers.push(Number::Float(f64::from_bits(rng.random())));
			numbers
				.push(Number::Float(rng.random::<f64>() * 10f64.powi(rng.random_range(-40..40))));
			let mantissa = (rng.random::<u128>() >> rng.random_range(32..128)) as i128;
			numbers.push(Number::Decimal(Decimal::from_i128_with_scale(
				if rng.random() {
					-mantissa
				} else {
					mantissa
				},
				rng.random_range(0..=28),
			)));
			numbers.push(Number::Int(rng.random::<i64>() >> rng.random_range(0..64)));
		}
		for n in numbers {
			let key = n.as_decimal_buf();
			let (decoded, read) = (Number::from_decimal_buf(&key).unwrap(), read_from_text(&key));
			let same = match (&decoded, &read) {
				(Number::Decimal(a), Number::Decimal(b)) => a.serialize() == b.serialize(),
				(Number::Float(a), Number::Float(b)) => a.to_bits() == b.to_bits() || a == b,
				_ => false,
			};
			if same {
				continue;
			}
			let written = DecimalLexEncoder::decode(&key).unwrap();
			assert!(
				written.is_finite()
					&& !matches!(
						written,
						LexDecimal::Finite {
							coefficient: 0,
							..
						}
					) && !agrees(written, &read, 9)
					&& agrees(written, &decoded, 15),
				"{n:?} decoded as {decoded:?}, which differs from {read:?}, read from text"
			);
		}
	}

	#[test]
	fn as_int_lossless_accepts_valid() {
		assert_eq!(Number::Int(0).as_int_lossless(), Some(0));
		assert_eq!(Number::Int(i64::MIN).as_int_lossless(), Some(i64::MIN));
		assert_eq!(Number::Int(i64::MAX).as_int_lossless(), Some(i64::MAX));
		assert_eq!(Number::Float(0.0).as_int_lossless(), Some(0));
		assert_eq!(Number::Float(-0.0).as_int_lossless(), Some(0));
		assert_eq!(Number::Float(7.0).as_int_lossless(), Some(7));
		// `i64::MIN` is exactly representable as `f64` and is the inclusive lower bound.
		assert_eq!(Number::Float(i64::MIN as f64).as_int_lossless(), Some(i64::MIN));
		assert_eq!(Number::Decimal(Decimal::ZERO).as_int_lossless(), Some(0));
		assert_eq!(Number::Decimal(Decimal::ONE).as_int_lossless(), Some(1));
		assert_eq!(Number::Decimal(Decimal::NEGATIVE_ONE).as_int_lossless(), Some(-1));
	}

	#[test]
	fn as_int_lossless_rejects_invalid() {
		assert_eq!(Number::Float(1.5).as_int_lossless(), None);
		assert_eq!(Number::Float(-1.5).as_int_lossless(), None);
		assert_eq!(Number::Float(f64::NAN).as_int_lossless(), None);
		assert_eq!(Number::Float(f64::INFINITY).as_int_lossless(), None);
		assert_eq!(Number::Float(f64::NEG_INFINITY).as_int_lossless(), None);
		// `i64::MAX as f64` rounds up to 2^63, which is *not* in `i64` range —
		// the exclusive upper bound is `-(i64::MIN as f64)`.
		assert_eq!(Number::Float(i64::MAX as f64).as_int_lossless(), None);
		assert_eq!(Number::Float(1e30).as_int_lossless(), None);
		assert_eq!(Number::Float(-1e30).as_int_lossless(), None);
		assert_eq!(
			Number::Decimal(Decimal::from_str_normalized("1.5").unwrap()).as_int_lossless(),
			None,
		);
	}

	#[test]
	fn as_array_index_accepts_valid() {
		assert_eq!(Number::Int(0).as_array_index(), Some(0));
		assert_eq!(Number::Int(7).as_array_index(), Some(7));
		assert_eq!(Number::Float(0.0).as_array_index(), Some(0));
		assert_eq!(Number::Float(-0.0).as_array_index(), Some(0));
		assert_eq!(Number::Float(3.0).as_array_index(), Some(3));
		assert_eq!(Number::Decimal(Decimal::ZERO).as_array_index(), Some(0));
		assert_eq!(Number::Decimal(Decimal::ONE).as_array_index(), Some(1));
	}

	#[test]
	fn as_array_index_rejects_invalid() {
		assert_eq!(Number::Int(-1).as_array_index(), None);
		assert_eq!(Number::Int(i64::MIN).as_array_index(), None);
		assert_eq!(Number::Float(-1.0).as_array_index(), None);
		assert_eq!(Number::Float(1.5).as_array_index(), None);
		assert_eq!(Number::Float(f64::NAN).as_array_index(), None);
		assert_eq!(Number::Float(f64::INFINITY).as_array_index(), None);
		assert_eq!(Number::Float(f64::NEG_INFINITY).as_array_index(), None);
		// 1e30 is far beyond usize::MAX and not exactly representable as usize.
		assert_eq!(Number::Float(1e30).as_array_index(), None);
		assert_eq!(Number::Decimal(Decimal::NEGATIVE_ONE).as_array_index(), None);
		assert_eq!(
			Number::Decimal(Decimal::from_str_normalized("1.5").unwrap()).as_array_index(),
			None,
		);
	}

	#[test]
	fn fixed_int_is_unchanged_regardless_of_precision() {
		assert_eq!(Number::Int(101).fixed(0), Number::Int(101));
		assert_eq!(Number::Int(101).fixed(2), Number::Int(101));
		assert_eq!(Number::Int(101).fixed(319), Number::Int(101));
		assert_eq!(Number::Int(-7).fixed(5), Number::Int(-7));
	}

	#[test]
	fn fixed_float_rounds_to_requested_precision() {
		assert_eq!(Number::Float(101.5).fixed(2), Number::Float(101.5));
		assert_eq!(Number::Float(101.1111111).fixed(2), Number::Float(101.11));
		// Subnormal at precision past f64's significant-digit range: the
		// trailing digits get truncated, producing a smaller-but-finite
		// neighbour rather than 0, inf, or NaN.
		assert_eq!(
			Number::Float(2.2250738585072014e-308).fixed(319),
			Number::Float(2.22507385851e-308),
		);
	}

	#[test]
	fn fixed_float_preserves_non_finite() {
		// The Float arm relies on `format!`/`parse` round-tripping every
		// f64 — including NaN and ±inf. Pin that invariant here so a future
		// change to the formatter cannot silently break the `expect` in
		// `Number::fixed`.
		let Number::Float(nan) = Number::Float(f64::NAN).fixed(2) else {
			panic!("expected Number::Float(NaN)");
		};
		assert!(nan.is_nan());
		assert_eq!(Number::Float(f64::INFINITY).fixed(2), Number::Float(f64::INFINITY));
		assert_eq!(Number::Float(f64::NEG_INFINITY).fixed(2), Number::Float(f64::NEG_INFINITY));
	}

	#[test]
	fn fixed_decimal_uses_round_dp() {
		let d = Decimal::from_str_normalized("1.2345").unwrap();
		assert_eq!(
			Number::Decimal(d).fixed(2),
			Number::Decimal(Decimal::from_str_normalized("1.23").unwrap()),
		);
	}

	/// A number as a record key stores it, and as a scan reads it back.
	fn through_record_key(n: &Number) -> (Vec<u8>, Number) {
		let key = storekey::encode_vec(n).unwrap();
		let back = storekey::decode_borrow::<Number>(&key)
			.unwrap_or_else(|e| panic!("the key of {n:?} does not decode: {e}"));
		(key, back)
	}

	/// A record id read out of a key is used to fetch the record again, which
	/// re-encodes it, so decoding must give back the value the key was written
	/// from: the same float bit for bit, not a neighbour of it, and not an error.
	fn assert_float_key_round_trips(f: f64) {
		let (key, back) = through_record_key(&Number::Float(f));
		let Number::Float(g) = back else {
			panic!("{f:e} decoded as {back:?}, not a float");
		};
		// The sign of a zero and the payload of a NaN are not stored.
		let same =
			g.to_bits() == f.to_bits() || (f == 0.0 && g == 0.0) || (f.is_nan() && g.is_nan());
		assert!(same, "{f:e} decoded as {g:e}");
		assert_eq!(
			through_record_key(&Number::Float(g)).0,
			key,
			"{f:e} does not re-encode to its key"
		);
	}

	#[test]
	fn a_float_key_decodes_to_the_float_it_was_written_from() {
		for f in [
			0.0,
			-0.0,
			0.1,
			0.2 + 0.1,
			1.5,
			5.0,
			1.6330027767028172,
			std::f64::consts::PI,
			9007199254740994.0,
			f64::EPSILON,
			f64::INFINITY,
			f64::NEG_INFINITY,
			f64::NAN,
			// Past the largest magnitude a `Decimal` holds, ~7.9e28.
			1e28,
			7.9e28,
			1e29,
			1e30,
			-1e30,
			1e300,
			f64::MAX,
			f64::MIN,
			// Below the smallest scale a `Decimal` holds, and the subnormals.
			1e-6,
			1e-7,
			5.6527459748369866e-8,
			1e-20,
			1e-28,
			1e-29,
			1e-300,
			f64::MIN_POSITIVE,
			5e-324,
		] {
			assert_float_key_round_trips(f);
		}
		// Every decimal exponent, over mantissas of every length.
		for e in -324..=308 {
			for m in ["1", "9.999999999999999", "1.2345678901234567", "5", "7.1"] {
				if let Ok(f) = format!("{m}e{e}").parse::<f64>() {
					assert_float_key_round_trips(f);
					assert_float_key_round_trips(-f);
				}
			}
		}
		let mut rng = rand::rng();
		for _ in 0..20_000 {
			assert_float_key_round_trips(f64::from_bits(rng.random()));
			assert_float_key_round_trips(rng.random::<f64>());
			assert_float_key_round_trips(
				rng.random::<f64>() * 10f64.powi(rng.random_range(-30..30)),
			);
		}
	}

	#[test]
	fn a_decimal_key_decodes_to_the_decimal_it_was_written_from() {
		fn assert_round_trips(d: Decimal) {
			let (key, back) = through_record_key(&Number::Decimal(d));
			assert!(matches!(back, Number::Decimal(g) if g == d), "{d} decoded as {back:?}");
			assert_eq!(through_record_key(&back).0, key, "{d} does not re-encode to its key");
		}
		for d in [
			"0",
			"1",
			"-1",
			"1.5",
			"1.50",
			"100.000",
			"1000000",
			// A scale of 7 or more is printed in scientific notation by `LexDecimal`.
			"0.0000001",
			"-0.0000000121",
			"0.0000000000000000000000000001",
			"7.9228162514264337593543950335",
			"79228162514264337593543950335",
			"-79228162514264337593543950335",
		] {
			assert_round_trips(d.parse().unwrap());
		}
		assert_round_trips(Decimal::MAX);
		assert_round_trips(Decimal::MIN);
		let mut rng = rand::rng();
		for _ in 0..20_000 {
			let mantissa = (rng.random::<u128>() >> rng.random_range(32..128)) as i128;
			let mantissa = if rng.random() {
				-mantissa
			} else {
				mantissa
			};
			assert_round_trips(Decimal::from_i128_with_scale(mantissa, rng.random_range(0..=28)));
		}
	}

	#[test]
	fn an_int_key_decodes_to_the_int_it_was_written_from() {
		let mut ints = vec![0, 1, -1, i64::MAX, i64::MIN];
		for e in 0..19 {
			let p = 10i64.pow(e);
			ints.extend([p, -p, p - 1, p + 1, 5 * p]);
		}
		for i in ints {
			let (key, back) = through_record_key(&Number::Int(i));
			assert!(matches!(back, Number::Int(g) if g == i), "{i} decoded as {back:?}");
			assert_eq!(through_record_key(&back).0, key);
		}
	}

	/// What a released binary stored for these numbers. The bytes are the format
	/// of every record id and key that holds one, so they must not change: a
	/// scan on a newer release decodes exactly these.
	#[test]
	fn the_stored_bytes_of_a_numeric_key_are_frozen() {
		fn hex(b: &[u8]) -> String {
			b.iter().map(|b| format!("{b:02x}")).collect()
		}
		for (n, stored) in [
			(Number::Float(1e30), "a01937211111111111111112a995735949767001000003"),
			(Number::Float(0.1), "a01918211111111111111116662226234236893813229001000003"),
			(Number::Float(5e-324), "a017d35a51767569523576552876798a3979332483480f01000003"),
			(Number::Decimal("0.0000001".parse().unwrap()), "a019122001000004"),
			(Number::Int(1000), "a0191c21110f01000002"),
		] {
			let key = storekey::encode_vec(&n).unwrap();
			assert_eq!(hex(&key), stored, "{n:?}");
		}
	}
}
