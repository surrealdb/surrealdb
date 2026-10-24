use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::hash;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::Kind;
use crate::sql::{SqlFormat, ToSql, fmt_non_finite_f64};

/// Represents a numeric value in SurrealDB
///
/// Numbers in SurrealDB can be integers, floating-point numbers, or decimal numbers.
/// This enum provides type-safe representation for all numeric types.

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Number {
	/// A 64-bit signed integer
	Int(i64),
	/// A 64-bit floating-point number
	Float(f64),
	/// A decimal number with arbitrary precision
	Decimal(Decimal),
	/// An 8-bit signed integer
	I8(i8),
	/// An 8-bit unsigned integer
	U8(u8),
	/// A 16-bit signed integer
	I16(i16),
	/// A 16-bit unsigned integer
	U16(u16),
	/// A 32-bit signed integer
	I32(i32),
	/// A 32-bit unsigned integer
	U32(u32),
	/// A 64-bit unsigned integer
	U64(u64),
	/// A 16-bit IEEE 754 floating-point number
	#[serde(with = "serde_f16")]
	F16(#[cfg_attr(feature = "arbitrary", arbitrary(with = arbitrary_f16))] half::f16),
	/// A 32-bit IEEE 754 floating-point number
	F32(f32),
}

const _: () = assert!(std::mem::size_of::<Number>() == 24);

mod serde_f16 {
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	#[allow(clippy::trivially_copy_pass_by_ref)]
	pub fn serialize<S: Serializer>(v: &half::f16, serializer: S) -> Result<S::Ok, S::Error> {
		v.to_f32().serialize(serializer)
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<half::f16, D::Error> {
		f32::deserialize(deserializer).map(half::f16::from_f32)
	}
}

#[cfg(feature = "arbitrary")]
fn arbitrary_f16(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<half::f16> {
	<u16 as arbitrary::Arbitrary>::arbitrary(u).map(half::f16::from_bits)
}

/// A number with its width dropped onto one of the three full-width representations.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Wide {
	/// A 64-bit signed integer
	Int(i64),
	/// A 64-bit floating-point number
	Float(f64),
	/// A decimal number
	Decimal(Decimal),
}

/// The kind of a [`Number`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NumberKind {
	/// `i8`
	I8,
	/// `u8`
	U8,
	/// `i16`
	I16,
	/// `u16`
	U16,
	/// `i32`
	I32,
	/// `u32`
	U32,
	/// `int` (`i64`)
	Int,
	/// `u64`
	U64,
	/// `f16`
	F16,
	/// `f32`
	F32,
	/// `float` (`f64`)
	Float,
	/// `decimal`
	Decimal,
}

impl NumberKind {
	/// The [`Kind`] corresponding to this [`NumberKind`].
	pub const fn to_kind(self) -> Kind {
		match self {
			Self::I8 => Kind::I8,
			Self::U8 => Kind::U8,
			Self::I16 => Kind::I16,
			Self::U16 => Kind::U16,
			Self::I32 => Kind::I32,
			Self::U32 => Kind::U32,
			Self::Int => Kind::Int,
			Self::U64 => Kind::U64,
			Self::F16 => Kind::F16,
			Self::F32 => Kind::F32,
			Self::Float => Kind::Float,
			Self::Decimal => Kind::Decimal,
		}
	}

	/// The arithmetic join (`⊔`) of two number kinds.
	pub const fn join(self, other: Self) -> Self {
		use NumberKind::{Decimal, F16, F32, Float, I8, I16, I32, Int, U8, U16, U32, U64};
		match (self, other) {
			(Decimal, _) | (_, Decimal) => Decimal,
			(Float, _) | (_, Float) => Float,
			(F32, F32 | F16 | I8 | U8 | I16 | U16) | (F16 | I8 | U8 | I16 | U16, F32) => F32,
			(F32, I32 | U32 | Int | U64) | (I32 | U32 | Int | U64, F32) => Float,
			(F16, F16 | I8 | U8) | (I8 | U8, F16) => F16,
			(F16, I16 | U16) | (I16 | U16, F16) => F32,
			(F16, I32 | U32 | Int | U64) | (I32 | U32 | Int | U64, F16) => Float,
			(U64, U64 | U8 | U16 | U32) | (U8 | U16 | U32, U64) => U64,
			(U64, I8 | I16 | I32 | Int) | (I8 | I16 | I32 | Int, U64) => Decimal,
			(Int, Int | I8 | U8 | I16 | U16 | I32 | U32)
			| (I8 | U8 | I16 | U16 | I32 | U32, Int) => Int,
			(U32, I8 | I16 | I32) | (I8 | I16 | I32, U32) => Int,
			(U32, U32 | U8 | U16) | (U8 | U16, U32) => U32,
			(I32, I32 | I8 | U8 | I16 | U16) | (I8 | U8 | I16 | U16, I32) => I32,
			(U16, I8 | I16) | (I8 | I16, U16) => I32,
			(U16, U16 | U8) | (U8, U16) => U16,
			(I16, I16 | I8 | U8) | (I8 | U8, I16) => I16,
			(U8, I8) | (I8, U8) => I16,
			(U8, U8) => U8,
			(I8, I8) => I8,
		}
	}
}

impl Number {
	/// A NaN number
	pub const NAN: Self = Self::Float(f64::NAN);

	/// The value with its width dropped. Exact for every kind: sized integers become `Int`,
	/// a `u64` above `i64::MAX` becomes `Decimal`, `f16` and `f32` become the `Float` they equal.
	pub fn wide(self) -> Wide {
		match self {
			Number::Int(v) => Wide::Int(v),
			Number::Float(v) => Wide::Float(v),
			Number::Decimal(v) => Wide::Decimal(v),
			Number::I8(v) => Wide::Int(i64::from(v)),
			Number::U8(v) => Wide::Int(i64::from(v)),
			Number::I16(v) => Wide::Int(i64::from(v)),
			Number::U16(v) => Wide::Int(i64::from(v)),
			Number::I32(v) => Wide::Int(i64::from(v)),
			Number::U32(v) => Wide::Int(i64::from(v)),
			Number::U64(v) => match i64::try_from(v) {
				Ok(i) => Wide::Int(i),
				Err(_) => Wide::Decimal(Decimal::from(v)),
			},
			Number::F16(v) => Wide::Float(v.to_f64()),
			Number::F32(v) => Wide::Float(f64::from(v)),
		}
	}

	/// `self.wide()` as a [`Number`].
	pub fn widened(self) -> Number {
		match self.wide() {
			Wide::Int(v) => Number::Int(v),
			Wide::Float(v) => Number::Float(v),
			Wide::Decimal(v) => Number::Decimal(v),
		}
	}

	/// The number's kind as a [`NumberKind`].
	pub const fn number_kind(self) -> NumberKind {
		match self {
			Number::Int(_) => NumberKind::Int,
			Number::Float(_) => NumberKind::Float,
			Number::Decimal(_) => NumberKind::Decimal,
			Number::I8(_) => NumberKind::I8,
			Number::U8(_) => NumberKind::U8,
			Number::I16(_) => NumberKind::I16,
			Number::U16(_) => NumberKind::U16,
			Number::I32(_) => NumberKind::I32,
			Number::U32(_) => NumberKind::U32,
			Number::U64(_) => NumberKind::U64,
			Number::F16(_) => NumberKind::F16,
			Number::F32(_) => NumberKind::F32,
		}
	}

	/// Checks if this number is NaN.
	pub fn is_nan(&self) -> bool {
		match self {
			Number::Float(v) => v.is_nan(),
			Number::F16(v) => v.is_nan(),
			Number::F32(v) => v.is_nan(),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_) => false,
		}
	}

	/// Converts this number into an i64.
	///
	/// Returns 0 if the number cannot be converted.
	pub fn to_int(&self) -> Option<i64> {
		match self.wide() {
			Wide::Int(v) => Some(v),
			Wide::Float(v) => Some(v as i64),
			Wide::Decimal(v) => v.to_i64(),
		}
	}

	/// Converts this number into an f64.
	///
	/// Returns 0.0 if the number cannot be converted.
	pub fn to_f64(&self) -> Option<f64> {
		match self.wide() {
			Wide::Int(v) => Some(v as f64),
			Wide::Float(v) => Some(v),
			Wide::Decimal(v) => v.to_f64(),
		}
	}
}

/// Formats a `half::f16` value using the shortest decimal string that parses back to the same
/// bits, followed by `f16` (or `<f16>NaN` / `<f16>Infinity` / `<f16>-Infinity` when non-finite).
pub fn fmt_f16_sql(v: half::f16, f: &mut String) {
	if v.is_nan() {
		f.push_str("<f16>NaN");
		return;
	}
	if v.is_infinite() {
		if v.is_sign_positive() {
			f.push_str("<f16>Infinity");
		} else {
			f.push_str("<f16>-Infinity");
		}
		return;
	}
	if v.to_bits() == 0 {
		f.push_str("0f16");
		return;
	}
	if v.to_bits() == 0x8000 {
		f.push_str("-0f16");
		return;
	}
	let f64_val = v.to_f64();
	for prec in 0..=9 {
		let mut s = format!("{f64_val:.prec$}");
		if s.contains('.') {
			let trimmed_len = s.trim_end_matches('0').trim_end_matches('.').len();
			s.truncate(trimmed_len);
		}
		#[allow(clippy::disallowed_methods)]
		if <f64 as std::str::FromStr>::from_str(&s)
			.is_ok_and(|parsed| half::f16::from_f64(parsed).to_bits() == v.to_bits())
		{
			f.push_str(&s);
			f.push_str("f16");
			return;
		}
	}
	f.push_str(&v.to_f32().to_string());
	f.push_str("f16");
}

/// Formats an `f32` value followed by `f32` (or `<f32>NaN` / `<f32>Infinity` / `<f32>-Infinity`
/// when non-finite).
pub fn fmt_f32_sql(v: f32, f: &mut String) {
	if v.is_nan() {
		f.push_str("<f32>NaN");
	} else if v.is_infinite() {
		if v.is_sign_positive() {
			f.push_str("<f32>Infinity");
		} else {
			f.push_str("<f32>-Infinity");
		}
	} else {
		f.push_str(&v.to_string());
		f.push_str("f32");
	}
}

impl Display for Number {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Number::Int(v) => Display::fmt(v, f),
			Number::I8(v) => write!(f, "{v}i8"),
			Number::U8(v) => write!(f, "{v}u8"),
			Number::I16(v) => write!(f, "{v}i16"),
			Number::U16(v) => write!(f, "{v}u16"),
			Number::I32(v) => write!(f, "{v}i32"),
			Number::U32(v) => write!(f, "{v}u32"),
			Number::U64(v) => write!(f, "{v}u64"),
			Number::Float(v) => {
				match fmt_non_finite_f64(*v) {
					// Special case: Infinity, -Infinity or NaN
					Some(special) => write!(f, "{}", special),
					// Regular float: add f to distinguish between int and float
					None => write!(f, "{v}f"),
				}
			}
			Number::F16(v) => {
				let mut buf = String::new();
				fmt_f16_sql(*v, &mut buf);
				f.write_str(&buf)
			}
			Number::F32(v) => {
				let mut buf = String::new();
				fmt_f32_sql(*v, &mut buf);
				f.write_str(&buf)
			}
			Number::Decimal(v) => write!(f, "{v}dec"),
		}
	}
}

impl ToSql for Number {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Number::Int(v) => f.push_str(&v.to_string()),
			Number::I8(v) => {
				f.push_str(&v.to_string());
				f.push_str("i8");
			}
			Number::U8(v) => {
				f.push_str(&v.to_string());
				f.push_str("u8");
			}
			Number::I16(v) => {
				f.push_str(&v.to_string());
				f.push_str("i16");
			}
			Number::U16(v) => {
				f.push_str(&v.to_string());
				f.push_str("u16");
			}
			Number::I32(v) => {
				f.push_str(&v.to_string());
				f.push_str("i32");
			}
			Number::U32(v) => {
				f.push_str(&v.to_string());
				f.push_str("u32");
			}
			Number::U64(v) => {
				f.push_str(&v.to_string());
				f.push_str("u64");
			}
			Number::Float(v) => {
				match fmt_non_finite_f64(*v) {
					// Special case: Infinity, -Infinity or NaN
					Some(special) => f.push_str(special),
					// Regular float: add f to distinguish between int and float
					None => {
						f.push_str(&v.to_string());
						f.push('f');
					}
				}
			}
			Number::F16(v) => fmt_f16_sql(*v, f),
			Number::F32(v) => fmt_f32_sql(*v, f),
			Number::Decimal(v) => v.fmt_sql(f, fmt),
		}
	}
}

macro_rules! impl_number {
	($($variant:ident($type:ty) => ($is:ident, $into:ident, $from:ident),)+) => {
		impl Number {
			/// Get the kind of number
			pub fn kind(&self) -> Kind {
				match self {
					$(
						Self::$variant(_) => Kind::$variant,
					)+
				}
			}

			$(
				/// Check if this is a of the given type
				pub fn $is(&self) -> bool {
					matches!(self, Self::$variant(_))
				}

				/// Convert this number into the given type
				pub fn $into(self) -> anyhow::Result<$type> {
					if let Self::$variant(v) = self {
						Ok(v)
					} else {
						Err(anyhow::anyhow!("Expected a {} but got a {}", Kind::$variant, self.kind()))
					}
				}

				/// Create a new number from the given type
				pub fn $from(v: $type) -> Self {
					Self::$variant(v)
				}
			)+
		}
	}
}

impl_number! (
	Int(i64) => (is_int, into_int, from_int),
	Float(f64) => (is_float, into_float, from_float),
	Decimal(Decimal) => (is_decimal, into_decimal, from_decimal),
	I8(i8) => (is_i8, into_i8, from_i8),
	U8(u8) => (is_u8, into_u8, from_u8),
	I16(i16) => (is_i16, into_i16, from_i16),
	U16(u16) => (is_u16, into_u16, from_u16),
	I32(i32) => (is_i32, into_i32, from_i32),
	U32(u32) => (is_u32, into_u32, from_u32),
	U64(u64) => (is_u64, into_u64, from_u64),
	F16(half::f16) => (is_f16, into_f16, from_f16),
	F32(f32) => (is_f32, into_f32, from_f32),
);

impl Default for Number {
	fn default() -> Self {
		Self::Int(0)
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

/// Numbers of every kind order as one line, by value: an integer, a float and
/// a decimal are equal exactly when they are the same number, so `1`, `1.0f`
/// and `1.00dec` are equal and `0.1f` and `0.1dec` are not. Every NaN,
/// whatever its sign and payload, is one value above every other number,
/// `+inf` included, as the engine orders it.
impl Ord for Number {
	fn cmp(&self, other: &Self) -> Ordering {
		fn total_cmp_f64(a: f64, b: f64) -> Ordering {
			match (a.is_nan(), b.is_nan()) {
				(true, true) => Ordering::Equal,
				(true, false) => Ordering::Greater,
				(false, true) => Ordering::Less,
				// -0.0 = 0.0
				(false, false) if a == 0.0 && b == 0.0 => Ordering::Equal,
				(false, false) => a.total_cmp(&b),
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

		match (self.wide(), other.wide()) {
			(Wide::Int(v), Wide::Int(w)) => v.cmp(&w),
			(Wide::Float(v), Wide::Float(w)) => total_cmp_f64(v, w),
			(Wide::Decimal(v), Wide::Decimal(w)) => v.cmp(&w),
			// ------------------------------
			(Wide::Int(v), Wide::Float(w)) => {
				// A NaN is above every integer.
				if w.is_nan() {
					return Ordering::Less;
				}
				// If the float is not finite, we don't need to compare it to the integer.
				if !w.is_finite() {
					return greater!(w).reverse();
				}
				// Cast int to i128 to avoid saturating.
				let l = v as i128;
				// Cast the integer-part of the float to i128 to avoid saturating.
				let r = w as i128;
				// Compare both integer parts.
				match l.cmp(&r) {
					// If the integer parts are equal then we need to compare the mantissa.
					Ordering::Equal => total_cmp_f64(0.0, w.fract()),
					// If the integer parts are not equal then we already know the correct ordering.
					ordering @ (Ordering::Less | Ordering::Greater) => ordering,
				}
			}
			(Wide::Float(v), Wide::Int(w)) => Number::Int(w).cmp(&Number::Float(v)).reverse(),
			// ------------------------------
			(Wide::Int(v), Wide::Decimal(w)) => Decimal::from(v).cmp(&w),
			(Wide::Decimal(v), Wide::Int(w)) => v.cmp(&Decimal::from(w)),
			// ------------------------------
			(Wide::Float(v), Wide::Decimal(w)) => cmp_float_decimal(v, w),
			(Wide::Decimal(v), Wide::Float(w)) => cmp_float_decimal(w, v).reverse(),
		}
	}
}

/// Orders a float against a decimal by their exact values, so `0.1f`, whose
/// binary value is `0.1000000000000000055511151231257827…`, is above `0.1dec`,
/// and the order is transitive across kinds. Every NaN is above every decimal.
///
/// The comparison is integer arithmetic on the float's binary parts and the
/// decimal's mantissa and scale, with no allocation. It is also the order of
/// the engine's index keys for every float and decimal: a key rounds a float's
/// expansion only past a `u128` coefficient, within `10^-37` relatively, and
/// no decimal lies that close to such a float.
fn cmp_float_decimal(v: f64, w: Decimal) -> Ordering {
	if v.is_nan() {
		return Ordering::Greater;
	}
	let w_negative = w.mantissa() < 0;
	let m = w.mantissa().unsigned_abs();
	let v_negative = v < 0.0;
	let by_sign = |negative: bool| {
		if negative {
			Ordering::Less
		} else {
			Ordering::Greater
		}
	};
	if v.is_infinite() {
		return by_sign(v_negative);
	}
	match (v == 0.0, m == 0) {
		(true, true) => return Ordering::Equal,
		(true, false) => return by_sign(w_negative).reverse(),
		(false, true) => return by_sign(v_negative),
		(false, false) => {}
	}
	if v_negative != w_negative {
		return by_sign(v_negative);
	}
	let (a, e) = float_parts(v);
	let magnitude = cmp_magnitudes(a, e, m, w.scale());
	if v_negative {
		magnitude.reverse()
	} else {
		magnitude
	}
}

/// The magnitude of a finite, non-zero float as `a · 2^e`, with `a` odd and
/// below `2^53`.
fn float_parts(v: f64) -> (u64, i32) {
	let bits = v.to_bits();
	let biased = ((bits >> 52) & 0x7ff) as i32;
	let fraction = bits & ((1 << 52) - 1);
	let (a, e) = if biased == 0 {
		(fraction, -1074)
	} else {
		(fraction | (1 << 52), biased - 1075)
	};
	let shift = a.trailing_zeros();
	(a >> shift, e + shift as i32)
}

/// Orders `a · 2^e` against `m · 10^-s`, for `a < 2^53` and `s <= 28`, the
/// most fractional digits a decimal holds. Scaling both by `10^s` gives
/// `a · 5^s · 2^(e + s)` against `m`, and `a · 5^s < 2^53 · 5^28 < 2^119`
/// fits a `u128`.
fn cmp_magnitudes(a: u64, e: i32, m: u128, s: u32) -> Ordering {
	let x = a as u128 * 5u128.pow(s);
	let shift = e + s as i32;
	if shift >= 0 {
		cmp_shifted(x, shift.unsigned_abs(), m)
	} else {
		cmp_shifted(m, shift.unsigned_abs(), x).reverse()
	}
}

/// Orders `x · 2^k` against `y`.
fn cmp_shifted(x: u128, k: u32, y: u128) -> Ordering {
	if x == 0 {
		0.cmp(&y)
	} else if k > x.leading_zeros() {
		// `x · 2^k >= 2^128 > y`.
		Ordering::Greater
	} else {
		(x << k).cmp(&y)
	}
}

/// Equal numbers hash alike whatever their kind, with no allocation:
/// `Int(1)`, `Float(1.0)` and `Decimal(1.00)` alike, `0.0` and `-0.0` alike,
/// every NaN alike. So `a == b` implies `hash(a) == hash(b)`.
impl hash::Hash for Number {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		Canonical::of(*self).hash(state);
	}
}

/// The one form every number equal to a given one shares.
#[derive(Hash)]
enum Canonical {
	/// An integer within `i64`.
	Int(i64),
	/// `±coefficient · 10^exponent`, the coefficient holding no trailing zero:
	/// any other value a decimal can equal.
	Decimal {
		negative: bool,
		coefficient: u128,
		exponent: i32,
	},
	/// The bits of a float no integer and no decimal equals: a NaN (all as
	/// one), an infinity, a fraction finer than `10^-28`, or a magnitude of
	/// `2^128` or more.
	Float(u64),
}

impl Canonical {
	fn of(n: Number) -> Self {
		match n.wide() {
			Wide::Int(v) => Canonical::Int(v),
			Wide::Float(v) if v.is_nan() => Canonical::Float(f64::NAN.to_bits()),
			Wide::Float(v) if v.is_infinite() => Canonical::Float(v.to_bits()),
			Wide::Float(0.0) => Canonical::Int(0),
			Wide::Float(v) => {
				let negative = v < 0.0;
				let (a, e) = float_parts(v);
				if e < 0 {
					// `a` is odd, so `a · 5^k / 10^k` has exactly `k` fractional digits.
					let k = e.unsigned_abs();
					if k > 28 {
						return Canonical::Float(v.to_bits());
					}
					return Canonical::Decimal {
						negative,
						coefficient: a as u128 * 5u128.pow(k),
						exponent: e,
					};
				}
				let bits = u64::BITS - a.leading_zeros() + e.unsigned_abs();
				if bits > u128::BITS {
					return Canonical::Float(v.to_bits());
				}
				Canonical::integer(negative, (a as u128) << e)
			}
			Wide::Decimal(w) => {
				let negative = w.mantissa() < 0;
				let mut m = w.mantissa().unsigned_abs();
				let mut s = w.scale();
				while s > 0 && m % 10 == 0 {
					m /= 10;
					s -= 1;
				}
				if s == 0 {
					Canonical::integer(negative, m)
				} else {
					Canonical::Decimal {
						negative,
						coefficient: m,
						exponent: -(s as i32),
					}
				}
			}
		}
	}

	/// The integer `±magnitude`.
	fn integer(negative: bool, magnitude: u128) -> Self {
		let signed = if negative {
			0i128.checked_sub_unsigned(magnitude)
		} else {
			i128::try_from(magnitude).ok()
		};
		if let Some(v) = signed.and_then(|v| i64::try_from(v).ok()) {
			return Canonical::Int(v);
		}
		let (mut coefficient, mut exponent) = (magnitude, 0);
		while coefficient % 10 == 0 {
			coefficient /= 10;
			exponent += 1;
		}
		Canonical::Decimal {
			negative,
			coefficient,
			exponent,
		}
	}
}

impl PartialEq for Number {
	fn eq(&self, other: &Self) -> bool {
		// Every NaN is one value, as `Ord` orders it.
		fn total_eq_f64(a: f64, b: f64) -> bool {
			a.to_bits().eq(&b.to_bits()) || (a == 0.0 && b == 0.0) || (a.is_nan() && b.is_nan())
		}

		match (self.wide(), other.wide()) {
			(Wide::Int(v), Wide::Int(w)) => v.eq(&w),
			(Wide::Float(v), Wide::Float(w)) => total_eq_f64(v, w),
			(Wide::Decimal(v), Wide::Decimal(w)) => v.eq(&w),
			// ------------------------------
			(Wide::Int(v), Wide::Float(w)) => {
				Number::Int(v).cmp(&Number::Float(w)) == Ordering::Equal
			}
			(Wide::Float(v), Wide::Int(w)) => {
				Number::Float(v).cmp(&Number::Int(w)) == Ordering::Equal
			}
			// ------------------------------
			(Wide::Int(v), Wide::Decimal(w)) => Decimal::from(v).eq(&w),
			(Wide::Decimal(v), Wide::Int(w)) => v.eq(&Decimal::from(w)),
			// ------------------------------
			(Wide::Float(v), Wide::Decimal(w)) => cmp_float_decimal(v, w) == Ordering::Equal,
			(Wide::Decimal(v), Wide::Float(w)) => cmp_float_decimal(w, v) == Ordering::Equal,
		}
	}
}

impl PartialOrd for Number {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

// From implementations for numeric types
impl From<i8> for Number {
	fn from(value: i8) -> Self {
		Number::Int(value as i64)
	}
}

impl From<u8> for Number {
	fn from(value: u8) -> Self {
		Number::Int(value as i64)
	}
}

impl From<i16> for Number {
	fn from(value: i16) -> Self {
		Number::Int(value as i64)
	}
}

impl From<u16> for Number {
	fn from(value: u16) -> Self {
		Number::Int(value as i64)
	}
}

impl From<i32> for Number {
	fn from(value: i32) -> Self {
		Number::Int(value as i64)
	}
}

impl From<u32> for Number {
	fn from(value: u32) -> Self {
		Number::Int(value as i64)
	}
}

impl From<i64> for Number {
	fn from(value: i64) -> Self {
		Number::Int(value)
	}
}

impl From<u64> for Number {
	fn from(value: u64) -> Self {
		match i64::try_from(value) {
			Ok(v) => Number::Int(v),
			Err(_) => Number::Decimal(Decimal::from(value)),
		}
	}
}

impl From<isize> for Number {
	fn from(value: isize) -> Self {
		Number::Int(value as i64)
	}
}

impl From<usize> for Number {
	fn from(value: usize) -> Self {
		match i64::try_from(value) {
			Ok(v) => Number::Int(v),
			Err(_) => Number::Decimal(Decimal::from(value)),
		}
	}
}

impl From<half::f16> for Number {
	fn from(value: half::f16) -> Self {
		Number::Float(f64::from(value))
	}
}

impl From<f32> for Number {
	fn from(value: f32) -> Self {
		Number::Float(value as f64)
	}
}

impl From<f64> for Number {
	fn from(value: f64) -> Self {
		Number::Float(value)
	}
}

impl From<Decimal> for Number {
	fn from(value: Decimal) -> Self {
		Number::Decimal(value)
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;
	use std::hash::{BuildHasher, RandomState};

	use super::*;

	#[test]
	fn every_nan_is_one_value_above_every_number() {
		let hasher = RandomState::new();
		let nans = [
			Number::Float(f64::NAN),
			Number::Float(-f64::NAN),
			Number::Float(f64::from_bits(0x7ff0_0000_0000_0001)),
			Number::F32(f32::NAN),
			Number::F32(-f32::NAN),
			Number::F16(half::f16::NAN),
			Number::F16(-half::f16::NAN),
		];
		let numbers = [
			Number::Float(f64::INFINITY),
			Number::Float(f64::NEG_INFINITY),
			Number::F32(f32::INFINITY),
			Number::F16(half::f16::INFINITY),
			Number::Float(0.0),
			Number::Int(i64::MAX),
			Number::Int(i64::MIN),
			Number::U64(u64::MAX),
			Number::Decimal(Decimal::MAX),
			Number::Decimal(Decimal::MIN),
		];
		for x in nans {
			for y in nans {
				assert_eq!(x, y);
				assert_eq!(x.cmp(&y), Ordering::Equal);
				assert_eq!(hasher.hash_one(x), hasher.hash_one(y));
			}
			for n in &numbers {
				assert_eq!(x.cmp(n), Ordering::Greater, "{x:?} against {n:?}");
				assert_eq!(n.cmp(&x), Ordering::Less, "{n:?} against {x:?}");
				assert_ne!(&x, n);
			}
		}
	}

	fn dec(s: &str) -> Number {
		Number::Decimal(s.parse().unwrap())
	}

	#[test]
	fn equal_numbers_of_every_kind_hash_alike() {
		let hasher = RandomState::new();
		let groups = [
			vec![
				Number::Int(1),
				Number::I8(1),
				Number::U8(1),
				Number::I16(1),
				Number::U16(1),
				Number::I32(1),
				Number::U32(1),
				Number::U64(1),
				Number::F16(half::f16::from_f32(1.0)),
				Number::F32(1.0),
				Number::Float(1.0),
				dec("1"),
				dec("1.00"),
			],
			vec![
				Number::Float(-0.0),
				Number::F32(-0.0),
				Number::F16(half::f16::from_f32(-0.0)),
				Number::Int(0),
				Number::I8(0),
				Number::U8(0),
				Number::U64(0),
				dec("0"),
				dec("-0.000"),
			],
			vec![
				Number::Float(0.5),
				Number::F32(0.5),
				Number::F16(half::f16::from_f32(0.5)),
				dec("0.5"),
				dec("0.50"),
			],
			vec![
				Number::Float(-2.5),
				Number::F32(-2.5),
				Number::F16(half::f16::from_f32(-2.5)),
				dec("-2.5"),
				dec("-2.500"),
			],
			vec![
				Number::U64(u64::MAX),
				dec("18446744073709551615"),
				dec("18446744073709551615.00"),
			],
			vec![Number::Float(2f64.powi(70)), dec("1180591620717411303424")],
			vec![Number::Float(2f64.powi(-20)), dec("0.00000095367431640625")],
			vec![Number::Float(2f64.powi(-28)), dec("0.0000000037252902984619140625")],
			vec![Number::Float(2f64.powi(63)), Number::U64(1u64 << 63), dec("9223372036854775808")],
			vec![
				Number::Float(-(2f64.powi(63))),
				Number::Int(i64::MIN),
				dec("-9223372036854775808"),
			],
			vec![Number::Float(1e20), dec("100000000000000000000"), dec("100000000000000000000.0")],
		];
		for group in &groups {
			for a in group {
				for b in group {
					assert_eq!(a, b);
					assert_eq!(a.cmp(b), Ordering::Equal);
					assert_eq!(hasher.hash_one(a), hasher.hash_one(b), "{a:?} {b:?}");
				}
			}
		}
	}

	/// A float equals a decimal only when they are the same number, and the
	/// order is transitive across kinds.
	#[test]
	fn a_float_compares_with_a_decimal_by_its_exact_value() {
		for s in ["0.1", "0.3", "0.07", "1.1", "19.99", "-0.1"] {
			assert_ne!(Number::Float(s.parse().unwrap()), dec(s), "{s}");
		}
		assert!(Number::Float(0.1) > dec("0.1"));
		assert!(Number::Float(0.3) < dec("0.3"));
		assert!(Number::Float(-0.1) < dec("-0.1"));
		// The nearest float to `1.0000000000000001` is `1.0`: it equals one of
		// the two decimals and not the other.
		assert_eq!(Number::Float(1.0000000000000001), dec("1.0"));
		assert!(Number::Float(1.0) < dec("1.0000000000000001"));
		assert!(Number::Float(1.0) < dec("1.0000000000000000000000000001"));
		let subnormal = f64::from_bits(1);
		assert!(Number::Float(subnormal) < Number::Decimal(Decimal::new(1, 28)));
		assert!(Number::Float(subnormal) > dec("0"));
		assert!(Number::Float(-subnormal) < dec("-0"));
		assert!(Number::Float(f64::MAX) > Number::Decimal(Decimal::MAX));
		assert!(Number::Float(f64::MIN) < Number::Decimal(Decimal::MIN));
		assert!(Number::Float(f64::INFINITY) > Number::Decimal(Decimal::MAX));
		assert!(Number::Float(f64::NEG_INFINITY) < Number::Decimal(Decimal::MIN));
		assert!(Number::Float(7.922816251426434e28) > Number::Decimal(Decimal::MAX));
	}

	/// Over numbers of every kind, some of them equal across kinds: equal
	/// numbers hash alike, the order is antisymmetric and transitive, and it
	/// agrees with `f64` wherever both sides convert to a float exactly.
	#[test]
	fn ord_eq_and_hash_agree_across_kinds() {
		let hasher = RandomState::new();
		let mut state = 0x9e37_79b9_7f4a_7c15u64;
		let mut next = move || {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			state
		};
		let mut numbers = Vec::new();
		for _ in 0..300 {
			let r = next();
			let scale = (r % 29) as u32;
			let mantissa = (next() as i64 >> (r % 40)) as i128;
			let d = Decimal::from_i128_with_scale(mantissa, scale);
			numbers.push(Number::Decimal(d));
			// The float nearest the decimal, and the decimal that float is
			// exactly, where one holds it.
			if let Some(f) = d.to_f64() {
				numbers.push(Number::Float(f));
				if let Some(exact) = Decimal::from_f64_retain(f) {
					numbers.push(Number::Decimal(exact));
				}
			}
			let f = f64::from_bits(next());
			if f.is_finite() {
				numbers.push(Number::Float(f));
			}
			let f32_val = f32::from_bits(next() as u32);
			if f32_val.is_finite() {
				numbers.push(Number::F32(f32_val));
			}
			let f16_val = half::f16::from_bits(next() as u16);
			if f16_val.is_finite() {
				numbers.push(Number::F16(f16_val));
			}
			numbers.push(Number::Int(next() as i64 >> (r % 64)));
			numbers.push(Number::U64(next() >> (r % 64)));
			numbers.push(Number::I32(next() as i32));
			numbers.push(Number::U32(next() as u32));
			numbers.push(Number::I16(next() as i16));
			numbers.push(Number::U16(next() as u16));
			numbers.push(Number::I8(next() as i8));
			numbers.push(Number::U8(next() as u8));
		}
		for a in &numbers {
			for b in &numbers {
				let ab = a.cmp(b);
				assert_eq!(ab, b.cmp(a).reverse(), "{a:?} {b:?}");
				assert_eq!(a == b, ab == Ordering::Equal, "{a:?} {b:?}");
				if a == b {
					assert_eq!(hasher.hash_one(a), hasher.hash_one(b), "{a:?} {b:?}");
				}
			}
		}
		let mut sorted = numbers.clone();
		sorted.sort();
		for w in sorted.windows(2) {
			assert!(w[0] <= w[1]);
		}
		for (i, a) in sorted.iter().enumerate() {
			for b in &sorted[i..] {
				assert!(a <= b, "{a:?} {b:?}: the order is not transitive");
			}
		}
	}

	#[test]
	fn equal_zeros_hash_alike() {
		let hasher = RandomState::new();
		let (zero, negative) = (Number::Float(0.0), Number::Float(-0.0));
		assert_eq!(zero, negative);
		assert_eq!(hasher.hash_one(zero), hasher.hash_one(negative));
	}
}
