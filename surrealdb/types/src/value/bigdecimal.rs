//! Arbitrary-precision decimal numbers.
//!
//! [`BigDecimal`] stores a value as `coefficient * 10^(-scale)`. The coefficient is an
//! arbitrary-precision integer and the scale is never negative. The scale is kept as written,
//! so `1.50` has scale 2 and displays as `1.50`. `Eq`, `Ord` and `Hash` compare numeric values,
//! so `1.50 == 1.5` and both hash alike.
//!
//! # Exponents and limits
//!
//! Text is parsed with at most [`MAX_DIGITS`] significant digits and an exponent of magnitude at
//! most [`MAX_EXPONENT`]. An exponent is folded into the coefficient, so `1.5e3` is stored as
//! `1500` with scale 0. This matches PostgreSQL's `numeric`, whose display scale is never
//! negative. Parsed and rounded values have scale at most [`MAX_SCALE`]. Checked arithmetic
//! returns `None` when a product would exceed that bound.
//!
//! # Ordered encoding
//!
//! [`BigDecimal::encode_ordered`] writes a self-delimiting byte string whose lexicographic
//! order is the numeric order. The layout is:
//!
//! - one sign byte: `0x01` for negative, `0x02` for zero, `0x03` for positive. Zero has no further
//!   bytes;
//! - for a nonzero value, the adjusted exponent `E` as 8 big-endian bytes with the top bit flipped.
//!   `E` is the power of ten of the leading digit, so `1.5` has `E = 0`;
//! - one byte per significant decimal digit of the canonical coefficient (no trailing zero digits).
//!   Digit `d` is stored as `d + 1`, so the digit bytes are `1..=10`;
//! - a terminator `0x00`.
//!
//! For negative values every byte after the sign byte is complemented (`0xFF - b`). This
//! reverses their order, and the complemented terminator `0xFF` sorts above every complemented
//! digit. Positive values put the terminator below every digit, so a shorter digit string sorts
//! first. Numerically equal values have one canonical form and therefore one encoding.
//!
//! [`BigDecimal::decode_ordered`] reads one encoding and returns the value with its trailing
//! zeros removed, together with the number of bytes consumed. It never panics on arbitrary input.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Neg;
use std::str::FromStr;
use std::sync::OnceLock;

use num_bigint::{BigInt, Sign};
use num_traits::{One, Signed, ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Maximum number of significant digits accepted by [`FromStr`].
///
/// This is PostgreSQL's `numeric` limit: 131072 digits before the decimal point plus 16383 after.
pub const MAX_DIGITS: usize = 131_072 + 16_383;

/// Largest scale a value can have, and largest scale a checked operation may produce.
///
/// Twice [`MAX_DIGITS`]. Two operand scales can be added in an `i64` without overflow, and the
/// positional display of a value stays within a few hundred thousand characters.
pub const MAX_SCALE: i64 = 2 * MAX_DIGITS as i64;

/// Largest magnitude of an exponent accepted in text such as `1.5e-300`.
pub const MAX_EXPONENT: i64 = MAX_DIGITS as i64;

/// Largest magnitude of the adjusted exponent that [`BigDecimal::decode_ordered`] accepts.
///
/// Encodings whose adjusted exponent is larger are rejected rather than materialised.
pub const MAX_ENCODED_EXPONENT: i64 = MAX_SCALE;

/// Largest magnitude, in bytes, that [`BigDecimal::from_le_parts`] accepts for a coefficient.
///
/// Parsed values hold at most [`MAX_DIGITS`] significant digits, and a product of two of them
/// holds at most twice that. A coefficient of `n` digits needs at most `n / 2 + 1` bytes, since
/// `log2(10) < 4`, so this bound admits every such product.
pub const MAX_COEFFICIENT_BYTES: usize = 2 * MAX_DIGITS;

/// PostgreSQL limit on the display scale (`dscale`) of a `numeric`.
const PG_MAX_DSCALE: u16 = 16_383;
/// PostgreSQL limit on the number of digits before the decimal point.
const PG_MAX_INTEGER_DIGITS: usize = 131_072;
/// PostgreSQL sign word of a non-negative `numeric`.
const PG_POSITIVE: u16 = 0x0000;
/// PostgreSQL sign word of a negative `numeric`.
const PG_NEGATIVE: u16 = 0x4000;

const NEGATIVE_TAG: u8 = 0x01;
const ZERO_TAG: u8 = 0x02;
const POSITIVE_TAG: u8 = 0x03;
const TERMINATOR: u8 = 0x00;

/// Errors returned by [`BigDecimal`] parsing, arithmetic, conversions and encodings.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BigDecimalError {
	/// The text is empty.
	#[error("empty text is not a decimal number")]
	Empty,
	/// The text is not an optional sign, digits, an optional fraction and an optional exponent.
	#[error("text is not a decimal number")]
	InvalidSyntax,
	/// The text is `NaN` or an infinity. Decimal values are always finite.
	#[error("decimal values must be finite")]
	NonFinite,
	/// The text has more significant digits than [`MAX_DIGITS`].
	#[error("decimal has more significant digits than the supported maximum")]
	TooManyDigits,
	/// The exponent is larger in magnitude than [`MAX_EXPONENT`].
	#[error("decimal exponent is out of range")]
	ExponentOutOfRange,
	/// A scale or a number of digits is larger than [`MAX_SCALE`].
	#[error("decimal scale is out of range")]
	ScaleOutOfRange,
	/// The divisor is zero.
	#[error("division by zero")]
	DivisionByZero,
	/// The value has no exact representation in the target type.
	#[error("value does not fit exactly in {0}")]
	DoesNotFit(&'static str),
	/// The `f64` is NaN or infinite.
	#[error("f64 value is not finite")]
	NonFiniteFloat,
	/// The PostgreSQL numeric parts are malformed or outside the PostgreSQL limits.
	#[error("invalid PostgreSQL numeric: {0}")]
	PgNumeric(&'static str),
	/// The bytes are not a valid ordered encoding of a decimal.
	#[error("invalid ordered decimal encoding: {0}")]
	InvalidEncoding(&'static str),
}

/// An arbitrary-precision decimal number, `coefficient * 10^(-scale)`.
///
/// Invariant: `scale >= 0`. Equality, ordering and hashing are numeric, while
/// [`BigDecimal::scale`] and [`Display`] report the scale as written.
#[derive(Clone, Debug)]
pub struct BigDecimal {
	coefficient: BigInt,
	scale: i64,
}

#[derive(Clone, Copy)]
enum Rounding {
	HalfAwayFromZero,
	Truncate,
	Floor,
	Ceiling,
}

impl BigDecimal {
	/// The value `coefficient * 10^(-scale)`. The caller keeps `scale` non-negative.
	#[cfg(test)]
	pub(crate) fn from_parts(coefficient: BigInt, scale: i64) -> Self {
		debug_assert!(scale >= 0);
		Self {
			coefficient,
			scale,
		}
	}

	/// The value `coefficient * 10^(-scale)`, or `None` when the coefficient has more than
	/// [`MAX_DIGITS`] digits or the scale is beyond [`MAX_SCALE`]. Every arithmetic result is
	/// built through here, so no operation yields a value that [`FromStr`] or the stored encoding
	/// refuses to read back.
	fn bounded(coefficient: BigInt, scale: i64) -> Option<Self> {
		static LIMIT: OnceLock<BigInt> = OnceLock::new();
		let limit = LIMIT.get_or_init(|| pow10(MAX_DIGITS as u64));
		if scale > MAX_SCALE || coefficient.magnitude() >= limit.magnitude() {
			return None;
		}
		Some(Self {
			coefficient,
			scale,
		})
	}

	/// The coefficient and scale as stored: the value is `coefficient * 10^(-scale)`.
	pub(crate) fn parts(&self) -> (&BigInt, i64) {
		(&self.coefficient, self.scale)
	}

	/// The sign and the magnitude of the coefficient, for a stored form.
	///
	/// The magnitude is little-endian and has no zero high byte: it is empty for zero and
	/// otherwise holds the fewest bytes that hold it. [`Self::from_le_parts`] reads it back.
	pub fn magnitude_le_bytes(&self) -> Vec<u8> {
		if self.coefficient.is_zero() {
			Vec::new()
		} else {
			self.coefficient.magnitude().to_bytes_le()
		}
	}

	/// The value with the given sign, the magnitude as [`Self::magnitude_le_bytes`] writes it,
	/// and the scale as written.
	///
	/// Refuses a scale outside `0..=MAX_SCALE` and a magnitude longer than
	/// [`MAX_COEFFICIENT_BYTES`]. A zero magnitude is zero whatever the sign, so a negative zero
	/// is read as zero. Accepts a magnitude with a zero high byte; the stored grammar refuses it
	/// before this is called.
	pub fn from_le_parts(
		negative: bool,
		magnitude: &[u8],
		scale: i64,
	) -> Result<Self, BigDecimalError> {
		if !(0..=MAX_SCALE).contains(&scale) {
			return Err(BigDecimalError::ScaleOutOfRange);
		}
		if magnitude.len() > MAX_COEFFICIENT_BYTES {
			return Err(BigDecimalError::InvalidEncoding("coefficient magnitude is too large"));
		}
		let sign = if negative && magnitude.iter().any(|&b| b != 0) {
			Sign::Minus
		} else {
			Sign::Plus
		};
		Ok(Self {
			coefficient: BigInt::from_bytes_le(sign, magnitude),
			scale,
		})
	}

	/// The decimal digits of the coefficient's magnitude, as ASCII, with its trailing zeros: the
	/// value is these digits times `10^(-scale)`, negated when [`Self::signum`] is `-1`. `1.50`
	/// has the digits `150` and `-1500` the digits `1500`. Zero has the digits `0`.
	pub fn coefficient_digits(&self) -> String {
		self.coefficient.magnitude().to_string()
	}

	/// The value of the ASCII decimal `digits` times `10^exponent`, negated when `negative`.
	///
	/// A negative exponent becomes the scale and a non-negative one is folded into the
	/// coefficient, so the digits `150` with exponent `-2` are `1.50` and with exponent `1` are
	/// `1500`. Leading zero digits are allowed and zero is zero whatever the sign.
	///
	/// Refuses digits that are empty or not all decimal ([`BigDecimalError::InvalidSyntax`]), a
	/// scale beyond [`MAX_SCALE`], and a coefficient of more than [`MAX_COEFFICIENT_BYTES`]
	/// bytes, which [`Self::from_le_parts`] also refuses. The size of the coefficient is checked
	/// before it is built.
	pub fn from_digits(
		negative: bool,
		digits: &[u8],
		exponent: i64,
	) -> Result<Self, BigDecimalError> {
		// A byte holds at least two decimal digits, so a coefficient of more digits than this
		// cannot fit `MAX_COEFFICIENT_BYTES` bytes.
		const MAX_COEFFICIENT_DIGITS: u64 = 3 * MAX_COEFFICIENT_BYTES as u64;
		if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
			return Err(BigDecimalError::InvalidSyntax);
		}
		let scale = if exponent < 0 {
			exponent.checked_neg().filter(|scale| *scale <= MAX_SCALE)
		} else {
			Some(0)
		}
		.ok_or(BigDecimalError::ScaleOutOfRange)?;
		let zeros = exponent.max(0).unsigned_abs();
		let total = (digits.len() as u64).saturating_add(zeros);
		if total > MAX_COEFFICIENT_DIGITS {
			return Err(BigDecimalError::InvalidEncoding("coefficient magnitude is too large"));
		}
		let magnitude = BigInt::parse_bytes(digits, 10).ok_or(BigDecimalError::InvalidSyntax)?;
		let magnitude = if zeros > 0 {
			magnitude * pow10(zeros)
		} else {
			magnitude
		};
		if magnitude.magnitude().bits().div_ceil(8) > MAX_COEFFICIENT_BYTES as u64 {
			return Err(BigDecimalError::InvalidEncoding("coefficient magnitude is too large"));
		}
		Ok(Self {
			coefficient: if negative {
				-magnitude
			} else {
				magnitude
			},
			scale,
		})
	}

	/// Whether the value has no trailing zero in its fractional part, as [`Self::normalized`]
	/// writes it: its scale is 0 or its last fractional digit is not zero.
	pub fn is_normalized(&self) -> bool {
		self.scale == 0 || !(&self.coefficient % BigInt::from(10u8)).is_zero()
	}

	/// The number of fractional digits the value is written with.
	///
	/// Always at most [`MAX_SCALE`]. Trailing zeros count, so `1.50` has scale 2.
	pub fn scale(&self) -> i64 {
		self.scale
	}

	/// Whether the value is zero, regardless of its sign or scale.
	pub fn is_zero(&self) -> bool {
		self.coefficient.is_zero()
	}

	/// The sign of the value: `-1`, `0` or `1`.
	pub fn signum(&self) -> i8 {
		match self.coefficient.sign() {
			Sign::Minus => -1,
			Sign::NoSign => 0,
			Sign::Plus => 1,
		}
	}

	/// The absolute value, with the same scale.
	pub fn abs(&self) -> Self {
		Self {
			coefficient: self.coefficient.abs(),
			scale: self.scale,
		}
	}

	/// The number of significant digits of the value.
	///
	/// Trailing zeros of the integer part are not significant, so `1500` has precision 2 and
	/// `1.50` has precision 2. Zero has precision 1.
	pub fn precision(&self) -> usize {
		if self.coefficient.is_zero() {
			return 1;
		}
		let (coefficient, _) = self.canonical();
		coefficient.magnitude().to_string().len()
	}

	/// The same value with trailing zeros of the fractional part removed.
	///
	/// The scale becomes the number of fractional digits left. Integer values keep their digits,
	/// so `1.500` becomes `1.5` and `1500` stays `1500`, both with a scale of 0 or more.
	pub fn normalized(&self) -> Self {
		let (coefficient, exponent) = self.canonical();
		if exponent >= 0 {
			Self {
				coefficient: coefficient * pow10(exponent.unsigned_abs()),
				scale: 0,
			}
		} else {
			Self {
				coefficient,
				scale: -exponent,
			}
		}
	}

	/// The sum, with the larger of the two scales.
	///
	/// Returns `None` if the sum's coefficient has more than [`MAX_DIGITS`] digits.
	pub fn checked_add(&self, other: &Self) -> Option<Self> {
		let scale = self.scale.max(other.scale);
		let lhs = scaled(&self.coefficient, scale - self.scale);
		let rhs = scaled(&other.coefficient, scale - other.scale);
		Self::bounded(&*lhs + &*rhs, scale)
	}

	/// The difference, with the larger of the two scales.
	///
	/// Returns `None` if the difference's coefficient has more than [`MAX_DIGITS`] digits.
	pub fn checked_sub(&self, other: &Self) -> Option<Self> {
		let scale = self.scale.max(other.scale);
		let lhs = scaled(&self.coefficient, scale - self.scale);
		let rhs = scaled(&other.coefficient, scale - other.scale);
		Self::bounded(&*lhs - &*rhs, scale)
	}

	/// The exact product, with scale equal to the sum of the operand scales.
	///
	/// Returns `None` if that scale would exceed [`MAX_SCALE`] or the product's coefficient has
	/// more than [`MAX_DIGITS`] digits. A product whose operands are already too wide for it is
	/// refused before it is computed.
	pub fn checked_mul(&self, other: &Self) -> Option<Self> {
		let scale = self.scale.checked_add(other.scale)?;
		if scale > MAX_SCALE {
			return None;
		}
		// The product of an a-bit and a b-bit number has at least a + b - 1 bits, and a number
		// of more than `MAX_DIGITS * log2(10)` bits has more than `MAX_DIGITS` digits.
		let bits = (self.coefficient.bits() + other.coefficient.bits()).saturating_sub(1);
		if bits > MAX_DIGITS as u64 * 3322 / 1000 + 1 {
			return None;
		}
		Self::bounded(&self.coefficient * &other.coefficient, scale)
	}

	/// The quotient rounded half away from zero to `frac_digits` fractional digits.
	///
	/// The result has scale `frac_digits`. Returns [`BigDecimalError::DivisionByZero`] if `other`
	/// is zero, and [`BigDecimalError::ScaleOutOfRange`] if `frac_digits` exceeds [`MAX_SCALE`].
	pub fn div(&self, other: &Self, frac_digits: u32) -> Result<Self, BigDecimalError> {
		let frac = i64::from(frac_digits);
		if frac > MAX_SCALE {
			return Err(BigDecimalError::ScaleOutOfRange);
		}
		if other.coefficient.is_zero() {
			return Err(BigDecimalError::DivisionByZero);
		}
		// self / other * 10^frac = (c_a / c_b) * 10^(s_b - s_a + frac)
		let shift = other.scale - self.scale + frac;
		let (mut numerator, mut denominator) = if shift >= 0 {
			(&self.coefficient * pow10(shift.unsigned_abs()), other.coefficient.clone())
		} else {
			(self.coefficient.clone(), &other.coefficient * pow10(shift.unsigned_abs()))
		};
		if denominator.is_negative() {
			numerator = -numerator;
			denominator = -denominator;
		}
		let coefficient = divide_rounded(&numerator, &denominator, Rounding::HalfAwayFromZero);
		Self::bounded(coefficient, frac).ok_or(BigDecimalError::TooManyDigits)
	}

	/// The quotient with `max(20, largest operand scale) + 4` fractional digits, rounded half
	/// away from zero.
	pub fn div_default(&self, other: &Self) -> Result<Self, BigDecimalError> {
		let frac = self.scale.max(other.scale).max(20) + 4;
		let frac = u32::try_from(frac).map_err(|_| BigDecimalError::ScaleOutOfRange)?;
		self.div(other, frac)
	}

	/// The value rounded half away from zero to `dp` fractional digits.
	///
	/// A negative `dp` rounds to tens, hundreds and so on, and the result then has scale 0.
	/// A positive `dp` above the current scale pads the result with zeros.
	pub fn round(&self, dp: i64) -> Result<Self, BigDecimalError> {
		self.quantize(dp, Rounding::HalfAwayFromZero)
	}

	/// The value written with `scale` fractional digits, rounded half away from zero.
	///
	/// Equivalent to [`Self::round`] with a non-negative `dp`.
	pub fn rescale(&self, scale: u32) -> Result<Self, BigDecimalError> {
		self.round(i64::from(scale))
	}

	/// The value truncated towards zero to `dp` fractional digits.
	///
	/// Uses the same `dp` convention as [`Self::round`].
	pub fn trunc(&self, dp: i64) -> Result<Self, BigDecimalError> {
		self.quantize(dp, Rounding::Truncate)
	}

	/// The largest integer not greater than the value, with scale 0.
	pub fn floor(&self) -> Result<Self, BigDecimalError> {
		self.quantize(0, Rounding::Floor)
	}

	/// The smallest integer not less than the value, with scale 0.
	pub fn ceil(&self) -> Result<Self, BigDecimalError> {
		self.quantize(0, Rounding::Ceiling)
	}

	/// The integer value, or `None` if the value has a nonzero fractional part.
	pub fn to_i64(&self) -> Option<i64> {
		self.integral_part()?.to_i64()
	}

	/// The integer value, or `None` if the value has a nonzero fractional part or is negative
	/// (other than zero) or too large.
	pub fn to_u64(&self) -> Option<u64> {
		self.integral_part()?.to_u64()
	}

	/// The nearest `f64`.
	///
	/// Values beyond the `f64` range become infinities, and values too small become zero.
	pub fn to_f64(&self) -> f64 {
		let text = format!("{}e{}", self.coefficient, -self.scale);
		text.parse::<f64>().unwrap_or(f64::NAN)
	}

	/// The exact value of the shortest decimal text that round-trips to `value`.
	///
	/// Returns [`BigDecimalError::NonFiniteFloat`] for NaN and infinities.
	pub fn try_from_f64(value: f64) -> Result<Self, BigDecimalError> {
		if !value.is_finite() {
			return Err(BigDecimalError::NonFiniteFloat);
		}
		format!("{value:e}").parse()
	}

	/// The PostgreSQL binary `numeric` parts: `(ndigits, weight, sign, dscale, digits)`.
	///
	/// `digits` are base-10000 groups, `weight` is the power of 10000 of the first group, `sign`
	/// is `0x0000` or `0x4000`, and `dscale` is the scale. Trailing zero groups are removed.
	/// Zero has no digits, weight 0 and sign `0x0000`.
	///
	/// Returns [`BigDecimalError::PgNumeric`] if the scale exceeds 16383, if there are more than
	/// 131072 digits before the decimal point, or if the groups do not fit in the wire format.
	pub fn to_pg_numeric(&self) -> Result<(i16, i16, u16, u16, Vec<i16>), BigDecimalError> {
		let overflow = |reason| BigDecimalError::PgNumeric(reason);
		let dscale = u16::try_from(self.scale)
			.ok()
			.filter(|scale| *scale <= PG_MAX_DSCALE)
			.ok_or(overflow("display scale exceeds 16383"))?;
		if self.coefficient.is_zero() {
			return Ok((0, 0, PG_POSITIVE, dscale, Vec::new()));
		}
		let sign = if self.coefficient.is_negative() {
			PG_NEGATIVE
		} else {
			PG_POSITIVE
		};
		let digits = self.coefficient.magnitude().to_string();
		let scale = usize::from(dscale);
		let (integer, fraction) = if digits.len() > scale {
			let (integer, fraction) = digits.split_at(digits.len() - scale);
			(integer.to_owned(), fraction.to_owned())
		} else {
			(String::new(), format!("{}{digits}", "0".repeat(scale - digits.len())))
		};
		if integer.len() > PG_MAX_INTEGER_DIGITS {
			return Err(overflow("more than 131072 digits before the decimal point"));
		}
		// Pad so that the decimal point falls on a group boundary.
		let integer = format!("{}{integer}", "0".repeat((4 - integer.len() % 4) % 4));
		let fraction = format!("{fraction}{}", "0".repeat((4 - fraction.len() % 4) % 4));
		let all = format!("{integer}{fraction}");
		let mut groups: Vec<i16> = all
			.as_bytes()
			.chunks(4)
			.map(|chunk| chunk.iter().fold(0i16, |acc, byte| acc * 10 + i16::from(*byte - b'0')))
			.collect();
		let integer_groups =
			i64::try_from(integer.len() / 4).map_err(|_| overflow("weight out of range"))?;
		let mut weight = integer_groups - 1;
		let Some(first) = groups.iter().position(|group| *group != 0) else {
			return Ok((0, 0, PG_POSITIVE, dscale, Vec::new()));
		};
		groups.drain(..first);
		weight -= i64::try_from(first).map_err(|_| overflow("weight out of range"))?;
		while groups.last() == Some(&0) {
			groups.pop();
		}
		let ndigits = i16::try_from(groups.len())
			.map_err(|_| overflow("too many digits for the wire format"))?;
		let weight = i16::try_from(weight).map_err(|_| overflow("weight out of range"))?;
		Ok((ndigits, weight, sign, dscale, groups))
	}

	/// The value described by PostgreSQL binary `numeric` parts.
	///
	/// The result has scale `dscale`. Returns [`BigDecimalError::PgNumeric`] if `ndigits` does not
	/// match `digits`, if the sign is not `0x0000` or `0x4000` (NaN and infinities have no
	/// decimal form), if a group is outside `0..=9999`, if `dscale` exceeds 16383, or if the
	/// value has nonzero digits beyond `dscale`.
	pub fn from_pg_numeric(
		ndigits: i16,
		weight: i16,
		sign: u16,
		dscale: u16,
		digits: &[i16],
	) -> Result<Self, BigDecimalError> {
		let overflow = |reason| BigDecimalError::PgNumeric(reason);
		if usize::try_from(ndigits).ok() != Some(digits.len()) {
			return Err(overflow("ndigits does not match the digit count"));
		}
		if sign != PG_POSITIVE && sign != PG_NEGATIVE {
			return Err(overflow("sign is neither positive nor negative"));
		}
		if dscale > PG_MAX_DSCALE {
			return Err(overflow("display scale exceeds 16383"));
		}
		let scale = i64::from(dscale);
		if digits.iter().any(|group| !(0..=9999).contains(group)) {
			return Err(overflow("digit outside 0..=9999"));
		}
		if digits.is_empty() {
			return Ok(Self {
				coefficient: BigInt::zero(),
				scale,
			});
		}
		// The groups form the coefficient of value * 10^(4 * exponent), where `exponent` is the
		// power of 10000 of the last group.
		let text: String = digits.iter().map(|group| format!("{group:04}")).collect();
		let coefficient =
			BigInt::parse_bytes(text.as_bytes(), 10).ok_or(overflow("digits are not decimal"))?;
		let last_group = i64::from(weight)
			- (i64::try_from(digits.len()).map_err(|_| overflow("too many digits"))? - 1);
		let exponent = last_group * 4;
		let (coefficient, exact_scale) = if exponent < 0 {
			(coefficient, -exponent)
		} else {
			(coefficient * pow10(exponent.unsigned_abs()), 0)
		};
		let coefficient = if exact_scale > scale {
			let divisor = pow10((exact_scale - scale).unsigned_abs());
			if !(&coefficient % &divisor).is_zero() {
				return Err(overflow("nonzero digits beyond the display scale"));
			}
			&coefficient / &divisor
		} else {
			coefficient * pow10((scale - exact_scale).unsigned_abs())
		};
		let coefficient = if sign == PG_NEGATIVE {
			-coefficient
		} else {
			coefficient
		};
		Ok(Self {
			coefficient,
			scale,
		})
	}

	/// Appends the order-preserving encoding of the value to `out`.
	///
	/// See the module documentation for the layout. Equal values produce equal bytes, and the
	/// byte order of encodings equals the numeric order of the values.
	pub fn encode_ordered(&self, out: &mut Vec<u8>) {
		let (coefficient, exponent) = self.canonical();
		if coefficient.is_zero() {
			out.push(ZERO_TAG);
			return;
		}
		let digits = coefficient.magnitude().to_string();
		let digit_count = i64::try_from(digits.len()).unwrap_or(i64::MAX);
		let adjusted = exponent + digit_count - 1;
		let mut body = Vec::with_capacity(digits.len() + 9);
		body.extend_from_slice(&((adjusted as u64) ^ (1u64 << 63)).to_be_bytes());
		body.extend(digits.bytes().map(|byte| byte - b'0' + 1));
		body.push(TERMINATOR);
		if coefficient.is_negative() {
			out.push(NEGATIVE_TAG);
			out.extend(body.into_iter().map(|byte| !byte));
		} else {
			out.push(POSITIVE_TAG);
			out.extend(body);
		}
	}

	/// Decodes one encoding from the start of `bytes`.
	///
	/// Returns the value with its trailing zeros removed and the number of bytes consumed. The
	/// input is rejected, and never panics, if it is truncated, has a digit or sign byte out of
	/// range, is not in canonical form, or has an adjusted exponent beyond
	/// [`MAX_ENCODED_EXPONENT`] or a scale beyond [`MAX_SCALE`].
	pub fn decode_ordered(bytes: &[u8]) -> Result<(Self, usize), BigDecimalError> {
		let Some((&header, _)) = bytes.split_first() else {
			return Err(BigDecimalError::InvalidEncoding("empty input"));
		};
		match header {
			ZERO_TAG => Ok((Self::default(), 1)),
			NEGATIVE_TAG | POSITIVE_TAG => {
				let negative = header == NEGATIVE_TAG;
				let read = |byte: u8| {
					if negative {
						!byte
					} else {
						byte
					}
				};
				let exponent_bytes = bytes
					.get(1..9)
					.and_then(|slice| <[u8; 8]>::try_from(slice).ok())
					.ok_or(BigDecimalError::InvalidEncoding("truncated exponent"))?;
				let biased = u64::from_be_bytes(exponent_bytes.map(read));
				let adjusted = (biased ^ (1u64 << 63)) as i64;
				if adjusted.unsigned_abs() > MAX_ENCODED_EXPONENT.unsigned_abs() {
					return Err(BigDecimalError::InvalidEncoding("exponent out of range"));
				}
				let mut digits: Vec<u8> = Vec::new();
				let mut position = 9;
				loop {
					let raw = *bytes
						.get(position)
						.ok_or(BigDecimalError::InvalidEncoding("missing terminator"))?;
					position += 1;
					match read(raw) {
						TERMINATOR => break,
						byte @ 1..=10 => digits.push(b'0' + (byte - 1)),
						_ => return Err(BigDecimalError::InvalidEncoding("digit out of range")),
					}
				}
				match digits.last().copied() {
					None | Some(b'0') => {
						return Err(BigDecimalError::InvalidEncoding("non-canonical digits"));
					}
					Some(_) => {}
				}
				let digit_count = i64::try_from(digits.len())
					.map_err(|_| BigDecimalError::InvalidEncoding("too many digits"))?;
				let exponent = adjusted - (digit_count - 1);
				let magnitude = BigInt::parse_bytes(&digits, 10)
					.ok_or(BigDecimalError::InvalidEncoding("digits are not decimal"))?;
				let coefficient = if negative {
					-magnitude
				} else {
					magnitude
				};
				let value = if exponent >= 0 {
					Self {
						coefficient: coefficient * pow10(exponent.unsigned_abs()),
						scale: 0,
					}
				} else {
					if -exponent > MAX_SCALE {
						return Err(BigDecimalError::ScaleOutOfRange);
					}
					Self {
						coefficient,
						scale: -exponent,
					}
				};
				Ok((value, position))
			}
			_ => Err(BigDecimalError::InvalidEncoding("unknown sign byte")),
		}
	}

	/// The coefficient with no trailing zero digits, and the matching exponent, so that the
	/// value is `coefficient * 10^exponent`. Zero is `(0, 0)`.
	pub(crate) fn canonical(&self) -> (BigInt, i64) {
		if self.coefficient.is_zero() {
			return (BigInt::zero(), 0);
		}
		// 10^19 fits in u64, so most trailing zeros are removed in chunks.
		let chunk = BigInt::from(10_000_000_000_000_000_000u64);
		let ten = BigInt::from(10u8);
		let mut coefficient = self.coefficient.clone();
		let mut exponent = -self.scale;
		while (&coefficient % &chunk).is_zero() {
			coefficient = &coefficient / &chunk;
			exponent += 19;
		}
		while (&coefficient % &ten).is_zero() {
			coefficient = &coefficient / &ten;
			exponent += 1;
		}
		(coefficient, exponent)
	}

	/// The integer value, or `None` if the value is not an integer.
	fn integral_part(&self) -> Option<BigInt> {
		if self.scale == 0 {
			return Some(self.coefficient.clone());
		}
		let divisor = pow10(self.scale.unsigned_abs());
		if (&self.coefficient % &divisor).is_zero() {
			Some(&self.coefficient / &divisor)
		} else {
			None
		}
	}

	/// Rounds the value to `digits` fractional digits (negative for tens and above).
	fn quantize(&self, digits: i64, rounding: Rounding) -> Result<Self, BigDecimalError> {
		if !(-MAX_SCALE..=MAX_SCALE).contains(&digits) {
			return Err(BigDecimalError::ScaleOutOfRange);
		}
		// value * 10^digits = coefficient * 10^(digits - scale)
		let shift = digits - self.scale;
		let (numerator, denominator) = if shift >= 0 {
			(&self.coefficient * pow10(shift.unsigned_abs()), BigInt::one())
		} else {
			(self.coefficient.clone(), pow10(shift.unsigned_abs()))
		};
		let integer = divide_rounded(&numerator, &denominator, rounding);
		let quantized = if digits >= 0 {
			Self::bounded(integer, digits)
		} else {
			Self::bounded(integer * pow10(digits.unsigned_abs()), 0)
		};
		quantized.ok_or(BigDecimalError::TooManyDigits)
	}
}

/// `10^exponent` as an integer.
pub(crate) fn pow10(exponent: u64) -> BigInt {
	let mut result = BigInt::one();
	let mut base = BigInt::from(10u8);
	let mut remaining = exponent;
	while remaining > 0 {
		if remaining & 1 == 1 {
			result = &result * &base;
		}
		remaining >>= 1;
		if remaining > 0 {
			base = &base * &base;
		}
	}
	result
}

/// The coefficient multiplied by `10^by`, borrowed when `by` is zero.
fn scaled(coefficient: &BigInt, by: i64) -> Cow<'_, BigInt> {
	if by == 0 {
		Cow::Borrowed(coefficient)
	} else {
		Cow::Owned(coefficient * pow10(by.unsigned_abs()))
	}
}

/// `numerator / denominator` rounded to an integer. `denominator` must be positive.
fn divide_rounded(numerator: &BigInt, denominator: &BigInt, rounding: Rounding) -> BigInt {
	let quotient = numerator / denominator;
	let inexact = !(numerator % denominator).is_zero();
	match rounding {
		Rounding::Truncate => quotient,
		Rounding::Floor => {
			if inexact && numerator.is_negative() {
				quotient - BigInt::one()
			} else {
				quotient
			}
		}
		Rounding::Ceiling => {
			if inexact && numerator.is_positive() {
				quotient + BigInt::one()
			} else {
				quotient
			}
		}
		Rounding::HalfAwayFromZero => {
			// floor((2|n| + d) / 2d) rounds |n| / d to nearest, with ties going up in magnitude.
			let magnitude = numerator.abs();
			let two = BigInt::from(2u8);
			let rounded = (&magnitude * &two + denominator) / (denominator * &two);
			if numerator.is_negative() {
				-rounded
			} else {
				rounded
			}
		}
	}
}

fn all_ascii_digits(text: &str) -> bool {
	text.bytes().all(|byte| byte.is_ascii_digit())
}

/// Parses the signed integer after `e` or `E`, bounded by [`MAX_EXPONENT`].
fn parse_exponent(text: &str) -> Result<i64, BigDecimalError> {
	let (negative, digits) = match text.strip_prefix('-') {
		Some(rest) => (true, rest),
		None => (false, text.strip_prefix('+').unwrap_or(text)),
	};
	if digits.is_empty() || !all_ascii_digits(digits) {
		return Err(BigDecimalError::InvalidSyntax);
	}
	let significant = digits.trim_start_matches('0');
	if significant.len() > 18 {
		return Err(BigDecimalError::ExponentOutOfRange);
	}
	let magnitude: i64 = if significant.is_empty() {
		0
	} else {
		significant.parse().map_err(|_| BigDecimalError::ExponentOutOfRange)?
	};
	if magnitude > MAX_EXPONENT {
		return Err(BigDecimalError::ExponentOutOfRange);
	}
	Ok(if negative {
		-magnitude
	} else {
		magnitude
	})
}

impl FromStr for BigDecimal {
	type Err = BigDecimalError;

	/// Parses an optional sign, digits with an optional fraction, and an optional exponent
	/// (`e` or `E`). A leading `.` or trailing `.` is accepted. Leading zeros do not count
	/// as significant digits.
	///
	/// Returns [`BigDecimalError::NonFinite`] for `NaN` and infinities, and
	/// [`BigDecimalError::TooManyDigits`] for more than [`MAX_DIGITS`] significant digits.
	fn from_str(text: &str) -> Result<Self, Self::Err> {
		if text.is_empty() {
			return Err(BigDecimalError::Empty);
		}
		let (negative, unsigned) = match text.strip_prefix('-') {
			Some(rest) => (true, rest),
			None => (false, text.strip_prefix('+').unwrap_or(text)),
		};
		if ["nan", "inf", "infinity"].iter().any(|word| unsigned.eq_ignore_ascii_case(word)) {
			return Err(BigDecimalError::NonFinite);
		}
		let (mantissa, exponent_text) = match unsigned.find(['e', 'E']) {
			Some(index) => (&unsigned[..index], Some(&unsigned[index + 1..])),
			None => (unsigned, None),
		};
		let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
		if integer.is_empty() && fraction.is_empty() {
			return Err(BigDecimalError::InvalidSyntax);
		}
		if !all_ascii_digits(integer) || !all_ascii_digits(fraction) {
			return Err(BigDecimalError::InvalidSyntax);
		}
		let exponent = match exponent_text {
			Some(text) => parse_exponent(text)?,
			None => 0,
		};
		let all = [integer, fraction].concat();
		if all.trim_start_matches('0').len() > MAX_DIGITS {
			return Err(BigDecimalError::TooManyDigits);
		}
		let fraction_len =
			i64::try_from(fraction.len()).map_err(|_| BigDecimalError::ScaleOutOfRange)?;
		let scale = fraction_len.checked_sub(exponent).ok_or(BigDecimalError::ScaleOutOfRange)?;
		if scale > MAX_SCALE {
			return Err(BigDecimalError::ScaleOutOfRange);
		}
		// A positive exponent folds into the coefficient, whose positional digits must fit too.
		let significant = all.trim_start_matches('0').len();
		if significant > 0
			&& scale < 0
			&& significant as u64 + scale.unsigned_abs() > MAX_DIGITS as u64
		{
			return Err(BigDecimalError::TooManyDigits);
		}
		let mut coefficient =
			BigInt::parse_bytes(all.as_bytes(), 10).ok_or(BigDecimalError::InvalidSyntax)?;
		if negative {
			coefficient = -coefficient;
		}
		if scale < 0 {
			Ok(Self {
				coefficient: coefficient * pow10(scale.unsigned_abs()),
				scale: 0,
			})
		} else {
			Ok(Self {
				coefficient,
				scale,
			})
		}
	}
}

impl Display for BigDecimal {
	/// Positional notation with exactly [`BigDecimal::scale`] fractional digits. The sign
	/// appears only for negative nonzero values.
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		if self.coefficient.sign() == Sign::Minus {
			f.write_str("-")?;
		}
		let digits = self.coefficient.magnitude().to_string();
		let scale = usize::try_from(self.scale).map_err(|_| fmt::Error)?;
		if scale == 0 {
			f.write_str(&digits)
		} else if digits.len() > scale {
			let (integer, fraction) = digits.split_at(digits.len() - scale);
			write!(f, "{integer}.{fraction}")
		} else {
			write!(f, "0.{}{digits}", "0".repeat(scale - digits.len()))
		}
	}
}

impl PartialEq for BigDecimal {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for BigDecimal {}

impl PartialOrd for BigDecimal {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for BigDecimal {
	fn cmp(&self, other: &Self) -> Ordering {
		let sign = |s: Sign| match s {
			Sign::Minus => -1,
			Sign::NoSign => 0,
			Sign::Plus => 1,
		};
		let (lhs_sign, rhs_sign) = (sign(self.coefficient.sign()), sign(other.coefficient.sign()));
		if lhs_sign != rhs_sign || lhs_sign == 0 {
			return lhs_sign.cmp(&rhs_sign);
		}
		// `log2 |v|` lies in `[bits - 1, bits) - scale · log2 10`; two magnitudes this far apart
		// are ordered without rescaling either coefficient.
		let log2 =
			|v: &Self| v.coefficient.bits() as f64 - v.scale as f64 * std::f64::consts::LOG2_10;
		let gap = log2(self) - log2(other);
		if gap.abs() > 2.0 {
			let magnitude = if gap > 0.0 {
				Ordering::Greater
			} else {
				Ordering::Less
			};
			return if lhs_sign > 0 {
				magnitude
			} else {
				magnitude.reverse()
			};
		}
		let scale = self.scale.max(other.scale);
		let lhs = scaled(&self.coefficient, scale - self.scale);
		let rhs = scaled(&other.coefficient, scale - other.scale);
		(*lhs).cmp(&*rhs)
	}
}

impl Hash for BigDecimal {
	fn hash<H: Hasher>(&self, state: &mut H) {
		let (coefficient, exponent) = self.canonical();
		coefficient.hash(state);
		exponent.hash(state);
	}
}

impl Default for BigDecimal {
	/// Zero, with scale 0.
	fn default() -> Self {
		Self {
			coefficient: BigInt::zero(),
			scale: 0,
		}
	}
}

impl Neg for BigDecimal {
	type Output = Self;

	fn neg(self) -> Self {
		Self {
			coefficient: -self.coefficient,
			scale: self.scale,
		}
	}
}

impl Serialize for BigDecimal {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&self.to_string())
	}
}

impl<'de> Deserialize<'de> for BigDecimal {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let text = String::deserialize(deserializer)?;
		text.parse().map_err(serde::de::Error::custom)
	}
}

impl From<i64> for BigDecimal {
	fn from(value: i64) -> Self {
		Self {
			coefficient: BigInt::from(value),
			scale: 0,
		}
	}
}

impl From<u64> for BigDecimal {
	fn from(value: u64) -> Self {
		Self {
			coefficient: BigInt::from(value),
			scale: 0,
		}
	}
}

impl From<i128> for BigDecimal {
	fn from(value: i128) -> Self {
		Self {
			coefficient: BigInt::from(value),
			scale: 0,
		}
	}
}

impl From<u128> for BigDecimal {
	fn from(value: u128) -> Self {
		Self {
			coefficient: BigInt::from(value),
			scale: 0,
		}
	}
}

impl From<Decimal> for BigDecimal {
	/// Exact: the mantissa becomes the coefficient and the decimal scale is kept.
	fn from(value: Decimal) -> Self {
		Self {
			coefficient: BigInt::from(value.mantissa()),
			scale: i64::from(value.scale()),
		}
	}
}

impl TryFrom<&BigDecimal> for Decimal {
	type Error = BigDecimalError;

	/// Exact conversion. Trailing zeros are removed first, so `1.50` converts, while a value
	/// with more than 28 significant fractional digits, or a magnitude of 10^29 or more, does
	/// not fit and returns [`BigDecimalError::DoesNotFit`].
	fn try_from(value: &BigDecimal) -> Result<Self, Self::Error> {
		let does_not_fit = || BigDecimalError::DoesNotFit("rust_decimal::Decimal");
		let (coefficient, exponent) = value.canonical();
		// The magnitude is below 10^29 and the scale is at most 28.
		if !(-28..29).contains(&exponent) {
			return Err(does_not_fit());
		}
		let (mantissa, scale) = if exponent >= 0 {
			(coefficient * pow10(exponent.unsigned_abs()), 0u32)
		} else {
			let scale = u32::try_from(-exponent).map_err(|_| does_not_fit())?;
			(coefficient, scale)
		};
		let mantissa = mantissa.to_i128().ok_or_else(does_not_fit)?;
		Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|_| does_not_fit())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	use rust_decimal::RoundingStrategy;

	use super::*;

	/// Deterministic xorshift generator for the property-style loops.
	struct Xorshift(u64);

	impl Xorshift {
		fn next_u64(&mut self) -> u64 {
			let mut x = self.0;
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			self.0 = x;
			x
		}

		fn below(&mut self, bound: u64) -> u64 {
			self.next_u64() % bound
		}
	}

	fn dec(text: &str) -> BigDecimal {
		text.parse().unwrap()
	}

	fn random_decimal_text(rng: &mut Xorshift) -> String {
		let len = 1 + rng.below(20) as usize;
		let digits: String = (0..len).map(|_| char::from(b'0' + rng.below(10) as u8)).collect();
		let scale = rng.below(12) as usize;
		let sign = if rng.below(2) == 0 {
			""
		} else {
			"-"
		};
		let body = if scale == 0 {
			digits
		} else if scale < len {
			format!("{}.{}", &digits[..len - scale], &digits[len - scale..])
		} else {
			format!("0.{}{}", "0".repeat(scale - len), digits)
		};
		format!("{sign}{body}")
	}

	fn random_decimal(rng: &mut Xorshift) -> BigDecimal {
		random_decimal_text(rng).parse().unwrap()
	}

	fn hash_of(value: &BigDecimal) -> u64 {
		let mut hasher = DefaultHasher::new();
		value.hash(&mut hasher);
		hasher.finish()
	}

	fn encoded(value: &BigDecimal) -> Vec<u8> {
		let mut out = Vec::new();
		value.encode_ordered(&mut out);
		out
	}

	#[test]
	fn parse_keeps_written_scale() {
		assert_eq!(dec("1.50").to_string(), "1.50");
		assert_eq!(dec("1.50").scale(), 2);
		assert_eq!(dec("-0.00").to_string(), "0.00");
		assert_eq!(dec("+12").to_string(), "12");
		assert_eq!(dec(".5").to_string(), "0.5");
		assert_eq!(dec("5.").to_string(), "5");
		assert_eq!(dec("00012.300").to_string(), "12.300");
	}

	#[test]
	fn exponents_fold_into_the_coefficient() {
		assert_eq!(dec("1e3").to_string(), "1000");
		assert_eq!(dec("1e3").scale(), 0);
		assert_eq!(dec("1.5e3").to_string(), "1500");
		assert_eq!(dec("1.5e3").scale(), 0);
		assert_eq!(dec("1.50E-2").to_string(), "0.0150");
		assert_eq!(dec("0e-3").to_string(), "0.000");
		assert_eq!(dec("-2.5e0").to_string(), "-2.5");
	}

	#[test]
	fn parse_rejects_malformed_text() {
		for text in ["", "-", "+", ".", "e5", "1e", "1e+", "1.2.3", "12a", "1_000", "--1", " 1"] {
			assert!(text.parse::<BigDecimal>().is_err(), "accepted {text:?}");
		}
		assert_eq!("".parse::<BigDecimal>(), Err(BigDecimalError::Empty));
		assert_eq!("1e".parse::<BigDecimal>(), Err(BigDecimalError::InvalidSyntax));
	}

	#[test]
	fn parse_rejects_non_finite() {
		for text in ["NaN", "nan", "-NaN", "inf", "-inf", "+Infinity", "INFINITY"] {
			assert_eq!(
				text.parse::<BigDecimal>(),
				Err(BigDecimalError::NonFinite),
				"text {text:?}"
			);
		}
	}

	#[test]
	fn parse_rejects_large_exponents() {
		assert_eq!("1e1000000".parse::<BigDecimal>(), Err(BigDecimalError::ExponentOutOfRange));
		assert_eq!(
			"1e-99999999999999999999999".parse::<BigDecimal>(),
			Err(BigDecimalError::ExponentOutOfRange)
		);
		assert!("1e147454".parse::<BigDecimal>().is_ok());
		assert_eq!("1e147455".parse::<BigDecimal>(), Err(BigDecimalError::TooManyDigits));
	}

	#[test]
	fn parse_accepts_65_digit_values() {
		let text = format!("{}.{}", "7".repeat(40), "3".repeat(25));
		let value = dec(&text);
		assert_eq!(value.to_string(), text);
		assert_eq!(value.precision(), 65);
	}

	#[test]
	fn parse_accepts_maximum_digits_and_refuses_one_more() {
		let max = "7".repeat(MAX_DIGITS);
		assert_eq!(dec(&max).to_string(), max);
		let over = "7".repeat(MAX_DIGITS + 1);
		assert_eq!(over.parse::<BigDecimal>(), Err(BigDecimalError::TooManyDigits));
	}

	#[test]
	fn leading_zeros_are_not_significant() {
		let text = format!("{}1", "0".repeat(200_000));
		assert_eq!(dec(&text).to_string(), "1");
	}

	#[test]
	fn scale_limit_is_enforced() {
		let fits = format!("0.{}1", "0".repeat(MAX_SCALE as usize - 1));
		assert_eq!(dec(&fits).scale(), MAX_SCALE);
		let too_far = format!("0.{}1", "0".repeat(MAX_SCALE as usize));
		assert_eq!(too_far.parse::<BigDecimal>(), Err(BigDecimalError::ScaleOutOfRange));
	}

	#[test]
	fn equality_and_hash_are_numeric() {
		assert_eq!(dec("1.50"), dec("1.5"));
		assert_eq!(hash_of(&dec("1.50")), hash_of(&dec("1.5")));
		assert_eq!(hash_of(&dec("0.00")), hash_of(&dec("-0")));
		assert_eq!(hash_of(&dec("1500")), hash_of(&dec("1.5e3")));
		assert_ne!(dec("1.5"), dec("1.6"));
	}

	#[test]
	fn ordering_is_numeric() {
		let ordered = [
			"-1000", "-1.5", "-1", "-0.001", "0", "0.001", "0.1", "1", "1.5", "1.50001", "10",
			"100",
		];
		for pair in ordered.windows(2) {
			assert!(dec(pair[0]) < dec(pair[1]), "{} < {}", pair[0], pair[1]);
		}
		assert_eq!(dec("-0").cmp(&dec("0.00")), Ordering::Equal);
	}

	#[test]
	fn normalized_strips_fractional_zeros() {
		let value = dec("1.500").normalized();
		assert_eq!(value.to_string(), "1.5");
		assert_eq!(value.scale(), 1);
		assert_eq!(dec("1500").normalized().to_string(), "1500");
		assert_eq!(dec("1500").normalized().scale(), 0);
		assert_eq!(dec("0.000").normalized().to_string(), "0");
		assert_eq!(dec("0.000").normalized().scale(), 0);
	}

	#[test]
	fn precision_counts_significant_digits() {
		assert_eq!(dec("1.50").precision(), 2);
		assert_eq!(dec("1500").precision(), 2);
		assert_eq!(dec("123.456").precision(), 6);
		assert_eq!(dec("0.00").precision(), 1);
	}

	#[test]
	fn zero_sign_and_abs() {
		assert!(dec("-0.00").is_zero());
		assert_eq!(dec("-0.00").signum(), 0);
		assert_eq!(dec("-3.5").signum(), -1);
		assert_eq!(dec("3.5").signum(), 1);
		assert_eq!(dec("-3.50").abs().to_string(), "3.50");
		assert_eq!((-dec("2.0")).to_string(), "-2.0");
		assert_eq!((-dec("0.0")).to_string(), "0.0");
	}

	#[test]
	fn checked_add_and_sub() {
		assert_eq!(dec("1.5").checked_add(&dec("2.25")).unwrap().to_string(), "3.75");
		assert_eq!(dec("1").checked_sub(&dec("1.001")).unwrap().to_string(), "-0.001");
		assert_eq!(dec("1.50").checked_add(&dec("0.5")).unwrap().scale(), 2);
	}

	#[test]
	fn checked_mul_is_exact() {
		let product = dec("1.5").checked_mul(&dec("2.00")).unwrap();
		assert_eq!(product.to_string(), "3.000");
		assert_eq!(dec("-0.5").checked_mul(&dec("0.5")).unwrap().to_string(), "-0.25");
	}

	#[test]
	fn parsed_text_round_trips_through_its_display() {
		let widest = format!("9e{}", MAX_DIGITS - 1);
		let parsed = widest.parse::<BigDecimal>().unwrap();
		assert_eq!(parsed.to_string().parse::<BigDecimal>().unwrap(), parsed);
		assert_eq!(
			format!("9e{MAX_DIGITS}").parse::<BigDecimal>(),
			Err(BigDecimalError::TooManyDigits)
		);
		assert!(format!("0e{MAX_DIGITS}").parse::<BigDecimal>().is_ok());
	}

	#[test]
	fn far_apart_magnitudes_order_without_rescaling() {
		let tiny = BigDecimal::from_parts(BigInt::from(1), 147_000);
		let one = BigDecimal::from_parts(BigInt::from(1), 0);
		let pairs = [
			(one.clone(), tiny.clone(), Ordering::Greater),
			(-one.clone(), -tiny.clone(), Ordering::Less),
			(-one, tiny.clone(), Ordering::Less),
			(BigDecimal::default(), tiny, Ordering::Less),
			("1.5".parse().unwrap(), "2.25".parse().unwrap(), Ordering::Less),
			("-15".parse().unwrap(), "-15.000".parse().unwrap(), Ordering::Equal),
			("99.9".parse().unwrap(), "100".parse().unwrap(), Ordering::Less),
		];
		for (lhs, rhs, expected) in pairs {
			assert_eq!(lhs.cmp(&rhs), expected, "{lhs} vs {rhs}");
			assert_eq!(rhs.cmp(&lhs), expected.reverse(), "{rhs} vs {lhs}");
		}
	}

	#[test]
	fn arithmetic_refuses_a_coefficient_storage_cannot_read_back() {
		let nines: BigDecimal = "9".repeat(70_000).parse().unwrap();
		let square = nines.checked_mul(&nines).expect("140000 digits fit");
		assert!(square.checked_mul(&nines).is_none(), "210000 digits do not fit");
		assert!(square.checked_mul(&square).is_none(), "280000 digits do not fit");
		// The widest value text can spell: one more digit is refused, as text would be.
		let widest: BigDecimal = "9".repeat(MAX_DIGITS).parse().unwrap();
		assert!(widest.checked_add(&BigDecimal::from(1i64)).is_none());
		assert!(widest.checked_sub(&-widest.clone()).is_none());
		assert!(widest.round(1).is_err(), "a padded digit past the limit");
		let less = widest.checked_sub(&BigDecimal::from(1i64)).unwrap();
		assert_eq!(less.to_string().parse::<BigDecimal>().unwrap(), less);
	}

	#[test]
	fn checked_mul_refuses_scale_overflow() {
		let big = dec(&format!("0.{}1", "0".repeat(147_456)));
		assert_eq!(big.scale(), 147_457);
		assert!(big.checked_mul(&big).is_none());
	}

	#[test]
	fn division_rounds_half_away_from_zero() {
		assert_eq!(dec("1").div(&dec("3"), 5).unwrap().to_string(), "0.33333");
		assert_eq!(dec("2").div(&dec("3"), 2).unwrap().to_string(), "0.67");
		assert_eq!(dec("-2").div(&dec("3"), 2).unwrap().to_string(), "-0.67");
		assert_eq!(dec("1").div(&dec("8"), 2).unwrap().to_string(), "0.13");
		assert_eq!(dec("-1").div(&dec("8"), 2).unwrap().to_string(), "-0.13");
		assert_eq!(dec("10").div(&dec("4"), 0).unwrap().to_string(), "3");
		assert_eq!(dec("-10").div(&dec("4"), 0).unwrap().to_string(), "-3");
	}

	#[test]
	fn division_by_zero_is_an_error() {
		assert_eq!(dec("1").div(&dec("0.00"), 2), Err(BigDecimalError::DivisionByZero));
		assert_eq!(dec("1").div_default(&dec("0")), Err(BigDecimalError::DivisionByZero));
	}

	#[test]
	fn default_division_uses_twenty_four_digits() {
		let quotient = dec("1").div_default(&dec("3")).unwrap();
		assert_eq!(quotient.scale(), 24);
		assert_eq!(quotient.to_string(), format!("0.{}", "3".repeat(24)));
		// The operand has 26 fractional digits, so the default is 26 + 4 = 30.
		assert_eq!(dec("1.00000000000000000000000000").div_default(&dec("1")).unwrap().scale(), 30);
	}

	#[test]
	fn round_handles_ties_in_both_signs() {
		assert_eq!(dec("2.5").round(0).unwrap().to_string(), "3");
		assert_eq!(dec("-2.5").round(0).unwrap().to_string(), "-3");
		assert_eq!(dec("2.4999").round(0).unwrap().to_string(), "2");
		assert_eq!(dec("0.125").round(2).unwrap().to_string(), "0.13");
		assert_eq!(dec("-0.125").round(2).unwrap().to_string(), "-0.13");
		assert_eq!(dec("0.135").round(2).unwrap().to_string(), "0.14");
	}

	#[test]
	fn round_with_negative_dp() {
		let rounded = dec("1234.5678").round(-2).unwrap();
		assert_eq!(rounded.to_string(), "1200");
		assert_eq!(rounded.scale(), 0);
		assert_eq!(dec("1250").round(-2).unwrap().to_string(), "1300");
		assert_eq!(dec("-1250").round(-2).unwrap().to_string(), "-1300");
	}

	#[test]
	fn round_pads_to_requested_scale() {
		let padded = dec("1.5").round(3).unwrap();
		assert_eq!(padded.to_string(), "1.500");
		assert_eq!(padded.scale(), 3);
		assert_eq!(dec("1.5").rescale(0).unwrap().to_string(), "2");
	}

	#[test]
	fn trunc_floor_ceil() {
		assert_eq!(dec("-2.9").trunc(0).unwrap().to_string(), "-2");
		assert_eq!(dec("1234.5678").trunc(-2).unwrap().to_string(), "1200");
		assert_eq!(dec("-2.1").floor().unwrap().to_string(), "-3");
		assert_eq!(dec("2.9").floor().unwrap().to_string(), "2");
		assert_eq!(dec("-2.9").ceil().unwrap().to_string(), "-2");
		assert_eq!(dec("2.1").ceil().unwrap().to_string(), "3");
		assert_eq!(dec("2").floor().unwrap().scale(), 0);
		assert_eq!(dec("-4.0").ceil().unwrap().to_string(), "-4");
	}

	#[test]
	fn integer_conversions() {
		assert_eq!(BigDecimal::from(-5i64).to_string(), "-5");
		assert_eq!(BigDecimal::from(u64::MAX).to_string(), u64::MAX.to_string());
		assert_eq!(BigDecimal::from(i128::MIN).to_string(), i128::MIN.to_string());
		assert_eq!(BigDecimal::from(u128::MAX).to_string(), u128::MAX.to_string());
		assert_eq!(BigDecimal::from(Decimal::new(150, 2)).to_string(), "1.50");
	}

	#[test]
	fn to_i64_and_to_u64_are_exact() {
		assert_eq!(dec("42").to_i64(), Some(42));
		assert_eq!(dec("42.0").to_i64(), Some(42));
		assert_eq!(dec("42.5").to_i64(), None);
		assert_eq!(dec("9223372036854775808").to_i64(), None);
		assert_eq!(dec("-9223372036854775808").to_i64(), Some(i64::MIN));
		assert_eq!(dec("-1").to_u64(), None);
		assert_eq!(dec("18446744073709551615").to_u64(), Some(u64::MAX));
	}

	#[test]
	fn to_f64_is_nearest() {
		assert_eq!(dec("1.5").to_f64(), 1.5);
		assert_eq!(dec("0.1").to_f64(), 0.1);
		assert!(dec("1e400").to_f64().is_infinite());
		assert_eq!(dec("1e-400").to_f64(), 0.0);
	}

	#[test]
	fn try_from_f64_uses_shortest_text() {
		assert_eq!(BigDecimal::try_from_f64(0.1).unwrap().to_string(), "0.1");
		assert_eq!(BigDecimal::try_from_f64(2.5).unwrap().to_string(), "2.5");
		assert_eq!(
			BigDecimal::try_from_f64(1e21).unwrap().to_string(),
			format!("1{}", "0".repeat(21))
		);
		assert_eq!(BigDecimal::try_from_f64(-0.0).unwrap().to_string(), "0");
		assert_eq!(BigDecimal::try_from_f64(f64::NAN), Err(BigDecimalError::NonFiniteFloat));
		assert_eq!(BigDecimal::try_from_f64(f64::INFINITY), Err(BigDecimalError::NonFiniteFloat));
	}

	#[test]
	fn rust_decimal_conversion_is_exact() {
		let fits = Decimal::try_from(&dec("1.50")).unwrap();
		assert_eq!(fits, Decimal::new(15, 1));
		assert_eq!(fits.to_string(), "1.5");
		// 1 + 10^-28: the coefficient is 10^28 + 1 at scale 28.
		assert_eq!(
			Decimal::try_from(&dec(&format!("1.{}1", "0".repeat(27)))).unwrap(),
			Decimal::from_i128_with_scale(10_000_000_000_000_000_000_000_000_001, 28)
		);
		assert_eq!(
			Decimal::try_from(&dec("1e28")).unwrap(),
			Decimal::from_i128_with_scale(10_000_000_000_000_000_000_000_000_000, 0)
		);
	}

	#[test]
	fn rust_decimal_conversion_refuses_what_does_not_fit() {
		let does_not_fit = BigDecimalError::DoesNotFit("rust_decimal::Decimal");
		assert_eq!(Decimal::try_from(&dec("1e29")), Err(does_not_fit.clone()));
		// Scale 29 is one digit more than rust_decimal supports.
		assert_eq!(Decimal::try_from(&dec(&format!("0.{}1", "0".repeat(28)))), Err(does_not_fit));
	}

	#[test]
	fn pg_numeric_layout() {
		assert_eq!(dec("1234.5678").to_pg_numeric().unwrap(), (2, 0, 0x0000, 4, vec![1234, 5678]));
		assert_eq!(dec("-0.0001").to_pg_numeric().unwrap(), (1, -1, 0x4000, 4, vec![1]));
		assert_eq!(dec("0.00").to_pg_numeric().unwrap(), (0, 0, 0, 2, Vec::new()));
		assert_eq!(dec("10000").to_pg_numeric().unwrap(), (1, 1, 0x0000, 0, vec![1]));
	}

	#[test]
	fn pg_numeric_refuses_out_of_limit_scale() {
		let deep = dec(&format!("0.{}1", "0".repeat(16_383)));
		assert!(matches!(deep.to_pg_numeric(), Err(BigDecimalError::PgNumeric(_))));
	}

	#[test]
	fn pg_numeric_round_trip_on_samples() {
		for text in [
			"0",
			"1",
			"-1",
			"1234.5678",
			"-0.0001",
			"10000",
			"123456789.000000001",
			"1e100",
			"1.5e-7",
		] {
			let value = dec(text);
			let (ndigits, weight, sign, dscale, digits) = value.to_pg_numeric().unwrap();
			let back = BigDecimal::from_pg_numeric(ndigits, weight, sign, dscale, &digits).unwrap();
			assert_eq!(back, value, "text {text}");
			assert_eq!(back.scale(), value.scale(), "text {text}");
		}
	}

	#[test]
	fn pg_numeric_rejects_malformed_parts() {
		assert!(BigDecimal::from_pg_numeric(1, 0, 0xC000, 0, &[1]).is_err());
		assert!(BigDecimal::from_pg_numeric(2, 0, 0, 0, &[1]).is_err());
		assert!(BigDecimal::from_pg_numeric(1, 0, 0, 0, &[10000]).is_err());
		assert!(BigDecimal::from_pg_numeric(1, 0, 0, 0, &[-1]).is_err());
		assert!(BigDecimal::from_pg_numeric(1, 0, 0, 16_384, &[1]).is_err());
		// Group 0.5 under dscale 2 would need digits beyond the display scale.
		assert!(BigDecimal::from_pg_numeric(1, -1, 0, 2, &[5001]).is_err());
		// Group 5000 at weight -1 is 0.5, which is exact at dscale 2.
		assert_eq!(BigDecimal::from_pg_numeric(1, -1, 0, 2, &[5000]).unwrap().to_string(), "0.50");
	}

	#[test]
	fn encoding_orders_like_numbers() {
		let samples = [
			"-1000", "-1.5", "-1", "-0.001", "0", "0.001", "0.1", "0.10001", "1", "1.5", "1.50001",
			"10", "100", "1e100", "-1e100",
		];
		let mut encodings: Vec<(Vec<u8>, BigDecimal)> =
			samples.iter().map(|text| (encoded(&dec(text)), dec(text))).collect();
		encodings.sort_by(|a, b| a.0.cmp(&b.0));
		for pair in encodings.windows(2) {
			assert!(pair[0].1 <= pair[1].1, "{} then {}", pair[0].1, pair[1].1);
		}
	}

	#[test]
	fn encoding_is_canonical_for_equal_values() {
		assert_eq!(encoded(&dec("1.50")), encoded(&dec("1.5")));
		assert_eq!(encoded(&dec("1500")), encoded(&dec("1.5e3")));
		assert_eq!(encoded(&dec("0.00")), encoded(&dec("-0")));
	}

	#[test]
	fn decode_round_trips_to_normalized_value() {
		for text in ["0", "-0.000", "1.50", "-2.5", "1e10", "123456.789", "1e-30"] {
			let value = dec(text);
			let bytes = encoded(&value);
			let (decoded, used) = BigDecimal::decode_ordered(&bytes).unwrap();
			assert_eq!(used, bytes.len(), "text {text}");
			assert_eq!(decoded, value.normalized(), "text {text}");
		}
	}

	#[test]
	fn decode_consumes_concatenated_encodings() {
		let mut bytes = encoded(&dec("1.5"));
		bytes.extend(encoded(&dec("-7")));
		let (first, used) = BigDecimal::decode_ordered(&bytes).unwrap();
		let (second, rest) = BigDecimal::decode_ordered(&bytes[used..]).unwrap();
		assert_eq!(first, dec("1.5"));
		assert_eq!(second, dec("-7"));
		assert_eq!(used + rest, bytes.len());
	}

	#[test]
	fn decode_rejects_bad_input() {
		assert!(BigDecimal::decode_ordered(&[]).is_err());
		assert!(BigDecimal::decode_ordered(&[0x07]).is_err());
		assert!(BigDecimal::decode_ordered(&[POSITIVE_TAG, 0x80]).is_err());
		let mut truncated = encoded(&dec("12.5"));
		truncated.pop();
		assert!(BigDecimal::decode_ordered(&truncated).is_err());
		let mut trailing_zero = encoded(&dec("10"));
		let last = trailing_zero.len() - 1;
		trailing_zero.insert(last, 0x01);
		assert!(BigDecimal::decode_ordered(&trailing_zero).is_err());
	}

	#[test]
	fn decode_never_panics_on_random_bytes() {
		let mut rng = Xorshift(0x9E37_79B9_7F4A_7C15);
		for _ in 0..20_000 {
			let len = rng.below(40) as usize;
			let mut bytes: Vec<u8> = (0..len).map(|_| rng.next_u64() as u8).collect();
			if let Some(first) = bytes.first_mut() {
				*first = [NEGATIVE_TAG, ZERO_TAG, POSITIVE_TAG][rng.below(3) as usize];
			}
			let _ = BigDecimal::decode_ordered(&bytes);
		}
	}

	#[test]
	fn random_values_round_trip_through_text_and_encoding() {
		let mut rng = Xorshift(0x2545_F491_4F6C_DD1D);
		for _ in 0..2_000 {
			let value = random_decimal(&mut rng);
			let text = value.to_string();
			let reparsed: BigDecimal = text.parse().unwrap();
			assert_eq!(reparsed, value);
			assert_eq!(reparsed.scale(), value.scale(), "text {text}");
			let bytes = encoded(&value);
			let (decoded, used) = BigDecimal::decode_ordered(&bytes).unwrap();
			assert_eq!(used, bytes.len());
			assert_eq!(decoded, value.normalized());
		}
	}

	#[test]
	fn random_values_encoding_order_matches_numeric_order() {
		let mut rng = Xorshift(0xDEAD_BEEF_CAFE_F00D);
		for _ in 0..2_000 {
			let a = random_decimal(&mut rng);
			let b = random_decimal(&mut rng);
			assert_eq!(encoded(&a).cmp(&encoded(&b)), a.cmp(&b), "{a} vs {b}");
		}
	}

	#[test]
	fn random_values_pg_numeric_round_trip() {
		let mut rng = Xorshift(0x0123_4567_89AB_CDEF);
		for _ in 0..2_000 {
			let value = random_decimal(&mut rng);
			let (ndigits, weight, sign, dscale, digits) = value.to_pg_numeric().unwrap();
			let back = BigDecimal::from_pg_numeric(ndigits, weight, sign, dscale, &digits).unwrap();
			assert_eq!(back, value);
			assert_eq!(back.scale(), value.scale());
		}
	}

	#[test]
	fn random_values_agree_with_rust_decimal() {
		let mut rng = Xorshift(0x5DEE_CE66_D1CE_4E5B);
		for _ in 0..2_000 {
			let a = random_decimal(&mut rng);
			let b = random_decimal(&mut rng);
			let (Ok(da), Ok(db)) = (Decimal::try_from(&a), Decimal::try_from(&b)) else {
				continue;
			};
			assert_eq!(a.cmp(&b), da.cmp(&db), "{a} vs {b}");
			// rust_decimal rounds results it cannot hold exactly, while this type keeps them
			// exact and refuses the conversion. Compare only where the exact result fits.
			let exact_sum = a.checked_add(&b).and_then(|v| Decimal::try_from(&v).ok());
			if let (Some(expected), Some(actual)) = (da.checked_add(db), exact_sum) {
				assert_eq!(actual, expected, "{a} + {b}");
			}
			let exact_difference = a.checked_sub(&b).and_then(|v| Decimal::try_from(&v).ok());
			if let (Some(expected), Some(actual)) = (da.checked_sub(db), exact_difference) {
				assert_eq!(actual, expected, "{a} - {b}");
			}
			let exact_product = a.checked_mul(&b).and_then(|v| Decimal::try_from(&v).ok());
			if let (Some(expected), Some(actual)) = (da.checked_mul(db), exact_product) {
				assert_eq!(actual, expected, "{a} * {b}");
			}
			let dp = rng.below(7) as i64;
			let expected =
				da.round_dp_with_strategy(dp as u32, RoundingStrategy::MidpointAwayFromZero);
			let actual = Decimal::try_from(&a.round(dp).unwrap()).unwrap();
			assert_eq!(actual, expected, "round({a}, {dp})");
		}
	}
}
