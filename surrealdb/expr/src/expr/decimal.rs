//! Order-preserving byte encoding of numbers for index keys.
//!
//! [`DecimalLexEncoder`] turns a [`LexDecimal`] into a variable-length byte
//! string whose bytewise order matches numeric order, which is what lets
//! index keys built from numbers be range-scanned. The encoding is stream
//! friendly: the mantissa carries an in-band terminator and the encoder appends
//! a trailing 0x00, so decoders can read until the first zero when values are
//! concatenated in composite keys.
//!
//! The bytes are a persisted format. Every [`Number`](crate::val::Number)
//! variant is converted to a [`LexDecimal`] first, and the digits that
//! conversion produces — including trailing zeros and, for floats, the exact
//! rounding described in `from_f64` — are part of that format.
//!
//! Where it’s used:
//! - val::number::Number::{as_decimal_buf, from_decimal_buf}
//! - key::value::StoreKeyNumber serde impls used in index key material
//!
//! Ordering overview:
//! - Finite negatives < zero < finite positives
//! - −∞ < all finite < +∞ < NaN
//!
//! See the struct-level documentation below for the precise byte format.

mod arith;
mod float;

use std::fmt::{self, Display, Formatter, Write};

use anyhow::Result;
use rust_decimal::Decimal;

use crate::expr::Error;

/// A number in the form the key encoding works on: `±coefficient ·
/// 10^exponent`, or one of the non-finite values.
///
/// The coefficient is kept as given, so `1000` (coefficient 1000, exponent 0)
/// and `1000.0` as a decimal (coefficient 10000, exponent -1) are distinct
/// values with distinct encodings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LexDecimal {
	NaN,
	Infinite {
		negative: bool,
	},
	Finite {
		negative: bool,
		coefficient: u128,
		exponent: i32,
	},
}

impl LexDecimal {
	pub const ZERO: LexDecimal = LexDecimal::Finite {
		negative: false,
		coefficient: 0,
		exponent: 0,
	};

	/// The integer exactly, with exponent 0.
	pub fn from_i64(v: i64) -> Self {
		LexDecimal::Finite {
			negative: v < 0,
			coefficient: v.unsigned_abs() as u128,
			exponent: 0,
		}
	}

	/// The decimal's own mantissa and scale, trailing zeros included.
	pub fn from_decimal(v: Decimal) -> Self {
		let mantissa = v.mantissa();
		LexDecimal::Finite {
			negative: mantissa < 0,
			coefficient: mantissa.unsigned_abs(),
			exponent: -(v.scale() as i32),
		}
	}

	/// The float's binary value in decimal: exact when the expansion fits a
	/// `u128` coefficient, otherwise rounded to as many digits as fit (see
	/// `from_f64` for the precise rounding). Integral floats keep their
	/// trailing zeros (`1000.0` has coefficient 1000); fractional ones have
	/// none, since the expansion of a binary fraction ends in 5.
	pub fn from_f64(v: f64) -> Self {
		if v.is_nan() {
			LexDecimal::NaN
		} else if v.is_infinite() {
			LexDecimal::Infinite {
				negative: v < 0.0,
			}
		} else if v == 0.0 {
			LexDecimal::Finite {
				negative: v.is_sign_negative(),
				coefficient: 0,
				exponent: 0,
			}
		} else {
			let (negative, coefficient, exponent) = float::from_f64(v);
			LexDecimal::Finite {
				negative,
				coefficient,
				exponent,
			}
		}
	}

	pub fn is_finite(&self) -> bool {
		matches!(self, LexDecimal::Finite { .. })
	}

	pub fn is_nan(&self) -> bool {
		matches!(self, LexDecimal::NaN)
	}

	/// Converts to a `rust_decimal::Decimal` by parsing the [`Display`] form,
	/// so the result follows that parser's rules for precision and range. Only
	/// values that [`Display`] writes positionally convert this way; see
	/// [`Self::to_decimal_exact`] for the conversion that keeps every Decimal.
	pub fn to_decimal(&self) -> Result<Decimal> {
		Ok(Decimal::from_str_radix(&self.to_string(), 10)?)
	}

	/// Converts to the `rust_decimal::Decimal` of exactly this value.
	///
	/// The inverse of [`LexDecimal::from_decimal`]: every Decimal converts back
	/// unchanged, whatever its scale. Trailing zeros past the 28th fractional
	/// place are dropped, since they are not significant. A value no Decimal
	/// holds exactly, a magnitude beyond the 96-bit mantissa or a significant
	/// digit past the 28th fractional place, and NaN and the infinities, are an
	/// error rather than a rounded value.
	pub fn to_decimal_exact(&self) -> Result<Decimal> {
		// Largest mantissa a Decimal holds: 2^96 - 1.
		const MAX_MANTISSA: u128 = (1 << 96) - 1;
		// Most fractional digits a Decimal holds.
		const MAX_SCALE: i32 = 28;

		let unrepresentable =
			|| anyhow::Error::new(Error::Serialization(format!("{self} is not a Decimal")));
		let LexDecimal::Finite {
			negative,
			coefficient,
			exponent,
		} = *self
		else {
			return Err(unrepresentable());
		};
		if coefficient == 0 {
			return Ok(Decimal::ZERO);
		}
		// The value is `coefficient * 10^exponent`.
		let mut mantissa = coefficient;
		let mut exponent = exponent;
		// A positive exponent is trailing zeros the integer has to spell out.
		while exponent > 0 {
			mantissa = mantissa.checked_mul(10).ok_or_else(unrepresentable)?;
			exponent -= 1;
		}
		// Past the Decimal's scale, only trailing zeros of the mantissa can go.
		while exponent < -MAX_SCALE && mantissa % 10 == 0 {
			mantissa /= 10;
			exponent += 1;
		}
		if exponent < -MAX_SCALE || mantissa > MAX_MANTISSA {
			return Err(unrepresentable());
		}
		let mantissa = mantissa as i128;
		Ok(Decimal::from_i128_with_scale(
			if negative {
				-mantissa
			} else {
				mantissa
			},
			(-exponent) as u32,
		))
	}

	/// Converts to the `rust_decimal::Decimal` that stands for this value, or
	/// `None` when none does.
	///
	/// A Decimal holds a 96-bit mantissa and at most 28 fractional digits, so
	/// the value is rounded, ties away from zero, to the nearest Decimal whose
	/// mantissa is below 2^96 and whose scale is at most 28. It is computed from
	/// the coefficient and exponent, never from the value's text.
	///
	/// A Decimal stands for a value only in the range `10^-6 <= |value| < 2^96`,
	/// and for zero. `None` is returned outside it, and for NaN and the
	/// infinities, and the caller carries the value as the nearest `f64` instead.
	pub fn to_decimal_rounded(&self) -> Option<Decimal> {
		// A Decimal's mantissa is 96 bits wide.
		const MANTISSA_LIMIT: u128 = 1 << 96;
		// A Decimal has at most 28 fractional digits.
		const MAX_SCALE: u32 = 28;
		// The power of ten of the leading digit of the smallest magnitude converted.
		const MIN_LEADING_EXPONENT: i32 = -6;

		let LexDecimal::Finite {
			negative,
			coefficient: mantissa,
			exponent,
		} = *self
		else {
			return None;
		};
		if mantissa == 0 {
			return Some(Decimal::ZERO);
		}
		let digits_count = mantissa.ilog10() as i32 + 1;
		if exponent + digits_count - 1 < MIN_LEADING_EXPONENT {
			return None;
		}
		let (kept, scale) = if exponent > 0 {
			// An integer: the mantissa followed by `exponent` zeros. A mantissa of at
			// least one outgrows 96 bits within 29 of them, so this stops early.
			let mut kept = mantissa;
			for _ in 0..exponent {
				kept = kept.checked_mul(10).filter(|kept| *kept < MANTISSA_LIMIT)?;
			}
			(kept, 0)
		} else {
			let fraction = exponent.unsigned_abs();
			let pow10 = |n: u32| 10u128.checked_pow(n);
			// Drop the fewest trailing digits that bring the scale and the mantissa
			// within what a Decimal holds.
			let mut dropped = fraction.saturating_sub(MAX_SCALE);
			while mantissa / pow10(dropped)? >= MANTISSA_LIMIT {
				dropped += 1;
			}
			// A digit left of the point cannot be dropped.
			if dropped > fraction {
				return None;
			}
			let mut kept = mantissa / pow10(dropped)?;
			let mut scale = fraction - dropped;
			if dropped > 0 && mantissa / pow10(dropped - 1)? % 10 >= 5 {
				kept += 1;
				// Rounding up carried into a 97th bit: keep one digit fewer.
				if kept >= MANTISSA_LIMIT {
					if scale == 0 {
						return None;
					}
					kept = (kept + 4) / 10;
					scale -= 1;
				}
			}
			(kept, scale)
		};
		// `kept` is below 2^96, so it is an `i128` and a Decimal's mantissa.
		let kept = kept as i128;
		Decimal::try_from_i128_with_scale(
			if negative {
				-kept
			} else {
				kept
			},
			scale,
		)
		.ok()
	}

	/// Converts to an `f64`: the nearest one, except that a coefficient
	/// beyond 64 bits whose leading digits straddle a rounding boundary is
	/// resolved by the guarded fallback described in `float`, which can land
	/// one unit in the last place away. Values beyond the `f64` range become
	/// infinite or zero.
	pub fn to_f64(&self) -> f64 {
		match *self {
			LexDecimal::NaN => f64::NAN,
			LexDecimal::Infinite {
				negative,
			} => {
				if negative {
					f64::NEG_INFINITY
				} else {
					f64::INFINITY
				}
			}
			LexDecimal::Finite {
				negative,
				coefficient,
				exponent,
			} => float::to_f64(negative, coefficient, exponent),
		}
	}
}

/// Plain notation, switching to scientific when the decimal point lies more
/// than 5 zeros left of the first digit (`1.5E-7`) or more than 15 zeros
/// right of the last (`15e+16`). This is the text [`LexDecimal::to_decimal`]
/// parses and integer keys are read back through.
impl Display for LexDecimal {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match *self {
			LexDecimal::NaN => f.write_str("NaN"),
			LexDecimal::Infinite {
				negative,
			} => f.write_str(if negative {
				"-Inf"
			} else {
				"Inf"
			}),
			LexDecimal::Finite {
				negative,
				coefficient,
				exponent,
			} => {
				let digits = coefficient.to_string();
				let len = digits.len() as i64;
				let scale = -(exponent as i64);
				if negative {
					f.write_char('-')?;
				}
				let leading_zeros = if scale >= 0 {
					(scale - len).max(0)
				} else {
					0
				};
				let trailing_zeros = if scale < 0 {
					-scale
				} else {
					0
				};
				if leading_zeros > 5 {
					let (first, rest) = digits.split_at(1);
					f.write_str(first)?;
					if !rest.is_empty() {
						f.write_char('.')?;
						f.write_str(rest)?;
					}
					write!(f, "E{:+}", len - scale - 1)
				} else if trailing_zeros > 15 {
					write!(f, "{digits}e{:+}", -scale)
				} else if scale <= 0 {
					f.write_str(&digits)?;
					for _ in 0..trailing_zeros {
						f.write_char('0')?;
					}
					Ok(())
				} else if scale < len {
					let (int, frac) = digits.split_at((len - scale) as usize);
					write!(f, "{int}.{frac}")
				} else {
					f.write_str("0.")?;
					for _ in 0..leading_zeros {
						f.write_char('0')?;
					}
					f.write_str(&digits)
				}
			}
		}
	}
}

/// Variable-length lexicographic encoding for [`LexDecimal`] values that
/// preserves sort order.
///
/// This encoder converts values into byte sequences that maintain the same
/// lexicographic ordering as the original decimal values. This is crucial for
/// database indexing where byte-level comparison must match numeric comparison.
///
/// ## Encoding Format
///
/// The encoding uses a variable-length format optimized for lexicographic
/// ordering:
///
/// ### Zero Values
/// - Zero is encoded as a single byte: `0x80`
/// - This ensures zero sorts between negative and positive numbers
///
/// ### Non-Zero Values
/// The format consists of:
/// 1. **Class/marker byte** (1 byte):
///    - `0x80` Zero
///    - `0x40` Finite negative
///    - `0xA0` Finite positive
///    - `0x20` Negative infinity
///    - `0xC0` Positive infinity
///    - `0xFF` NaN
///
/// 2. **Biased scale** (2 bytes):
///    - We bias the "scale" (not the raw exponent). Scale is defined as: `scale = exponent +
///      (digit_count - 1)`, i.e., the position of the most-significant digit in a
///      scientific-notation sense.
///    - Biased as `biased = scale + EXP_BIAS` (unsigned 16-bit), then written as the two base-255
///      digits of `biased`, each plus one, so neither byte is ever 0x00.
///    - For negative numbers: `0xFFFF - biased` is written instead, to reverse order
///    - EXP_BIAS = 6144. Coefficients have at most 39 digits and exponents of encoded values stay
///      within `[-6143, +6144]`, so `scale + EXP_BIAS` stays well within `u16`.
///
/// 3. **Packed digit representation** (variable length):
///    - Digits are taken from the coefficient's base-10 representation
///    - Each pair of digits is packed into one byte (4 bits per digit)
///    - For positive numbers: stored as-is
///    - For negative numbers: all bytes are bitwise complemented to reverse ordering
///    - Termination: encoding stops when a nibble equals `0x0`. This naturally handles both odd and
///      even digit counts: • odd count: the last byte has a low nibble of 0 • even count: an extra
///      full terminator byte is appended (0x0F for positives, 0xF0 for negatives)
///
/// Because a terminator is always present within (or immediately after) the
/// mantissa, any trailing type-marker byte appended by higher layers will never
/// be consumed by the mantissa decoder.
///
/// ## Properties
/// - Preserves lexicographic ordering: if `a < b` then `encode(a) < encode(b)`
/// - Variable length encoding (3+ bytes typical: 1 sign + 2 scale + packed digits)
/// - Uses packed digit encoding for efficient storage (2 digits per byte)
pub struct DecimalLexEncoder;

impl DecimalLexEncoder {
	/// Bias mapping the signed scale into u16 space.
	const EXP_BIAS: i32 = 6144;

	/// Largest scale a buffer may decode to. Values past it are not produced by
	/// the encoder and are refused rather than decoded.
	const MAX_DECODED_SCALE: i32 = 6177;

	const FINITE_NEGATIVE_MARKER: u8 = 0x40;
	const FINITE_POSITIVE_MARKER: u8 = 0xA0;

	// 0x80 = 128, middle value for proper ordering
	const ZERO_MARKER: u8 = 0x80;

	const INFINITE_NEGATIVE_MARKER: u8 = 0x20;
	const INFINITE_POSITIVE_MAKER: u8 = 0xC0;
	const NAN_MARKER: u8 = 0xFF;

	/// Encodes a value into a lexicographically ordered byte sequence.
	///
	/// The encoding preserves sort order: if `a < b` then `encode(a) <
	/// encode(b)`. This is essential for database indexing where byte-level
	/// comparison must match numeric comparison.
	pub fn encode(dec: LexDecimal) -> Vec<u8> {
		let mut out = Vec::with_capacity(24);
		Self::encode_into(dec, &mut out);
		out
	}

	/// Appends the encoding of `dec` to `out`.
	pub fn encode_into(dec: LexDecimal, out: &mut Vec<u8>) {
		let (negative, coefficient, exponent) = match dec {
			LexDecimal::NaN => {
				out.extend_from_slice(&[Self::NAN_MARKER, 0x00]);
				return;
			}
			LexDecimal::Infinite {
				negative: true,
			} => {
				out.extend_from_slice(&[Self::INFINITE_NEGATIVE_MARKER, 0x00]);
				return;
			}
			LexDecimal::Infinite {
				negative: false,
			} => {
				out.extend_from_slice(&[Self::INFINITE_POSITIVE_MAKER, 0x00]);
				return;
			}
			LexDecimal::Finite {
				coefficient: 0,
				..
			} => {
				out.extend_from_slice(&[Self::ZERO_MARKER, 0x00]);
				return;
			}
			LexDecimal::Finite {
				negative,
				coefficient,
				exponent,
			} => (negative, coefficient, exponent),
		};

		let mut digits = [0u8; 39];
		let digits = coefficient_digits(coefficient, &mut digits);

		// The scale positions the first digit as the most significant one, as in
		// scientific notation.
		let scale = exponent + (digits.len() as i32 - 1);
		let biased_exponent = (scale + Self::EXP_BIAS) as u16;

		let encode_exponent = |e: u16| {
			let q = (e / 255) as u8;
			let r = (e % 255) as u8;
			[q + 1, r + 1]
		};

		out.reserve(4 + digits.len().div_ceil(2) + 1);
		if negative {
			// The marker sorts negatives first; the complemented scale and digits
			// make larger magnitudes sort earlier among them.
			out.push(Self::FINITE_NEGATIVE_MARKER);
			out.extend(encode_exponent(0xFFFF - biased_exponent));
			Self::pack_digits(digits, 0xFF, out);
		} else {
			out.push(Self::FINITE_POSITIVE_MARKER);
			out.extend(encode_exponent(biased_exponent));
			Self::pack_digits(digits, 0x00, out);
		}
		out.push(0x00);
	}

	/// Decodes a lexicographically encoded byte sequence back to a value.
	///
	/// Decoding stops at the mantissa terminator, so trailing bytes are
	/// ignored. The coefficient comes back with exactly the digits that were
	/// encoded.
	pub fn decode(bytes: &[u8]) -> Result<LexDecimal> {
		if bytes.is_empty() {
			return Err(Error::Serialization("Cannot decode from empty buffer".to_string()).into());
		}

		let negative = match bytes[0] {
			Self::ZERO_MARKER => return Ok(LexDecimal::ZERO),
			Self::INFINITE_NEGATIVE_MARKER => {
				return Ok(LexDecimal::Infinite {
					negative: true,
				});
			}
			Self::INFINITE_POSITIVE_MAKER => {
				return Ok(LexDecimal::Infinite {
					negative: false,
				});
			}
			Self::NAN_MARKER => return Ok(LexDecimal::NaN),
			Self::FINITE_NEGATIVE_MARKER => true,
			Self::FINITE_POSITIVE_MARKER => false,
			marker => {
				return Err(Error::Serialization(format!("Invalid marker byte: {marker}")).into());
			}
		};

		// Need at least 3 bytes: marker (1) + exponent (2)
		if bytes.len() < 3 {
			return Err(Error::Serialization(format!("Buffer too short: {}", bytes.len())).into());
		}

		let biased_exponent = Self::exponent_byte(bytes[1])? * 255 + Self::exponent_byte(bytes[2])?;
		let biased_exponent = if negative {
			0xFFFF - biased_exponent
		} else {
			biased_exponent
		};
		let scale = biased_exponent as i32 - Self::EXP_BIAS;
		if scale > Self::MAX_DECODED_SCALE {
			return Err(Error::Serialization(format!("Exponent out of range: {scale}")).into());
		}

		let flip = if negative {
			0xFF
		} else {
			0x00
		};
		let (coefficient, digit_count) = Self::unpack_digits(&bytes[3..], flip)?;
		if digit_count == 0 {
			return Err(Error::Serialization("Empty mantissa".to_string()).into());
		}
		Ok(LexDecimal::Finite {
			negative,
			coefficient,
			exponent: scale - (digit_count - 1),
		})
	}

	/// Reads one byte of a biased scale. Each byte is written shifted up by one
	/// so that none is `0x00`, which the stream terminator owns; a `0x00` here is
	/// not part of any encoding.
	fn exponent_byte(byte: u8) -> Result<u16> {
		match byte.checked_sub(1) {
			Some(byte) => Ok(u16::from(byte)),
			None => Err(Error::Serialization("Invalid exponent byte: 0".to_string()).into()),
		}
	}

	/// Packs digit values (0..=9) two per byte, mapping each digit `d` to the
	/// nibble `d + 1` so that a 0 nibble can serve as the terminator, and XORs
	/// every byte with `flip`. An odd count leaves a 0 low nibble in the last
	/// byte; an even count appends a 0x0F terminator byte (before the flip).
	fn pack_digits(digits: &[u8], flip: u8, buf: &mut Vec<u8>) {
		let mut iter = digits.chunks_exact(2);
		for pair in &mut iter {
			buf.push((((pair[0] + 1) << 4) | (pair[1] + 1)) ^ flip);
		}
		match iter.remainder().first() {
			Some(d) => buf.push(((d + 1) << 4) ^ flip),
			None => buf.push(0x0F ^ flip),
		}
	}

	/// Unpacks digits after XORing each byte with `flip`, stopping at the first
	/// 0 nibble. Returns the coefficient and the number of digits read.
	fn unpack_digits(buf: &[u8], flip: u8) -> Result<(u128, i32)> {
		let mut m = 0u128;
		let mut l = 0i32;
		for pack in buf {
			let pack = *pack ^ flip;
			for (half, nibble) in [("high", pack >> 4), ("low", pack & 0x0F)] {
				if nibble == 0 {
					return Ok((m, l));
				}
				if !(1..=10).contains(&nibble) {
					return Err(
						Error::Serialization(format!("Invalid {half} nibble: {nibble}")).into()
					);
				}
				m = m
					.checked_mul(10)
					.and_then(|m| m.checked_add((nibble - 1) as u128))
					.ok_or_else(|| Error::Serialization("Mantissa too large".to_string()))?;
				l += 1;
			}
		}
		Ok((m, l))
	}
}

/// Writes the decimal digit values (0..=9) of a non-zero coefficient into
/// `buf`, most significant first, and returns the written slice.
fn coefficient_digits(mut n: u128, buf: &mut [u8; 39]) -> &[u8] {
	const P19: u128 = 10_000_000_000_000_000_000;
	let mut end = buf.len();
	// Peel off 19-digit chunks so most of the work is in u64 arithmetic.
	while n >= P19 {
		let mut chunk = (n % P19) as u64;
		n /= P19;
		for _ in 0..19 {
			end -= 1;
			buf[end] = (chunk % 10) as u8;
			chunk /= 10;
		}
	}
	let mut rest = n as u64;
	while rest > 0 {
		end -= 1;
		buf[end] = (rest % 10) as u8;
		rest /= 10;
	}
	&buf[end..]
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::Number;
	use crate::val::number::NumberKind;

	fn finite(negative: bool, coefficient: u128, exponent: i32) -> LexDecimal {
		LexDecimal::Finite {
			negative,
			coefficient,
			exponent,
		}
	}

	fn test_cases() -> [LexDecimal; 32] {
		[
			LexDecimal::from_f64(f64::NEG_INFINITY),
			LexDecimal::from_f64(f64::MIN),
			finite(true, i128::MIN.unsigned_abs(), 0),
			LexDecimal::from_i64(i64::MIN),
			LexDecimal::from_i64(-1001),
			LexDecimal::from_i64(-1000),
			LexDecimal::from_i64(-999),
			LexDecimal::from_i64(-100),
			LexDecimal::from_i64(-10),
			LexDecimal::from_i64(-9),
			LexDecimal::from_f64(-3.15),
			LexDecimal::from_f64(-std::f64::consts::PI),
			LexDecimal::from_f64(-1.5f64),
			LexDecimal::from_i64(-1),
			LexDecimal::ZERO,
			LexDecimal::from_i64(1),
			LexDecimal::from_f64(1.5f64),
			LexDecimal::from_i64(2),
			LexDecimal::from_f64(std::f64::consts::PI),
			LexDecimal::from_f64(3.15),
			LexDecimal::from_i64(9),
			LexDecimal::from_i64(10),
			LexDecimal::from_i64(100),
			LexDecimal::from_i64(999),
			LexDecimal::from_i64(1000),
			LexDecimal::from_i64(1001),
			LexDecimal::from_i64(i64::MAX),
			finite(false, i128::MAX as u128, 0),
			finite(false, u128::MAX, 0),
			LexDecimal::from_f64(f64::MAX),
			LexDecimal::from_f64(f64::INFINITY),
			LexDecimal::from_f64(f64::NAN),
		]
	}

	#[test]
	fn test_encode_decode_roundtrip() {
		for (i, case) in test_cases().into_iter().enumerate() {
			let encoded = DecimalLexEncoder::encode(case);
			let decoded = DecimalLexEncoder::decode(&encoded).expect("Decode should succeed");
			assert_eq!(case, decoded, "Roundtrip failed for {i}: {case} != {decoded}");
		}
	}

	#[test]
	fn test_encode_terminate_with_zero() {
		for (i, case) in test_cases().into_iter().enumerate() {
			let encoded = DecimalLexEncoder::encode(case);
			assert_eq!(
				encoded.iter().filter(|&b| *b == 0x00).count(),
				1,
				"Encoded buffer should contains only one 0x00 - {i}: {case} {encoded:?}"
			);
			assert_eq!(
				encoded.iter().position(|&b| b == 0x00).unwrap(),
				encoded.len() - 1,
				"Encoded buffer should terminate with 0x00 - {i}: {case} {encoded:?}"
			);
		}
	}

	#[test]
	fn test_lexicographic_ordering() {
		for (i, window) in test_cases().windows(2).enumerate() {
			let b1 = DecimalLexEncoder::encode(window[0]);
			let b2 = DecimalLexEncoder::encode(window[1]);
			assert!(
				b1 < b2,
				"#{i} - {} < {} (after serialization) - {b1:?} < {b2:?}",
				window[0],
				window[1]
			);
		}
	}

	#[test]
	fn test_decode_empty_buffer() {
		let result = DecimalLexEncoder::decode(&[]);
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Cannot decode from empty buffer"));
	}

	#[test]
	fn test_decode_buffer_too_short() {
		let result = DecimalLexEncoder::decode(&[0xA0]);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Buffer too short"), "{err:?}");
	}

	#[test]
	fn test_decode_invalid_marker() {
		let result = DecimalLexEncoder::decode(&[0x42, 0x00, 0x00, 0x00]);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.to_string(), "Serialization error: Invalid marker byte: 66", "{err:?}");
	}

	#[test]
	fn test_decode_empty_mantissa() {
		let result = DecimalLexEncoder::decode(&[0xA0, 0x01, 0x01, 0x00]);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Empty mantissa"), "{err:?}");
	}

	#[test]
	fn test_decode_rejects_mantissa_overflow() {
		// Forty 9s cannot be a u128 coefficient.
		let mut bytes = vec![0xA0, 0x19, 0x19];
		bytes.extend(std::iter::repeat_n(0xAA, 20));
		bytes.push(0x0F);
		let err = DecimalLexEncoder::decode(&bytes).unwrap_err();
		assert!(err.to_string().contains("Mantissa too large"), "{err:?}");
	}

	/// One fixture line: the value, its key bytes, and what decoding returns.
	struct Frozen {
		value: Number,
		key: Vec<u8>,
		decoded: String,
		decoded_kind: String,
	}

	fn frozen() -> Vec<Frozen> {
		fn hex(s: &str) -> Vec<u8> {
			(0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
		}
		include_str!("decimal/number_keys.txt")
			.lines()
			.filter(|l| !l.is_empty() && !l.starts_with('#'))
			.map(|line| {
				let fields: Vec<&str> = line.split(' ').collect();
				let value: Vec<&str> = fields[0].split(':').collect();
				let value = match value[0] {
					"i" => Number::Int(value[1].parse().unwrap()),
					"d" => Number::Decimal(Decimal::from_i128_with_scale(
						value[1].parse().unwrap(),
						value[2].parse().unwrap(),
					)),
					"f" => {
						Number::Float(f64::from_bits(u64::from_str_radix(value[1], 16).unwrap()))
					}
					other => panic!("unknown kind {other}"),
				};
				Frozen {
					value,
					key: hex(fields[1]),
					decoded: fields[2].to_string(),
					decoded_kind: fields[3].to_string(),
				}
			})
			.collect()
	}

	fn repr(r: &Result<Number>) -> String {
		match r {
			Ok(Number::Int(v)) => format!("i:{v}"),
			Ok(Number::Float(v)) => format!("f:{:x}", v.to_bits()),
			Ok(Number::Decimal(d)) => format!("d:{}:{}", d.mantissa(), d.scale()),
			Err(_) => "err".to_string(),
		}
	}

	#[test]
	fn encodings_match_frozen_fixture() {
		let cases = frozen();
		assert!(cases.len() > 3000);
		for c in cases {
			assert_eq!(c.value.as_decimal_buf(), c.key, "key bytes changed for {:?}", c.value);
			assert_eq!(
				repr(&Number::from_decimal_buf(&c.key)),
				c.decoded,
				"decoding changed for {:?}",
				c.value
			);
			let kind = match c.value {
				Number::Int(_) => NumberKind::Int,
				Number::Float(_) => NumberKind::Float,
				Number::Decimal(_) => NumberKind::Decimal,
			};
			assert_eq!(
				repr(&Number::from_decimal_buf_kind(&c.key, kind)),
				c.decoded_kind,
				"kind-tagged decoding changed for {:?}",
				c.value
			);
		}
	}

	/// Within each variant, a smaller value has a smaller key.
	#[test]
	fn frozen_keys_preserve_order() {
		let cases = frozen();
		let mut ints: Vec<_> = cases.iter().filter(|c| matches!(c.value, Number::Int(_))).collect();
		let mut floats: Vec<_> = cases
			.iter()
			.filter(|c| matches!(c.value, Number::Float(f) if !f.is_nan() && f != 0.0))
			.collect();
		let mut decimals: Vec<_> =
			cases.iter().filter(|c| matches!(c.value, Number::Decimal(_))).collect();
		for set in [&mut ints, &mut floats, &mut decimals] {
			set.sort_by_key(|a| a.value);
			for w in set.windows(2) {
				if w[0].value < w[1].value {
					assert!(
						w[0].key < w[1].key,
						"{:?} < {:?} but keys are not",
						w[0].value,
						w[1].value
					);
				}
			}
		}
	}

	/// Across Int, Float and Decimal, for every pair of fixture values: the
	/// smaller value has the smaller key, NaN sorts above everything and -inf
	/// below everything.
	///
	/// Equal values carry no constraint, since trailing zeros are part of the
	/// format. A float's key holds its guarded decimal expansion, not its
	/// exact binary value, so a Float and a non-Float whose values agree to
	/// within the float's own precision (1e-12 relative) may order either way
	/// and are not compared.
	#[test]
	fn frozen_keys_preserve_order_across_kinds() {
		fn comparable(a: &Number, b: &Number) -> bool {
			let float = |n: &Number| matches!(n, Number::Float(_));
			if float(a) == float(b) {
				return true;
			}
			let (x, y) = (a.to_float(), b.to_float());
			(x - y).abs() > 1e-12 * x.abs().max(y.abs())
		}
		let cases = frozen();
		let (nan, rest): (Vec<_>, Vec<_>) =
			cases.iter().partition(|c| matches!(c.value, Number::Float(f) if f.is_nan()));
		assert!(!nan.is_empty());
		for n in &nan {
			assert_eq!(n.key, [0xFF, 0x00]);
			assert!(rest.iter().all(|c| c.key < n.key), "NaN must sort last");
		}
		for c in &rest {
			if c.value != Number::Float(f64::NEG_INFINITY) {
				assert!(c.key > vec![0x20, 0x00], "-inf must sort first");
			}
		}
		let mut compared = 0usize;
		for a in &rest {
			for b in &rest {
				if a.value < b.value && comparable(&a.value, &b.value) {
					compared += 1;
					assert!(a.key < b.key, "{:?} < {:?} but keys are not", a.value, b.value);
				}
			}
		}
		assert!(compared > 1_000_000);
	}

	#[test]
	fn test_display_matches_notation_thresholds() {
		let cases = [
			(finite(false, 15, -1), "1.5"),
			(finite(true, 15, -1), "-1.5"),
			(finite(false, 15, -6), "0.000015"),
			(finite(false, 15, -7), "0.0000015"),
			(finite(false, 15, -8), "1.5E-7"),
			(finite(false, 1, -7), "1E-7"),
			(finite(false, 1000, 0), "1000"),
			(finite(false, 15, 15), "15000000000000000"),
			(finite(false, 15, 16), "15e+16"),
			(finite(false, 12345, -2), "123.45"),
			(LexDecimal::ZERO, "0"),
			(LexDecimal::NaN, "NaN"),
		];
		for (value, text) in cases {
			assert_eq!(value.to_string(), text);
		}
	}

	/// A buffer in the encoder's layout for a finite value of the given digits
	/// and scale, built without going through `LexDecimal` so that it can hold a
	/// scale the encoder would never write.
	fn buffer(negative: bool, scale: i32, digits: &str) -> Vec<u8> {
		let digits: Vec<u8> = digits.bytes().map(|b| b - b'0').collect();
		let biased = (scale + DecimalLexEncoder::EXP_BIAS) as u16;
		let shift = |e: u16| [(e / 255) as u8 + 1, (e % 255) as u8 + 1];
		let mut buf = Vec::new();
		if negative {
			buf.push(DecimalLexEncoder::FINITE_NEGATIVE_MARKER);
			buf.extend(shift(0xFFFF - biased));
			DecimalLexEncoder::pack_digits(&digits, 0xFF, &mut buf);
		} else {
			buf.push(DecimalLexEncoder::FINITE_POSITIVE_MARKER);
			buf.extend(shift(biased));
			DecimalLexEncoder::pack_digits(&digits, 0x00, &mut buf);
		}
		buf.push(0x00);
		buf
	}

	/// Parses the text a [`LexDecimal`] displays as, with an optional sign,
	/// fractional digits and an `E` exponent, so that cases can be written as
	/// they are spelt in the docs.
	fn lex(text: &str) -> LexDecimal {
		let (negative, rest) = match text.strip_prefix('-') {
			Some(rest) => (true, rest),
			None => (false, text),
		};
		let (mantissa, exp) = match rest.split_once(['E', 'e']) {
			Some((mantissa, exp)) => (mantissa, exp.parse::<i32>().unwrap()),
			None => (rest, 0),
		};
		let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
		LexDecimal::Finite {
			negative,
			coefficient: format!("{int}{frac}").parse().unwrap(),
			exponent: exp - frac.len() as i32,
		}
	}

	#[test]
	fn decode_refuses_a_scale_past_the_largest_encoded() {
		for negative in [false, true] {
			// Scales of every f64 and Decimal, and up to the largest the decoder accepts.
			for scale in [-324, 0, 28, 308, 6177] {
				let decoded = DecimalLexEncoder::decode(&buffer(negative, scale, "1"));
				assert!(decoded.is_ok(), "scale {scale} negative={negative}: {decoded:?}");
			}
			for scale in [6178, 40_000, 50_000, 58_000] {
				let err = DecimalLexEncoder::decode(&buffer(negative, scale, "1"))
					.expect_err("a scale past the largest encoded one is not decodable");
				assert!(err.to_string().contains("Exponent out of range"), "{err:?}");
			}
		}
	}

	#[test]
	fn decode_refuses_a_zero_exponent_byte() {
		// The encoder shifts each exponent byte up by one, so none is ever 0x00.
		for marker in
			[DecimalLexEncoder::FINITE_POSITIVE_MARKER, DecimalLexEncoder::FINITE_NEGATIVE_MARKER]
		{
			for exponent in [[0x00, 0x01], [0x01, 0x00], [0x00, 0x00]] {
				let err =
					DecimalLexEncoder::decode(&[marker, exponent[0], exponent[1], 0x20, 0x00])
						.expect_err("a zero exponent byte is not part of any encoding");
				assert!(err.to_string().contains("Invalid exponent byte"), "{err:?}");
			}
		}
	}

	#[test]
	fn decode_never_panics_over_every_exponent_byte_pair() {
		for marker in
			[DecimalLexEncoder::FINITE_POSITIVE_MARKER, DecimalLexEncoder::FINITE_NEGATIVE_MARKER]
		{
			let digits: [u8; 2] = if marker == DecimalLexEncoder::FINITE_POSITIVE_MARKER {
				[0x20, 0x00]
			} else {
				[!0x20, 0xFF]
			};
			for hi in 0..=u8::MAX {
				for lo in 0..=u8::MAX {
					let decoded =
						DecimalLexEncoder::decode(&[marker, hi, lo, digits[0], digits[1]]);
					if hi == 0 || lo == 0 {
						assert!(
							decoded.is_err(),
							"[{marker:#x}, {hi}, {lo}] decoded as {decoded:?}"
						);
					}
				}
			}
		}
	}

	#[test]
	fn decode_refuses_a_mantissa_wider_than_the_coefficient() {
		// The largest coefficient, 2^128 - 1, decodes.
		let widest = u128::MAX.to_string();
		for negative in [false, true] {
			let decoded = DecimalLexEncoder::decode(&buffer(negative, 38, &widest))
				.expect("the widest coefficient is decodable");
			assert!(matches!(
				decoded,
				LexDecimal::Finite {
					coefficient: u128::MAX,
					..
				}
			));
			// One digit more, or the same number of digits with a larger value, is not.
			for too_wide in [format!("{widest}0"), "9".repeat(39), "9".repeat(80)] {
				let err = DecimalLexEncoder::decode(&buffer(negative, 38, &too_wide))
					.expect_err("a coefficient past 2^128 is not decodable");
				assert!(err.to_string().contains("Mantissa too large"), "{too_wide}: {err:?}");
			}
		}
	}

	#[test]
	fn to_decimal_rounded_reads_a_value_the_way_a_decimal_parses_it() {
		let cases: [(&str, &str); 17] = [
			("0", "0"),
			("1", "1"),
			("-1.50", "-1.50"),
			("100.000", "100.000"),
			("0.000001", "0.000001"),
			("-0.000001234", "-0.000001234"),
			("79228162514264337593543950335", "79228162514264337593543950335"),
			("7.9228162514264337593543950335", "7.9228162514264337593543950335"),
			// More fractional digits than a Decimal holds are rounded at the 28th, ties up.
			("0.1000000000000000055511151231257827", "0.1000000000000000055511151231"),
			("0.12345678901234567890123456785", "0.1234567890123456789012345679"),
			("-0.12345678901234567890123456785", "-0.1234567890123456789012345679"),
			("0.12345678901234567890123456784999", "0.1234567890123456789012345678"),
			// A mantissa past 96 bits gives up a fractional digit rather than an integer one.
			("7.9228162514264337593543950336", "7.922816251426433759354395034"),
			// Rounding up into a 97th bit keeps one digit fewer.
			("7.92281625142643375935439503355", "7.922816251426433759354395034"),
			("79228162514264337593543950335.4", "79228162514264337593543950335"),
			("1E+28", "10000000000000000000000000000"),
			("-3.4E+2", "-340"),
		];
		for (value, expected) in cases {
			let expected: Decimal = expected.parse().unwrap();
			let got =
				lex(value).to_decimal_rounded().unwrap_or_else(|| panic!("{value} has a Decimal"));
			assert_eq!(got.serialize(), expected.serialize(), "{value}: {got} != {expected}");
		}
	}

	#[test]
	fn to_decimal_rounded_has_nothing_for_what_a_decimal_does_not_stand_for() {
		for text in [
			// A magnitude a Decimal's mantissa cannot hold.
			"79228162514264337593543950336",
			"-79228162514264337593543950336",
			"79228162514264337593543950335.5",
			"1E+29",
			"1E+6144",
			"9999999999999999455752309870428160",
			// A magnitude below a millionth.
			"9.99999E-7",
			"1E-7",
			"-1.5E-10",
			"9.9999999999999994515327145420957165173E-21",
			"1E-6143",
		] {
			assert_eq!(lex(text).to_decimal_rounded(), None, "{text}");
		}
		for value in [
			LexDecimal::NaN,
			LexDecimal::Infinite {
				negative: false,
			},
			LexDecimal::Infinite {
				negative: true,
			},
		] {
			assert_eq!(value.to_decimal_rounded(), None, "{value}");
		}
	}

	/// Over values that [`Display`] writes positionally, a Decimal reads that text
	/// to the same Decimal, at the same scale, that `to_decimal_rounded` computes
	/// from the digits, or fails to read it exactly where `to_decimal_rounded` has
	/// none.
	#[test]
	fn to_decimal_rounded_agrees_with_parsing_the_positional_text() {
		use rand::{Rng, SeedableRng};

		let mut rng = rand::rngs::StdRng::seed_from_u64(0xDEC1);
		let mut positional = 0u32;
		for _ in 0..200_000 {
			let digits = rng.random_range(1..=39u32);
			let coefficient = match 10u128.checked_pow(digits) {
				Some(limit) => rng.random::<u128>() % limit,
				None => rng.random::<u128>(),
			};
			let value = LexDecimal::Finite {
				negative: rng.random(),
				coefficient,
				exponent: rng.random_range(-60..=2i32),
			};
			// Only text without an exponent is one a Decimal reads.
			if coefficient == 0 || value.to_string().contains(['e', 'E']) {
				continue;
			}
			positional += 1;
			let parsed = value.to_decimal();
			let rounded = value.to_decimal_rounded();
			match (&parsed, &rounded) {
				(Ok(parsed), Some(rounded)) => {
					assert_eq!(
						parsed.serialize(),
						rounded.serialize(),
						"{value}: {parsed} != {rounded}"
					);
				}
				(Err(_), None) => {}
				_ => panic!("{value}: parsed as {parsed:?}, rounded to {rounded:?}"),
			}
		}
		assert!(positional > 50_000, "only {positional} positional values were generated");
	}

	#[test]
	fn to_decimal_exact_is_the_inverse_of_from_decimal() {
		for text in [
			"0",
			"1",
			"-1",
			"1.50",
			"100.000",
			"0.0000001",
			"-0.00000000000000000000000001",
			"7.9228162514264337593543950335",
			"79228162514264337593543950335",
			"-79228162514264337593543950335",
		] {
			let d: Decimal = text.parse().unwrap();
			let back = LexDecimal::from_decimal(d).to_decimal_exact().unwrap();
			assert_eq!(back, d, "{text}");
			assert_eq!(back.scale(), d.scale(), "{text} lost its scale");
		}
	}

	#[test]
	fn to_decimal_exact_reads_any_spelling_of_a_representable_value() {
		let cases: [(&str, &str); 5] = [
			// A positive exponent is trailing zeros.
			("1E+3", "1000"),
			("12E+27", "12000000000000000000000000000"),
			// Trailing zeros past the 28th place are not significant.
			("1200E-30", "0.0000000000000000000000000012"),
			("5E-7", "0.0000005"),
			("-0E+10", "0"),
		];
		for (value, decimal) in cases {
			let decimal: Decimal = decimal.parse().unwrap();
			assert_eq!(lex(value).to_decimal_exact().unwrap(), decimal, "{value}");
		}
	}

	#[test]
	fn to_decimal_exact_refuses_what_a_decimal_cannot_hold() {
		for text in [
			// Past the 96-bit mantissa.
			"1E+29",
			"79228162514264337593543950336",
			"9999999999999999999999999999999999",
			"1E+6144",
			// A significant digit past the 28th fractional place.
			"1E-29",
			"123E-30",
			"1E-6143",
		] {
			assert!(lex(text).to_decimal_exact().is_err(), "{text} is not a Decimal");
		}
		for value in [
			LexDecimal::NaN,
			LexDecimal::Infinite {
				negative: false,
			},
		] {
			assert!(value.to_decimal_exact().is_err(), "{value} is not a Decimal");
		}
	}
}
