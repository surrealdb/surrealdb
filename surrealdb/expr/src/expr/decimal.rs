//! Decimal functionality and extension traits.
//!
//! This module provides DecimalLexEncoder, a variable-length, lexicographic
//! byte encoding for fastnum::D128 values. The encoding ensures that
//! byte-wise ordering preserves numeric ordering, which is critical for key
//! construction in indexes. The encoder is stream-friendly: it guarantees an
//! in-band terminator within the mantissa encoding and appends a trailing 0x00
//! so decoders can read until the first zero when values are concatenated in
//! composite keys.
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

use anyhow::Result;
use fastnum::decimal::{Context, Sign};
use fastnum::{D128, U128};
use rust_decimal::Decimal;

use crate::expr::Error;

/// Variable-length lexicographic encoding for D128 values that preserves sort
/// order.
///
/// This encoder converts D128 values into byte sequences that maintain the same
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
/// 2. **Biased scale** (2 bytes, big-endian):
///    - We bias the "scale" (not the raw exponent). Scale is defined as: `scale = exponent +
///      (digit_count - 1)`, i.e., the position of the most-significant digit in a
///      scientific-notation sense.
///    - Stored as: `biased = scale + EXP_BIAS` (unsigned 16-bit)
///    - For negative numbers: stored as `0xFFFF - biased` (one's complement) to reverse order
///    - EXP_BIAS = 6144. With D128, `exponent ∈ [-6143, +6144]` and `digit_count ∈ [1, 34]`, so
///      `scale ∈ [-6143, 6177]`, which maps into `[1, 12321]` after biasing, well within `u16`.
///
/// 3. **Packed digit representation** (variable length):
///    - Digits are taken from the absolute value's base-10 representation
///    - Each pair of digits is packed into one byte (4 bits per digit)
///    - For positive numbers: stored as-is
///    - For negative numbers: all bytes are bitwise complemented to reverse ordering
///    - Termination: encoding stops when a nibble equals `0x0`. This naturally handles both odd and
///      even digit counts: • odd count: the last byte has a low nibble of 0 • even count: an extra
///      full terminator byte is appended (0x00 for positives, 0xFF for negatives)
///
/// Because a terminator is always present within (or immediately after) the
/// mantissa, any trailing type-marker byte appended by higher layers will never
/// be consumed by the mantissa decoder.
///
/// ## Properties
/// - Preserves lexicographic ordering: if `a < b` then `encode(a) < encode(b)`
/// - Variable length encoding (3+ bytes typical: 1 sign + 2 scale + packed digits)
/// - Handles full D128 range including extreme values
/// - Uses packed digit encoding for efficient storage (2 digits per byte)
pub struct DecimalLexEncoder;

impl DecimalLexEncoder {
	/// We use a 16-bit biased "scale" (not the raw exponent).
	/// With D128: exponent ∈ [-6143, +6144] and digit_count ∈ [1, 34], so
	/// scale = exponent + (digit_count - 1) ∈ [-6143, 6177]. Adding EXP_BIAS
	/// maps this into [1, 12321], comfortably within u16.
	const EXP_BIAS: i32 = 6144; // bias used for mapping signed scale into u16 space

	const FINITE_NEGATIVE_MARKER: u8 = 0x40;
	const FINITE_POSITIVE_MARKER: u8 = 0xA0;

	// 0x80 = 128, middle value for proper ordering
	const ZERO_MARKER: u8 = 0x80;

	const INFINITE_NEGATIVE_MARKER: u8 = 0x20;
	const INFINITE_POSITIVE_MAKER: u8 = 0xC0;
	const NAN_MARKER: u8 = 0xFF;

	/// Encodes a D128 value into a lexicographically ordered byte sequence.
	///
	/// The encoding preserves sort order: if `a < b` then `encode(a) <
	/// encode(b)`. This is essential for database indexing where byte-level
	/// comparison must match numeric comparison.
	pub fn encode(dec: D128) -> Vec<u8> {
		if dec.is_nan() {
			return vec![Self::NAN_MARKER, 0x00];
		}

		// Extract sign
		let is_negative = dec.is_negative();

		if dec.is_infinite() {
			if is_negative {
				return vec![Self::INFINITE_NEGATIVE_MARKER, 0x00];
			} else {
				return vec![Self::INFINITE_POSITIVE_MAKER, 0x00];
			}
		}
		// Special case: zero gets a fixed encoding that sorts between negative and
		// positive
		if dec.is_zero() {
			return vec![Self::ZERO_MARKER, 0x00];
		}

		// Work with absolute value
		let normalized = dec.abs(); // Get absolute value
		let e = -normalized.fractional_digits_count() as i32; // Exponent: negative of fractional digits
		let digit_count = normalized.digits_count();

		// Calculate the scale that positions the first digit as the most significant
		// This normalizes the number to scientific notation form
		let scale = e + (digit_count as i32 - 1); // Scale for scientific notation

		// Apply bias to map the scale range to unsigned 16-bit space
		// For D128, scale ∈ [-6143, 6177] → [1, 12321] after adding EXP_BIAS.
		let biased_exponent = (scale + Self::EXP_BIAS) as u16;

		let encode_exponent = |e: u16| {
			let q = (e / 255) as u8;
			let r = (e % 255) as u8;
			[q + 1, r + 1]
		};

		// Build the final encoded result
		// Capacity: 1 sign + 2 exponent + packed digits (2 digits per byte) + potential
		// terminator
		let mut result = Vec::with_capacity(5 + (digit_count + 1).div_ceil(2));

		// Convert the mantissa to decimal string representation for digit packing
		let radix10 = normalized.digits().to_str_radix(10);

		// Encode sign marker and biased scale based on sign
		if is_negative {
			// Sign marker: 0x00 ensures negative numbers sort before positive ones
			result.push(Self::FINITE_NEGATIVE_MARKER);
			// Complement of biased scale: reverses ordering so that more negative values
			// sort first This maintains total ordering for negatives when compared
			// bytewise.
			result.extend(encode_exponent(0xFFFF - biased_exponent));
			// Complement all packed digit bytes to reverse their ordering for negative
			// numbers
			Self::pack_digits_negative(&radix10, &mut result);
		} else {
			// Sign marker: 0xFF ensures positive numbers sort after negative ones
			result.push(Self::FINITE_POSITIVE_MARKER);
			// Biased scale: larger scales (greater magnitude) sort later for positives
			result.extend(encode_exponent(biased_exponent));
			// Store packed digit bytes directly for positive numbers
			Self::pack_digits_positive(&radix10, &mut result);
		}
		result.push(0x00);
		//
		result
	}

	/// Decodes a lexicographically encoded byte sequence back to a D128 value.
	///
	/// This reverses the encoding process, reconstructing the original D128
	/// from its byte representation while handling all the encoding
	/// transformations.
	///
	/// A buffer that is not an encoding is an error, never a panic: an unknown
	/// marker, a truncated buffer, an exponent byte or digit nibble outside the
	/// format, a coefficient wider than a D128's, or an exponent a D128 cannot
	/// hold.
	pub fn decode(bytes: &[u8]) -> Result<D128> {
		// Handle empty buffer
		if bytes.is_empty() {
			return Err(Error::Serialization("Cannot decode from empty buffer".to_string()).into());
		}

		// Special cases
		let is_negative = match bytes[0] {
			Self::ZERO_MARKER => {
				return Ok(D128::ZERO);
			}
			Self::INFINITE_NEGATIVE_MARKER => return Ok(D128::NEG_INFINITY),
			Self::INFINITE_POSITIVE_MAKER => return Ok(D128::INFINITY),
			Self::NAN_MARKER => return Ok(D128::NAN),
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

		// Extract biased exponent (2 bytes with shift to avoid 0x00)
		let biased_exponent = Self::exponent_byte(bytes[1])? * 255 + Self::exponent_byte(bytes[2])?;
		// Unbias the scale, handling negative number complement
		let biased_exponent = if is_negative {
			// For negative numbers, undo the complement applied during encoding
			0xFFFF - biased_exponent
		} else {
			biased_exponent
		};
		// Convert back to the original scale by removing the bias
		let scale = biased_exponent as i32 - Self::EXP_BIAS;

		// Unpack the digit bytes back to decimal string, handling sign-specific
		// encoding
		let (mantissa, digit_count) = if is_negative {
			Self::unpack_digits_negative(&bytes[3..])?
		} else {
			Self::unpack_digits_positive(&bytes[3..])?
		};
		if digit_count == 0 {
			return Err(Error::Serialization("Empty mantissa".to_string()).into());
		}
		// Calculate the final exponent: scale minus the position adjustment for
		// scientific notation
		let exponent = scale - (digit_count - 1);

		// `from_parts` panics on an exponent the D128 cannot hold when its context
		// traps, so it is built without traps and the overflow it signals is
		// reported as an error: a buffer read from storage is never trusted to
		// hold an in-range value.
		let decoded = D128::from_parts(
			mantissa,
			exponent,
			if is_negative {
				Sign::Minus
			} else {
				Sign::Plus
			},
			Context::default().without_traps(),
		);
		if decoded.is_op_overflow() || decoded.is_op_underflow() {
			return Err(Error::Serialization(format!("Exponent out of range: {exponent}")).into());
		}
		Ok(decoded.with_ctx(Context::default()))
	}

	/// Reads one byte of the biased scale. The encoder shifts each byte up by one
	/// so that none is `0x00`, which the stream terminator owns, so a `0x00` here
	/// is not part of any encoding.
	fn exponent_byte(byte: u8) -> Result<u16> {
		match byte.checked_sub(1) {
			Some(byte) => Ok(u16::from(byte)),
			None => Err(Error::Serialization("Invalid exponent byte: 0".to_string()).into()),
		}
	}

	/// Packs decimal digits for negative numbers with bit inversion for
	/// lexicographic ordering. Each pair of ASCII digits is packed into a
	/// single byte (4 bits each) and then inverted. Mapping: '0'..'9' → 1..10
	/// (we avoid 0 so that 0 nibbles can be used as terminators).
	/// For odd digit counts, the last byte has a zero low nibble; for even
	/// counts, an extra 0xFF terminator byte is appended after bit inversion.
	fn pack_digits_negative(radix10: &str, buf: &mut Vec<u8>) {
		let mut iter = radix10.as_bytes().chunks_exact(2);
		for pair in &mut iter {
			// pair is &[u8; 2]
			// Convert ASCII digits to numeric values: '0' (48) -> 1, '1' (49) -> 2, etc.
			// We subtract 47 instead of 48 to map '0'->1, '1'->2, ..., '9'->10
			// This ensures no digit maps to 0, which we use as termination marker
			let hi = pair[0] - 47;
			let lo = pair[1] - 47;
			let packed = (hi << 4) | lo;
			buf.push(!packed); // Invert bits for negative number lexicographic ordering
		}
		// If the length is odd, the remainder (the last lone byte) is here:
		if let Some(remainder) = iter.remainder().first() {
			let hi = remainder - 47;
			let packed = hi << 4;
			buf.push(!packed);
		} else {
			// Set the termination byte (inverted)
			buf.push(0xF0); // !0x0F
		}
	}

	/// Packs decimal digits for positive numbers into bytes for lexicographic
	/// ordering. Each pair of ASCII digits is packed into a single byte (4
	/// bits each). Mapping: '0'..'9' → 1..10. For odd digit counts, the last
	/// byte has a zero low nibble; for even counts, an extra 0x00 terminator
	/// byte is appended. This ensures decode will stop before any trailing
	/// type marker appended by higher layers.
	fn pack_digits_positive(radix10: &str, buf: &mut Vec<u8>) {
		let mut iter = radix10.as_bytes().chunks_exact(2);
		for pair in &mut iter {
			// pair is &[u8; 2]
			// Convert ASCII digits to numeric values: '0' (48) -> 1, '1' (49) -> 2, etc.
			// We subtract 47 instead of 48 to map '0'->1, '1'->2, ..., '9'->10
			// This ensures no digit maps to 0, which we use as termination marker
			let hi = pair[0] - 47;
			let lo = pair[1] - 47;
			let packed = (hi << 4) | lo;
			buf.push(packed);
		}

		// If the length is odd, the remainder (the last lone byte) is here:
		if let Some(remainder) = iter.remainder().first() {
			let hi = remainder - 47;
			let packed = hi << 4;
			buf.push(packed);
		} else {
			// Set the termination byte
			buf.push(0x0F);
		}
	}

	/// Unpacks digits from bytes for positive numbers.
	/// Reverses the packing process by extracting digit pairs from each byte.
	/// Stops when a nibble equals 0 (terminator). Accumulates the mantissa
	/// directly into U128 and returns the total number of decoded digits.
	fn unpack_digits_positive(buf: &[u8]) -> Result<(U128, i32)> {
		let mut m = U128::ZERO;
		let mut l = 0;
		for pack in buf {
			let d = Self::unpack_digit(*pack, &mut m)?;
			l += d as i32;
			if d < 2 {
				break;
			}
		}
		Ok((m, l))
	}

	/// Unpacks digits from bytes for negative numbers.
	/// First inverts each byte to undo the bit inversion, then extracts digit
	/// pairs. Stops when a nibble equals 0 (after inversion). Accumulates into
	/// U128 and returns the number of decoded digits.
	fn unpack_digits_negative(buf: &[u8]) -> Result<(U128, i32)> {
		let mut m = U128::ZERO;
		let mut l = 0i32;
		for pack in buf {
			let d = Self::unpack_digit(!*pack, &mut m)?;
			l += d as i32;
			if d < 2 {
				break;
			}
		}
		Ok((m, l))
	}

	/// Unpacks a single packed byte into one or two digits.
	/// Returns the number of digits appended (0, 1, or 2). A return of 0 or 1
	/// indicates that a terminator nibble (0x0) was encountered and the caller
	/// should stop.
	///
	/// The byte contains two 4-bit values (nibbles): high nibble and low
	/// nibble. Each nibble represents a digit value (1..=10 mapping to
	/// '0'..'9'). Values outside 1..=10 are rejected as corrupted input.
	fn unpack_digit(pack: u8, m: &mut U128) -> Result<u8> {
		let hi = pack >> 4;
		let lo = pack & 0x0F;
		if hi == 0x0 {
			return Ok(0);
		}
		if !(1..=10).contains(&hi) {
			return Err(anyhow::Error::new(Error::Serialization(format!(
				"Invalid high nibble: {hi}"
			))));
		}
		*m = Self::push_digit(*m, hi - 1)?;
		if lo == 0 {
			return Ok(1);
		}
		if !(1..=10).contains(&lo) {
			return Err(anyhow::Error::new(Error::Serialization(format!(
				"Invalid low nibble: {lo}"
			))));
		}
		*m = Self::push_digit(*m, lo - 1)?;
		Ok(2)
	}

	/// Appends a decimal digit to a mantissa, refusing one that no longer fits
	/// the D128's 128-bit coefficient.
	fn push_digit(mantissa: U128, digit: u8) -> Result<U128> {
		mantissa
			.checked_mul_digit(10)
			.and_then(|mantissa| mantissa.checked_add_digit(u64::from(digit)))
			.ok_or_else(|| Error::Serialization("Mantissa too large".to_string()).into())
	}

	/// Converts a finite fastnum::D128 to the rust_decimal::Decimal that stands
	/// for it in a decoded index key, or `None` when none does.
	///
	/// A Decimal holds a 96-bit mantissa and at most 28 fractional digits, so
	/// the value is rounded, ties away from zero, to the nearest Decimal whose
	/// mantissa is below 2^96 and whose scale is at most 28. It is computed from
	/// the mantissa and exponent, never from the D128's text, which switches to
	/// exponent notation that a Decimal's parser does not read.
	///
	/// A Decimal stands for a value only in the range `10^-6 <= |value| < 2^96`,
	/// and for zero. `None` is returned outside it, and for NaN and the
	/// infinities, and the caller carries the value as the nearest `f64` instead.
	/// Which of the two a value decodes to therefore depends on its magnitude
	/// alone.
	pub fn to_decimal_rounded(d128: D128) -> Option<Decimal> {
		// A Decimal's mantissa is 96 bits wide.
		const MANTISSA_LIMIT: u128 = 1 << 96;
		// A Decimal has at most 28 fractional digits.
		const MAX_SCALE: u32 = 28;
		// The power of ten of the leading digit of the smallest magnitude converted.
		const MIN_LEADING_EXPONENT: i32 = -6;

		if !d128.is_finite() {
			return None;
		}
		// A D128's coefficient is a u128, so this cannot fail.
		let mantissa = d128.digits().to_u128().ok()?;
		if mantissa == 0 {
			return Some(Decimal::ZERO);
		}
		let exponent = -i32::from(d128.fractional_digits_count());
		if exponent + d128.digits_count() as i32 - 1 < MIN_LEADING_EXPONENT {
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
			if d128.is_negative() {
				-kept
			} else {
				kept
			},
			scale,
		)
		.ok()
	}

	/// Converts a rust_decimal::Decimal to a fastnum::D128.
	///
	/// This conversion extracts the mantissa, scale, and sign from the Decimal
	/// and reconstructs them as a D128 value.
	pub fn to_d128(dec: Decimal) -> D128 {
		let scale = dec.scale();
		let mantissa = dec.mantissa(); // i128
		let sign = if mantissa < 0 {
			Sign::Minus
		} else {
			Sign::Plus
		};
		// Investigation of the code of the from_u128 at the time of adding the unwrap shows that
		// this function cannot panic, weirdly it seems unable to ever return an error.
		//
		// The function returns an error when the number has more significant bits then can fit
		// into the amount of bits of a u128 which seems impossible for a function which takes a
		// u128.
		let abs =
			U128::from_u128(mantissa.unsigned_abs()).expect("u128 conversion should not fail");
		D128::from_parts(abs, -(scale as i32), sign, Context::default())
	}

	/// Converts a fastnum::D128 to a rust_decimal::Decimal through the D128's
	/// text.
	///
	/// The text of a D128 whose exponent is below -6 is in scientific notation,
	/// which `Decimal::from_str_radix` does not read as a number, and a value
	/// with more than 28 fractional digits is rounded. It is therefore only
	/// meaningful for values that print positionally; a caller that must get the
	/// same value back uses [`Self::to_decimal_exact`].
	pub fn to_decimal(d128: D128) -> Result<Decimal> {
		Ok(Decimal::from_str_radix(&d128.to_string(), 10)?)
	}

	/// Converts a finite fastnum::D128 to the rust_decimal::Decimal of exactly
	/// the same value.
	///
	/// The inverse of [`Self::to_d128`]: every Decimal it was given converts
	/// back unchanged, whatever its scale. A D128 that no Decimal can hold
	/// exactly — a magnitude beyond the 96-bit mantissa, or a significant digit
	/// past the 28th fractional place — is an error rather than a rounded value.
	pub fn to_decimal_exact(d128: D128) -> Result<Decimal> {
		// Largest mantissa a Decimal holds: 2^96 - 1.
		const MAX_MANTISSA: u128 = (1 << 96) - 1;
		// Most fractional digits a Decimal holds.
		const MAX_SCALE: i32 = 28;

		let unrepresentable =
			|| anyhow::Error::new(Error::Serialization(format!("{d128} is not a Decimal")));
		if d128.is_zero() {
			return Ok(Decimal::ZERO);
		}
		let mut mantissa =
			d128.digits().to_str_radix(10).parse::<u128>().map_err(|_| unrepresentable())?;
		// The value is `mantissa * 10^exponent`.
		let mut exponent = -(d128.fractional_digits_count() as i32);
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
			if d128.is_negative() {
				-mantissa
			} else {
				mantissa
			},
			(-exponent) as u32,
		))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_cases() -> [D128; 32] {
		[
			D128::from(f64::NEG_INFINITY),
			D128::from(f64::MIN),
			D128::from_i128(i128::MIN).unwrap(),
			D128::from(i64::MIN),
			D128::from(-1001),
			D128::from(-1000),
			D128::from(-999),
			D128::from(-100),
			-D128::TEN,
			D128::from(-9),
			D128::from(-3.15),
			D128::from(-std::f64::consts::PI),
			D128::from(-1.5f64),
			-D128::ONE,
			D128::ZERO,
			D128::ONE,
			D128::from(1.5f64),
			D128::from(2),
			D128::from(std::f64::consts::PI),
			D128::from(3.15),
			D128::from(9),
			D128::TEN,
			D128::from(100),
			D128::from(999),
			D128::from(1000),
			D128::from(1001),
			D128::from(i64::MAX),
			D128::from_i128(i128::MAX).unwrap(),
			D128::from_u128(u128::MAX).unwrap(),
			D128::from(f64::MAX),
			D128::from(f64::INFINITY),
			D128::from(f64::NAN),
		]
	}

	#[test]
	fn test_encode_decode_roundtrip() {
		let cases = test_cases();
		for (i, case) in cases.into_iter().enumerate() {
			let encoded = DecimalLexEncoder::encode(case);
			let decoded = DecimalLexEncoder::decode(&encoded).expect("Decode should succeed");
			if case.is_nan() {
				assert!(decoded.is_nan(), "Roundtrip failed for {i}: {case} != {decoded}");
			} else {
				assert_eq!(case, decoded, "Roundtrip failed for {i}: {case} != {decoded}");
			}
		}
	}

	#[test]
	fn test_encode_terminate_with_zero() {
		let cases = test_cases();
		for (i, case) in cases.into_iter().enumerate() {
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
		let cases = test_cases();
		for (i, window) in cases.windows(2).enumerate() {
			let n1 = &window[0];
			let n2 = &window[1];
			assert!(n1 < n2, "#{i} - {n1:?} < {n2:?} (before serialization)");
			let b1 = DecimalLexEncoder::encode(*n1);
			let b2 = DecimalLexEncoder::encode(*n2);
			assert!(b1 < b2, "#{i} - {n1:?} < {n2:?} (after serialization) - {b1:?} < {b2:?}");
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
		// Create a buffer that starts correctly but is truncated during mantissa
		// decoding
		let result = DecimalLexEncoder::decode(&[0xA0, 0x01, 0x01, 0x00]); // Missing mantissa data
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Empty mantissa"), "{err:?}");
	}

	/// A buffer in the encoder's layout for a finite value of the given digits
	/// and scale, built without going through a D128 so that it can hold a scale
	/// the encoder would never write.
	fn buffer(negative: bool, scale: i32, radix10: &str) -> Vec<u8> {
		let biased = (scale + DecimalLexEncoder::EXP_BIAS) as u16;
		let shift = |e: u16| [(e / 255) as u8 + 1, (e % 255) as u8 + 1];
		let mut buf = Vec::new();
		if negative {
			buf.push(DecimalLexEncoder::FINITE_NEGATIVE_MARKER);
			buf.extend(shift(0xFFFF - biased));
			DecimalLexEncoder::pack_digits_negative(radix10, &mut buf);
		} else {
			buf.push(DecimalLexEncoder::FINITE_POSITIVE_MARKER);
			buf.extend(shift(biased));
			DecimalLexEncoder::pack_digits_positive(radix10, &mut buf);
		}
		buf.push(0x00);
		buf
	}

	#[test]
	fn decode_refuses_a_scale_the_d128_cannot_hold() {
		for negative in [false, true] {
			// Scales of every f64 and Decimal, and up to the largest the encoder documents,
			// then well past what any D128 holds.
			for scale in [-324, 0, 28, 308, 6177] {
				let decoded = DecimalLexEncoder::decode(&buffer(negative, scale, "1"));
				assert!(decoded.is_ok(), "scale {scale} negative={negative}: {decoded:?}");
			}
			for scale in [40_000, 50_000, 58_000] {
				let err = DecimalLexEncoder::decode(&buffer(negative, scale, "1"))
					.expect_err("a scale beyond every D128 exponent is not decodable");
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
		// The largest coefficient a D128 holds, 2^128 - 1, decodes.
		let widest = u128::MAX.to_string();
		for negative in [false, true] {
			let decoded = DecimalLexEncoder::decode(&buffer(negative, 38, &widest))
				.expect("the widest coefficient is a D128");
			assert_eq!(decoded.digits().to_str_radix(10), widest);
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
		for (d128, expected) in cases {
			let d128: D128 = d128.parse().unwrap();
			let expected: Decimal = expected.parse().unwrap();
			let got = DecimalLexEncoder::to_decimal_rounded(d128)
				.unwrap_or_else(|| panic!("{d128} has a Decimal"));
			assert_eq!(got.serialize(), expected.serialize(), "{d128}: {got} != {expected}");
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
			let d128: D128 = text.parse().unwrap();
			assert_eq!(DecimalLexEncoder::to_decimal_rounded(d128), None, "{text}");
		}
		for d128 in [D128::NAN, D128::INFINITY, D128::NEG_INFINITY] {
			assert_eq!(DecimalLexEncoder::to_decimal_rounded(d128), None, "{d128}");
		}
	}

	/// Over values that D128 writes positionally, a Decimal reads that text to
	/// the same Decimal, at the same scale, that `to_decimal_rounded` computes
	/// from the digits, or fails to read it exactly where `to_decimal_rounded`
	/// has none.
	#[test]
	fn to_decimal_rounded_agrees_with_parsing_the_positional_text() {
		use rand::{Rng, SeedableRng};

		let mut rng = rand::rngs::StdRng::seed_from_u64(0xD128);
		let mut positional = 0u32;
		for _ in 0..200_000 {
			let digits = rng.random_range(1..=39u32);
			let mantissa = match 10u128.checked_pow(digits) {
				Some(limit) => rng.random::<u128>() % limit,
				None => rng.random::<u128>(),
			};
			let exponent = rng.random_range(-60..=2i32);
			let d128 = D128::from_parts(
				U128::from_u128(mantissa).unwrap(),
				exponent,
				if rng.random() {
					Sign::Minus
				} else {
					Sign::Plus
				},
				Context::default(),
			);
			// Only text without an exponent is one a Decimal reads.
			if d128.is_zero() || d128.to_string().contains(['e', 'E']) {
				continue;
			}
			positional += 1;
			let parsed = DecimalLexEncoder::to_decimal(d128);
			let rounded = DecimalLexEncoder::to_decimal_rounded(d128);
			match (&parsed, &rounded) {
				(Ok(parsed), Some(rounded)) => {
					assert_eq!(
						parsed.serialize(),
						rounded.serialize(),
						"{d128}: {parsed} != {rounded}"
					);
				}
				(Err(_), None) => {}
				_ => panic!("{d128}: parsed as {parsed:?}, rounded to {rounded:?}"),
			}
		}
		assert!(positional > 50_000, "only {positional} positional values were generated");
	}

	#[test]
	fn to_decimal_exact_is_the_inverse_of_to_d128() {
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
			let back = DecimalLexEncoder::to_decimal_exact(DecimalLexEncoder::to_d128(d)).unwrap();
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
		for (d128, decimal) in cases {
			let d128: D128 = d128.parse().unwrap();
			let decimal: Decimal = decimal.parse().unwrap();
			assert_eq!(DecimalLexEncoder::to_decimal_exact(d128).unwrap(), decimal, "{d128}");
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
			let d128: D128 = text.parse().unwrap();
			assert!(DecimalLexEncoder::to_decimal_exact(d128).is_err(), "{text} is not a Decimal");
		}
	}
}
