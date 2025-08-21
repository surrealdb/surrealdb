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

use crate::err::Error;

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
pub(crate) struct DecimalLexEncoder;

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
	pub(crate) fn encode(dec: D128) -> Vec<u8> {
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
			Self::pack_digits_negative(radix10, &mut result);
		} else {
			// Sign marker: 0xFF ensures positive numbers sort after negative ones
			result.push(Self::FINITE_POSITIVE_MARKER);
			// Biased scale: larger scales (greater magnitude) sort later for positives
			result.extend(encode_exponent(biased_exponent));
			// Store packed digit bytes directly for positive numbers
			Self::pack_digits_positive(radix10, &mut result);
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
	pub(crate) fn decode(bytes: &[u8]) -> Result<D128> {
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
		let biased_exponent = (bytes[1] - 1) as u16 * 255 + (bytes[2] - 1) as u16;
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

		Ok(D128::from_parts(
			mantissa,
			exponent,
			if is_negative {
				Sign::Minus
			} else {
				Sign::Plus
			},
			Context::default(),
		))
	}

	/// Packs decimal digits for negative numbers with bit inversion for
	/// lexicographic ordering. Each pair of ASCII digits is packed into a
	/// single byte (4 bits each) and then inverted. Mapping: '0'..'9' → 1..10
	/// (we avoid 0 so that 0 nibbles can be used as terminators).
	/// For odd digit counts, the last byte has a zero low nibble; for even
	/// counts, an extra 0xFF terminator byte is appended after bit inversion.
	fn pack_digits_negative(radix10: String, buf: &mut Vec<u8>) {
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
	fn pack_digits_positive(radix10: String, buf: &mut Vec<u8>) {
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
		*m = *m * U128::TEN + U128::from(hi - 1);
		if lo == 0 {
			return Ok(1);
		}
		if !(1..=10).contains(&lo) {
			return Err(anyhow::Error::new(Error::Serialization(format!(
				"Invalid low nibble: {lo}"
			))));
		}
		*m = *m * U128::TEN + U128::from(lo - 1);
		Ok(2)
	}

	/// Converts a rust_decimal::Decimal to a fastnum::D128.
	///
	/// This conversion extracts the mantissa, scale, and sign from the Decimal
	/// and reconstructs them as a D128 value.
	pub(crate) fn to_d128(dec: Decimal) -> Result<D128> {
		let scale = dec.scale();
		let mantissa = dec.mantissa(); // i128
		let sign = if mantissa < 0 {
			Sign::Minus
		} else {
			Sign::Plus
		};
		let abs = U128::from_u128(mantissa.unsigned_abs()).map_err(|e| {
			Error::Serialization(format!("Failed to convert mantissa to u128: {e}"))
		})?;
		Ok(D128::from_parts(abs, -(scale as i32), sign, Context::default()))
	}

	/// Converts a fastnum::D128 to a rust_decimal::Decimal.
	///
	/// This conversion uses string representation as an intermediate format
	/// to ensure precision is maintained during the conversion.
	pub(crate) fn to_decimal(d128: D128) -> Result<Decimal> {
		Ok(Decimal::from_str_radix(&d128.to_string(), 10)?)
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
}
