//! Decimal functionality and extension traits.

use std::str::FromStr;

use crate::err::Error;
use anyhow::Result;
use fastnum::decimal::{Context, Sign};
use fastnum::{D128, U128};
use rust_decimal::Decimal;

/// A trait to extend the Decimal type with additional functionality.
pub trait DecimalExt {
	/// Converts a string to a Decimal, normalizing it in the process.
	///
	/// This method is a convenience wrapper around `rust_decimal::Decimal::from_str`
	/// which can parse a string into a Decimal and normalize it. If the value has
	/// higher precision than the Decimal type can handle, it will be rounded to the
	/// nearest representable value.
	fn from_str_normalized(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;

	/// Converts a string to a Decimal, normalizing it in the process.
	///
	/// This method is a convenience wrapper around `rust_decimal::Decimal::from_str_exact`
	/// which can parse a string into a Decimal and normalize it. If the value has
	/// higher precision than the Decimal type can handle an Underflow error will be returned.
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

/// Variable-length lexicographic encoding for D128 values that preserves sort order.
///
/// This encoder converts D128 values into byte sequences that maintain the same
/// lexicographic ordering as the original decimal values. This is crucial for database
/// indexing where byte-level comparison must match numeric comparison.
///
/// ## Encoding Format
///
/// The encoding uses a variable-length format optimized for lexicographic ordering:
///
/// ### Zero Values
/// - Zero is encoded as a single byte: `0x80`
/// - This ensures zero sorts between negative and positive numbers
///
/// ### Non-Zero Values
/// The format consists of:
/// 1. **Sign marker** (1 byte):
///    - `0xFF` for positive numbers
///    - `0x00` for negative numbers
///
/// 2. **Biased exponent** (2 bytes, big-endian):
///    - Calculated as: `scale + EXP_BIAS` where scale = e + (digit_count - 1)
///    - For positive: stored as-is
///    - For negative: stored as `0xFFFF - biased_exponent` (complement for reverse ordering)
///    - EXP_BIAS = 6144 to handle IEEE-754 decimal-128 exponent range [-6143, +6144]
///
/// 3. **Radix-100 digit representation** (variable length):
///    - Digits are converted to radix-100 string representation
///    - For positive numbers: stored as raw bytes
///    - For negative numbers: all bytes are bitwise complemented
///    - Terminated by a `0x00` byte
///
/// ## Properties
/// - Preserves lexicographic ordering: if `a < b` then `encode(a) < encode(b)`
/// - Variable length encoding (4+ bytes typical: 1 sign + 2 exponent + digits + 1 terminator)
/// - Handles full D128 range including extreme values
/// - Uses radix-100 encoding for efficient digit storage
///
pub(crate) struct DecimalLexEncoder;

impl DecimalLexEncoder {
	/// IEEE-754 decimal-128 allows E ∈ [-6143, +6144]
	/// We map that closed range into an unsigned 16-bit space by adding BIAS:
	const EXP_BIAS: i32 = 6144; // Emin → 0,  Emax → 12 287 (< 2¹⁴)

	/// Encodes a D128 value into a lexicographically ordered byte sequence.
	///
	/// The encoding preserves sort order: if `a < b` then `encode(a) < encode(b)`.
	/// This is essential for database indexing where byte-level comparison must
	/// match numeric comparison.
	pub(crate) fn encode(dec: D128) -> Vec<u8> {
		// Special case: zero gets a fixed encoding that sorts between negative and positive
		if dec.is_zero() {
			return vec![0x80]; // 0x80 = 128, middle value for proper ordering
		}

		// Extract sign and work with absolute value
		let is_negative = dec.is_negative();
		let normalized = dec.abs(); // Get absolute value
		let e = -normalized.fractional_digits_count() as i32; // Exponent: negative of fractional digits
		let digit_count = normalized.digits_count();

		// Calculate the scale that positions the first digit as the most significant
		// This normalizes the number to scientific notation form
		let scale = e + (digit_count as i32 - 1); // Scale for scientific notation

		// Apply bias to map the exponent range to unsigned 16-bit space
		// IEEE-754 decimal-128 exponents [-6143, +6144] → [1, 12287]
		let biased_exponent = (scale + Self::EXP_BIAS) as u16;

		// Build the final encoded result
		let mut result = Vec::with_capacity(5 + (digit_count + 1).div_ceil(2));

		let radix10 = normalized.digits().to_str_radix(10);
		println!(
			"Mantissa: {radix10} - Digit count: {digit_count} - Scale: {scale} - Biased Exponent: {biased_exponent} - Exponent: {e} - Is negative: {is_negative}"
		);

		// Encode sign marker and exponent based on sign
		if is_negative {
			// Sign marker: 0x00 ensures negative numbers sort before positive ones
			result.push(0x00);
			// Complement of biased exponent: larger magnitude (more negative) sorts first
			// This reverses the ordering for negative numbers to maintain lexicographic order
			result.extend((0xFFFF - biased_exponent).to_be_bytes());
			// Complement all radix-100 bytes to reverse their ordering for negative numbers
			Self::pack_digits_negative(radix10, &mut result);
		} else {
			// Sign marker: 0xFF ensures positive numbers sort after negative ones
			result.push(0xFF);
			// Biased exponent: larger exponents sort later (correct for positive numbers)
			result.extend(biased_exponent.to_be_bytes());
			// Store radix-100 bytes directly for positive numbers
			Self::pack_digits_positive(radix10, &mut result);
		}
		//
		result
	}

	/// Decodes a lexicographically encoded byte sequence back to a D128 value.
	///
	/// This reverses the encoding process, reconstructing the original D128
	/// from its byte representation while handling all the encoding transformations.
	pub(crate) fn decode(bytes: &[u8]) -> Result<D128> {
		// Handle empty buffer
		if bytes.is_empty() {
			return Err(Error::Serialization("Cannot decode from empty buffer".to_string()).into());
		}

		// Special case: zero
		if bytes.len() == 1 && bytes[0] == 0x80 {
			return Ok(D128::ZERO);
		}

		// Need at least 4 bytes: sign (1) + exponent (2) + terminator (1)
		if bytes.len() < 4 {
			return Err(Error::Serialization("Buffer too short".to_string()).into());
		}

		let sign_byte = bytes[0];
		let is_negative = match sign_byte {
			0x00 => true,  // Negative
			0xFF => false, // Positive
			_ => return Err(Error::Serialization("Invalid sentinel byte".to_string()).into()),
		};

		// Extract biased exponent (2 bytes, big-endian)
		let exp_bytes = [bytes[1], bytes[2]];
		let biased_exponent = u16::from_be_bytes(exp_bytes);
		// Unbias the exponent, handling negative number complement
		let biased_exponent = if is_negative {
			0xFFFF - biased_exponent
		} else {
			biased_exponent
		};
		let scale = biased_exponent as i32 - Self::EXP_BIAS;

		let mantissa = if is_negative {
			Self::unpack_digits_negative(&bytes[3..])
		} else {
			Self::unpack_digits_positive(&bytes[3..])
		};
		if mantissa.is_empty() {
			return Err(Error::Serialization("Empty mantissa".to_string()).into());
		}
		let exponent = scale - (mantissa.len() as i32 - 1);
		let mantissa = U128::from_str(&mantissa).map_err(|e| {
			Error::Serialization(format!("Failed to parse mantissa '{mantissa}'. Error: {e}"))
		})?;

		println!(
			"Mantissa: {mantissa} - Digit count: {} - Scale: {scale} - Biased Exponent: {biased_exponent} - Exponent: {exponent} - Is negative: {is_negative}",
			mantissa.digits().len()
		);

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

	fn pack_digits_negative(radix10: String, buf: &mut Vec<u8>) {
		let mut iter = radix10.as_bytes().chunks_exact(2);
		for pair in &mut iter {
			// pair is &[u8; 2]
			let hi = pair[0] - 47;
			let lo = pair[1] - 47;
			let packed = (hi << 4) | lo;
			buf.push(!packed);
		}
		// If the length is odd, the remainder (the last lone byte) is here:
		if let Some(remainder) = iter.remainder().first() {
			let hi = remainder - 47;
			let packed = (hi << 4) | 0x00;
			buf.push(!packed);
		} else {
			buf.push(0xFF);
		}
	}

	fn pack_digits_positive(radix10: String, buf: &mut Vec<u8>) {
		let mut iter = radix10.as_bytes().chunks_exact(2);
		for pair in &mut iter {
			// pair is &[u8; 2]
			let hi = pair[0] - 47;
			let lo = pair[1] - 47;
			let packed = (hi << 4) | lo;
			buf.push(packed);
		}

		// If the length is odd, the remainder (the last lone byte) is here:
		if let Some(remainder) = iter.remainder().first() {
			let hi = remainder - 47;
			let packed = (hi << 4) | 0x00;
			buf.push(packed);
		} else {
			buf.push(0x00);
		}
	}

	fn unpack_digits_positive(buf: &[u8]) -> String {
		let mut r = String::new();
		for pack in buf {
			Self::unpack_digit(*pack, &mut r);
		}
		r
	}

	fn unpack_digits_negative(buf: &[u8]) -> String {
		let mut r = String::new();
		for pack in buf {
			Self::unpack_digit(!*pack, &mut r);
		}
		r
	}

	fn unpack_digit(pack: u8, s: &mut String) {
		let hi = pack >> 4;
		let lo = pack & 0x0F;
		if hi > 0 {
			s.push((hi + 47) as char);
		}
		if lo > 0 {
			s.push((lo + 47) as char);
		}
	}

	pub(crate) fn to_d128(dec: Decimal) -> Result<D128> {
		// First convert to raw parts using available methods
		let scale = dec.scale();
		let is_negative = dec.is_sign_negative();

		// Get the unscaled value as i128
		let unscaled = dec.mantissa();

		// Create D128 from the unscaled value
		let mut d128 = D128::from_i128(unscaled)?;

		// Apply scale
		if scale > 0 {
			d128 = d128 / D128::from(10_i32.pow(scale));
		}

		// Apply sign
		if is_negative {
			Ok(-d128)
		} else {
			Ok(d128)
		}
	}

	pub(crate) fn to_decimal(d128: D128) -> Result<Decimal> {
		Ok(Decimal::from_str_exact(&d128.to_string())?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use rust_decimal::prelude::ToPrimitive;

	fn test_cases() -> [D128; 27] {
		[
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
			-D128::ONE,
			D128::ZERO,
			D128::ONE,
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
		]
	}

	#[test]
	fn test_encode_decode_roundtrip() {
		let cases = test_cases();
		for (i, case) in cases.into_iter().enumerate() {
			let encoded = DecimalLexEncoder::encode(case);
			let decoded = DecimalLexEncoder::decode(&encoded).expect("Decode should succeed");
			assert_eq!(case, decoded, "Roundtrip failed for {i}: {case} != {decoded}");
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
		let result = DecimalLexEncoder::decode(&[0xFF]);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Buffer too short"), "{err:?}");
	}

	#[test]
	fn test_decode_invalid_sentinel() {
		let result = DecimalLexEncoder::decode(&[0x42, 0x00, 0x00, 0x00]);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Invalid sentinel byte"), "{err:?}");
	}

	#[test]
	fn test_decode_empty_mantissa() {
		// Create a buffer that starts correctly but is truncated during mantissa decoding
		let result = DecimalLexEncoder::decode(&[0xFF, 0x00, 0x00, 0x00]); // Missing mantissa data
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Empty mantissa"), "{err:?}");
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
}
