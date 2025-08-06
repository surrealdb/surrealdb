//! Decimal functionality and extension traits.

use std::str::FromStr;

use crate::err::Error;
use anyhow::Result;
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

/// Variable-length lexicographic encoding for Decimal values that preserves sort order.
///
/// This encoder converts Decimal values into byte sequences that maintain the same
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
/// 2. **Biased exponent** (1 byte):
///    - For positive: `(exponent + 128)`
///    - For negative: `255 - (exponent + 128)` (complement for reverse ordering)
///
/// 3. **Packed digits** (variable length):
///    - Each byte contains two decimal digits as nibbles
///    - Each digit is stored as `(digit + 1)` to avoid zero values
///    - Terminated by a zero nibble
///    - For negative numbers, all bytes are bitwise complemented
///
/// ## Properties
/// - Preserves lexicographic ordering: if `a < b` then `encode(a) < encode(b)`
/// - Variable length encoding (1-16 bytes typical)
/// - Handles full Decimal range including extreme values
/// - Zero-copy decoding possible
///
pub(crate) struct DecimalLexEncoder;

impl DecimalLexEncoder {
	const EXP_BIAS: i32 = 32768;

	/// Packs digits into bytes using nibbles, handling both positive and negative cases
	fn pack_digits(mantissa: u128, is_negative: bool) -> Vec<u8> {
		// Extract individual decimal digits from mantissa, most significant first
		let mantissa = mantissa.to_string();
		let digits = mantissa.as_bytes();
		// if is_negative {
		// 	for i in 0..digits.len() {
		// 		digits[i] = !digits[i];
		// 	}
		// }
		// digits.push(0u8);
		// digits
		// Calculate exact capacity: ceil(digit_count/2) + terminator if even digit count
		let capacity = (digits.len() + 1).div_ceil(2)
			+ if (digits.len() & 1) == 0 {
				1
			} else {
				0
			};
		let mut result = Vec::with_capacity(capacity);
		let mut i = 0usize;

		while i < digits.len() {
			let hi = digits[i] - 47; // Convert ascii '0'-'9' to 1-10 and avoid zero nibbles
			debug_assert!((1..11).contains(&hi), "Digit {} is out of range", hi);
			i += 1;

			if i < digits.len() {
				// We have a pair of digits - pack both into one byte
				let lo = digits[i] - 47;
				debug_assert!((1..11).contains(&lo), "Digit {} is out of range", hi);
				i += 1;
				let byte = (hi << 4) | lo; // High nibble | low nibble
				result.push(if is_negative {
					!byte
				} else {
					byte
				});
			} else {
				// Odd number of digits - last digit goes in high nibble,
				// low nibble is 0 (terminator)
				let byte = hi << 4;
				result.push(if is_negative {
					!byte
				} else {
					byte
				});
			}
		}

		// If we had an even number of digits, we need a separate terminator byte
		if (digits.len() & 1) == 0 {
			let terminator = 0x00;
			result.push(if is_negative {
				!terminator
			} else {
				terminator
			});
		}

		result
	}

	/// Unpacks nibble-encoded digits and reconstructs the mantissa, returning both mantissa and digit count
	fn unpack_mantissa(bytes: &[u8], is_negative: bool) -> Result<(U128, i32)> {
		let mut idx = 3usize; // Start after sign and exponent bytes
		let mut mantissa = String::new(); // Accumulator for the mantissa

		loop {
			let byte = *bytes.get(idx).ok_or_else(|| {
				Error::Serialization(format!("Truncated buffer at index {}", idx))
			})?;
			idx += 1;

			// For negative numbers, complement the byte to reverse the encoding transformation
			let value = if !is_negative {
				byte // Positive: use byte as-is
			} else {
				!byte // Negative: complement to undo the encoding complement
			};

			// Process high nibble (upper 4 bits)
			let hi = value >> 4;
			if hi == 0 {
				// Terminator nibble found - end of digits
				break;
			}
			debug_assert!((1..11).contains(&hi), "Digit {hi} is out of range");
			// Convert back from encoded form: add 47 to get original ascii (0-9)
			mantissa.push((hi + 47) as char);

			// Process low nibble (lower 4 bits)
			let lo = value & 0x0F;
			if lo == 0 {
				// Terminator nibble found - end of digits
				break;
			}
			debug_assert!((1..11).contains(&lo), "Digit {lo} is out of range");
			// Convert back from encoded form: add 47 to get original ascii (0-9)
			mantissa.push((lo + 47) as char);
		}
		// Restore the mantisse
		let digit_count = mantissa.len() as i32;
		let mantisse = U128::from_str_radix(&mantissa, 10)
			.map_err(|e| Error::Serialization(format!("Invalid decimal digits: {e}",)))?;
		Ok((mantisse, digit_count))
	}

	/// Calculates the final coefficient and scale from mantissa, exponent, and digit count
	#[inline]
	fn calculate_coefficient_and_scale(
		mantissa: U128,
		exponent: i32,
		digit_count: i32,
	) -> Result<(U128, u32)> {
		// Reconstruct the scale from exponent and digit count
		// Reverse of: e = nd - s - 1, so s = nd - e - 1
		let scale_i32 = digit_count - exponent - 1;

		// Handle cases where the calculated scale would be negative
		// This happens when the number needs more integer digits than we have
		if scale_i32 >= 0 {
			// Normal case: scale is non-negative
			Ok((mantissa, scale_i32 as u32))
		} else {
			// Scale would be negative: multiply mantissa by appropriate power of 10
			// to shift decimal point and set scale to 0
			let k = (-scale_i32) as usize; // How many powers of 10 to multiply by
			let coefficient = mantissa * 10.pow(k);
			Ok((coefficient, 0u32))
		}
	}

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
		let is_negative = dec.is_sign_negative();
		let normalized = dec.abs(); // Get absolute value
		let scale = normalized.fractional_digits_count() as i32; // Number of digits after decimal point
		let mantissa = normalized.digits().to_u128().unwrap_or(0); // The significant digits as an integer
		let digit_count = normalized.digits_count();
		println!(
			"{dec}: - is_negative: {is_negative} - scale: {scale} digit_count: {digit_count} - mantissa: {mantissa}"
		);

		// Calculate the exponent: position of most significant digit relative to decimal point
		// For 123.45 (mantissa=12345, scale=2): nd=5, s=2, e=5-2-1=2
		// For 0.00123 (mantissa=123, scale=5): nd=3, s=5, e=3-5-1=-3
		let exponent = digit_count as i32 - scale - 1;

		// Bias the exponent by 32768 to make it unsigned for lexicographic ordering
		// This ensures larger exponents sort after smaller ones
		let biased_exponent = exponent + Self::EXP_BIAS;
		debug_assert!(
			(0..=0xFFFF).contains(&biased_exponent),
			"Exponent out of range: {biased_exponent} - {exponent}"
		);

		// Build the final encoded result
		let mut result = Vec::with_capacity(4 + (digit_count + 1).div_ceil(2));

		// Add sign marker and exponent
		if is_negative {
			// Sign marker: 0x00 ensures negative numbers sort before positive ones
			result.push(0x00);
			// Complement of biased exponent: larger magnitude (more negative) sorts first
			let biased_exponent = 0xFFFF - biased_exponent;
			result.extend(biased_exponent.to_be_bytes());
		} else {
			// Sign marker: 0xFF ensures positive numbers sort after negative ones
			result.push(0xFF);
			// Biased exponent: larger exponents sort later (correct for positive numbers)
			result.extend(biased_exponent.to_be_bytes());
		}
		// Pack digits into bytes using nibbles
		let packed_digits = Self::pack_digits(mantissa, is_negative);
		result.extend(packed_digits);

		result
	}

	/// Decodes a lexicographically encoded byte sequence back to a D128 value.
	///
	/// This reverses the encoding process, reconstructing the original D128
	/// from its byte representation while handling all the encoding transformations.
	pub(crate) fn decode(bytes: &[u8]) -> Result<D128> {
		// Input validation
		if bytes.is_empty() {
			return Err(Error::Serialization("Cannot decode from empty buffer".to_string()).into());
		}

		match bytes[0] {
			// Special case: zero has its own fixed encoding
			0x80 => Ok(D128::ZERO),

			// Non-zero values: 0xFF for positive, 0x00 for negative
			0xFF | 0x00 => {
				// Non-zero values require at least 2 bytes (sign + exponent)
				if bytes.len() < 3 {
					return Err(Error::Serialization(format!(
						"Buffer too short for non-zero value: expected at least 2 bytes, got {}",
						bytes.len()
					))
					.into());
				}

				// Determine sign from the first byte
				let is_negative = bytes[0] == 0x00;

				// Extract and unbias the exponent
				// read and un-bias exponent ------------------------------------------
				let exp_raw = u16::from_be_bytes([bytes[1], bytes[2]]);
				let biased_exponent = if is_negative {
					!exp_raw
				} else {
					exp_raw
				};
				let exponent = (biased_exponent as i32) - Self::EXP_BIAS;

				// Reconstruct the mantissa by unpacking nibble-encoded digits
				// This also returns the digit count, eliminating redundant computation
				let (mantissa, digit_count) = Self::unpack_mantissa(bytes, is_negative)?;

				// Calculate the final coefficient and scale
				let (coefficient, scale) =
					Self::calculate_coefficient_and_scale(mantissa, exponent, digit_count)?;

				// Create the final D128 with the reconstructed coefficient and scale
				// Convert coefficient to D128 and apply scale
				let mut decimal = D128::from_i128(coefficient)?;
				// Apply scale by dividing by 10^scale
				if scale > 0 {
					let scale_factor = D128::from_i128(10_i128.pow(scale))?;
					decimal = decimal / scale_factor;
				}

				// Apply the sign
				Ok(if is_negative || coefficient < 0 {
					-decimal
				} else {
					decimal
				})
			}
			_ => {
				Err(Error::Serialization(format!("Invalid sentinel byte: {:#x}", bytes[0])).into())
			}
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
	fn test_decode_truncated_buffer() {
		let result = DecimalLexEncoder::decode(&[0xFF]);
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Buffer too short"));
	}

	#[test]
	fn test_decode_invalid_sentinel() {
		let result = DecimalLexEncoder::decode(&[0x42, 0x00]);
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Invalid sentinel byte"));
	}

	#[test]
	fn test_decode_truncated_mantissa() {
		// Create a buffer that starts correctly but is truncated during mantissa decoding
		let result = DecimalLexEncoder::decode(&[0xFF, 0x80]); // Missing mantissa data
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Truncated buffer"));
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
