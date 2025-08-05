//! Decimal functionality and extension traits.

use std::str::FromStr;

use crate::err::Error;
use anyhow::Result;
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
pub struct DecimalLexEncoder;

impl DecimalLexEncoder {
	const POW10: [u128; 29] = [
		1,
		10,
		100,
		1_000,
		10_000,
		100_000,
		1_000_000,
		10_000_000,
		100_000_000,
		1_000_000_000,
		10_000_000_000,
		100_000_000_000,
		1_000_000_000_000,
		10_000_000_000_000,
		100_000_000_000_000,
		1_000_000_000_000_000,
		10_000_000_000_000_000,
		100_000_000_000_000_000,
		1_000_000_000_000_000_000,
		10_000_000_000_000_000_000,
		100_000_000_000_000_000_000,
		1_000_000_000_000_000_000_000,
		10_000_000_000_000_000_000_000,
		100_000_000_000_000_000_000_000,
		1_000_000_000_000_000_000_000_000,
		10_000_000_000_000_000_000_000_000,
		100_000_000_000_000_000_000_000_000,
		1_000_000_000_000_000_000_000_000_000,
		10_000_000_000_000_000_000_000_000_000,
	];

	/// Counts the number of digits in a u128 value using binary search
	#[inline]
	fn count_digits(value: u128) -> i32 {
		if value == 0 {
			return 1;
		}

		// Binary search on POW10 array to find the number of digits
		// This is O(log log n) instead of O(log n) for the division approach
		let mut left = 0;
		let mut right = Self::POW10.len();

		while left < right {
			let mid = (left + right) / 2;
			if value >= Self::POW10[mid] {
				left = mid + 1;
			} else {
				right = mid;
			}
		}

		left as i32
	}

	/// Extracts decimal digits from mantissa into an array, most significant first
	fn extract_digits(mantissa: u128) -> [u8; 29] {
		let mut digits = [0u8; 29];
		let mut value = mantissa;

		if value == 0 {
			digits[0] = 0;
			return digits;
		}

		// Calculate number of digits to know where to start placing them
		let digit_count = Self::count_digits(value) as usize;

		// Extract digits directly in most significant first order
		// by using powers of 10 to extract from left to right
		for (i, digit) in digits.iter_mut().enumerate().take(digit_count) {
			let power_index = digit_count - 1 - i;
			// This should never happen with valid input, but check bounds for safety
			debug_assert!(power_index < Self::POW10.len());
			let power = Self::POW10[power_index];
			*digit = (value / power) as u8;
			value %= power;
		}

		digits
	}

	/// Packs digits into bytes using nibbles, handling both positive and negative cases
	fn pack_digits(digits: &[u8; 29], digit_count: i32, is_negative: bool) -> Vec<u8> {
		// Calculate exact capacity: ceil(digit_count/2) + terminator if even digit count
		let capacity = (digit_count as usize + 1).div_ceil(2)
			+ if (digit_count & 1) == 0 {
				1
			} else {
				0
			};
		let mut result = Vec::with_capacity(capacity);
		let mut i = 0usize;

		while i < digit_count as usize {
			let hi = digits[i] + 1; // Add 1 to avoid zero nibbles (1-10 range)
			i += 1;

			if i < digit_count as usize {
				// We have a pair of digits - pack both into one byte
				let lo = digits[i] + 1;
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
		if (digit_count & 1) == 0 {
			let terminator = 0x00;
			result.push(if is_negative {
				!terminator
			} else {
				terminator
			});
		}

		result
	}

	/// Extracts and unbiases the exponent from encoded bytes
	#[inline]
	fn extract_exponent(bytes: &[u8], is_negative: bool) -> i32 {
		let biased_exponent = if !is_negative {
			// Positive: exponent is stored directly
			bytes[1]
		} else {
			// Negative: exponent was complemented, so reverse it
			0xFF - bytes[1]
		};
		// Remove the bias to get the actual exponent
		(biased_exponent as i32) - 128
	}

	/// Unpacks nibble-encoded digits and reconstructs the mantissa, returning both mantissa and digit count
	fn unpack_mantissa(bytes: &[u8], is_negative: bool) -> Result<(u128, i32)> {
		let mut idx = 2usize; // Start after sign and exponent bytes
		let mut mantissa: u128 = 0; // Accumulator for the mantissa
		let mut digit_count: i32 = 0; // Track digits as we unpack them

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
			// Convert back from encoded form: subtract 1 to get original digit (0-9)
			let digit = (hi - 1) as u128;
			debug_assert!(digit < 10);
			// Accumulate digit into mantissa (shift left by multiplying by 10)
			mantissa =
				mantissa.checked_mul(10).and_then(|v| v.checked_add(digit)).ok_or_else(|| {
					Error::Internal("Arithmetic overflow in mantissa calculation".to_string())
				})?;
			digit_count += 1;

			// Process low nibble (lower 4 bits)
			let lo = value & 0x0F;
			if lo == 0 {
				// Terminator nibble found - end of digits
				break;
			}
			// Convert back from encoded form and accumulate
			let digit = (lo - 1) as u128;
			debug_assert!(digit < 10);
			mantissa =
				mantissa.checked_mul(10).and_then(|v| v.checked_add(digit)).ok_or_else(|| {
					Error::Internal("Arithmetic overflow in mantissa calculation".to_string())
				})?;
			digit_count += 1;
		}

		Ok((mantissa, digit_count))
	}

	/// Calculates the final coefficient and scale from mantissa, exponent, and digit count
	#[inline]
	fn calculate_coefficient_and_scale(
		mantissa: u128,
		exponent: i32,
		digit_count: i32,
	) -> Result<(i128, u32)> {
		// Reconstruct the scale from exponent and digit count
		// Reverse of: e = nd - s - 1, so s = nd - e - 1
		let scale_i32 = digit_count - exponent - 1;

		// Handle cases where the calculated scale would be negative
		// This happens when the number needs more integer digits than we have
		if scale_i32 >= 0 {
			// Normal case: scale is non-negative
			Ok((mantissa as i128, scale_i32 as u32))
		} else {
			// Scale would be negative: multiply mantissa by appropriate power of 10
			// to shift decimal point and set scale to 0
			let k = (-scale_i32) as usize; // How many powers of 10 to multiply by

			// Check bounds for POW10 array access
			if k >= Self::POW10.len() {
				return Err(Error::Internal(format!(
					"Power of 10 index {} exceeds array bounds {}",
					k,
					Self::POW10.len()
				))
				.into());
			}

			let coefficient = mantissa.checked_mul(Self::POW10[k]).ok_or_else(|| {
				Error::Internal("Arithmetic overflow in coefficient calculation".to_string())
			})?;
			Ok((coefficient as i128, 0u32)) // Scale becomes 0 after adjustment
		}
	}

	/// Encodes a Decimal value into a lexicographically ordered byte sequence.
	///
	/// The encoding preserves sort order: if `a < b` then `encode(a) < encode(b)`.
	/// This is essential for database indexing where byte-level comparison must
	/// match numeric comparison.
	pub fn encode(dec: Decimal) -> Vec<u8> {
		// Special case: zero gets a fixed encoding that sorts between negative and positive
		if dec.is_zero() {
			return vec![0x80]; // 0x80 = 128, middle value for proper ordering
		}

		// Extract sign and work with absolute normalized value
		let is_negative = dec.is_sign_negative();
		let normalized = dec.abs().normalize(); // Remove trailing zeros for canonical form
		let scale = normalized.scale() as i32; // Number of digits after decimal point
		let mantissa = normalized.mantissa() as u128; // The significant digits as an integer
		debug_assert!(mantissa > 0); // Should never be zero after the check above

		// Calculate number of digits in the mantissa
		let digit_count = Self::count_digits(mantissa);

		// Calculate the exponent: position of most significant digit relative to decimal point
		// For 123.45 (mantissa=12345, scale=2): nd=5, s=2, e=5-2-1=2
		// For 0.00123 (mantissa=123, scale=5): nd=3, s=5, e=3-5-1=-3
		let exponent = digit_count - scale - 1;

		// Bias the exponent by 128 to make it unsigned for lexicographic ordering
		// This ensures larger exponents sort after smaller ones
		let biased_exponent = (exponent + 128) as u8;

		// Extract individual decimal digits from mantissa, most significant first
		let digits = Self::extract_digits(mantissa);

		// Build the final encoded result
		let mut result = Vec::with_capacity(3 + (digit_count as usize + 1).div_ceil(2));

		// Add sign marker and exponent
		if is_negative {
			// Sign marker: 0x00 ensures negative numbers sort before positive ones
			result.push(0x00);
			// Complement of biased exponent: larger magnitude (more negative) sorts first
			result.push(0xFF - biased_exponent);
		} else {
			// Sign marker: 0xFF ensures positive numbers sort after negative ones
			result.push(0xFF);
			// Biased exponent: larger exponents sort later (correct for positive numbers)
			result.push(biased_exponent);
		}

		// Pack digits into bytes using nibbles
		let packed_digits = Self::pack_digits(&digits, digit_count, is_negative);
		result.extend(packed_digits);

		result
	}

	/// Decodes a lexicographically encoded byte sequence back to a Decimal value.
	///
	/// This reverses the encoding process, reconstructing the original Decimal
	/// from its byte representation while handling all the encoding transformations.
	pub fn decode(bytes: &[u8]) -> Result<Decimal> {
		// Input validation
		if bytes.is_empty() {
			return Err(Error::Serialization("Cannot decode from empty buffer".to_string()).into());
		}

		match bytes[0] {
			// Special case: zero has its own fixed encoding
			0x80 => Ok(Decimal::ZERO),

			// Non-zero values: 0xFF for positive, 0x00 for negative
			0xFF | 0x00 => {
				// Non-zero values require at least 2 bytes (sign + exponent)
				if bytes.len() < 2 {
					return Err(Error::Serialization(format!(
						"Buffer too short for non-zero value: expected at least 2 bytes, got {}",
						bytes.len()
					))
					.into());
				}

				// Determine sign from the first byte
				let is_negative = bytes[0] == 0x00;

				// Extract and unbias the exponent
				let exponent = Self::extract_exponent(bytes, is_negative);

				// Reconstruct the mantissa by unpacking nibble-encoded digits
				// This also returns the digit count, eliminating redundant computation
				let (mantissa, digit_count) = Self::unpack_mantissa(bytes, is_negative)?;

				// Calculate the final coefficient and scale
				let (coefficient, scale) =
					Self::calculate_coefficient_and_scale(mantissa, exponent, digit_count)?;

				// Create the final Decimal with the reconstructed coefficient and scale
				let decimal = Decimal::from_i128_with_scale(coefficient, scale);

				// Apply the sign
				Ok(if is_negative {
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
}

#[cfg(test)]
mod tests {
	use super::*;
	use num_traits::FromPrimitive;
	use rust_decimal::prelude::ToPrimitive;

	fn test_cases() -> [Decimal; 24] {
		[
			Decimal::MIN,
			Decimal::from(i64::MIN),
			Decimal::from(-1001),
			-Decimal::ONE_THOUSAND,
			Decimal::from(-999),
			-Decimal::ONE_HUNDRED,
			-Decimal::TEN,
			Decimal::from(-9),
			Decimal::from_f64(-3.15).unwrap(),
			Decimal::from_f64(-std::f64::consts::PI).unwrap(),
			Decimal::NEGATIVE_ONE,
			Decimal::ZERO,
			Decimal::ONE,
			Decimal::TWO,
			Decimal::from_f64(std::f64::consts::PI).unwrap(),
			Decimal::from_f64(3.15).unwrap(),
			Decimal::from(9),
			Decimal::TEN,
			Decimal::ONE_HUNDRED,
			Decimal::from(999),
			Decimal::ONE_THOUSAND,
			Decimal::from(1001),
			Decimal::from(i64::MAX),
			Decimal::MAX,
		]
	}

	#[test]
	fn test_encode_decode_roundtrip() {
		let cases = test_cases();
		for (i, case) in cases.into_iter().enumerate() {
			let encoded = DecimalLexEncoder::encode(case);
			let decoded = DecimalLexEncoder::decode(&encoded).expect("Decode should succeed");
			assert_eq!(
				case.normalize(),
				decoded.normalize(),
				"Roundtrip failed for {i}: {case} != {decoded}"
			);
		}
	}

	#[test]
	fn test_lexicographic_ordering() {
		let cases = test_cases();
		for window in cases.windows(2) {
			let n1 = &window[0];
			let n2 = &window[1];
			assert!(n1 < n2, "{n1:?} < {n2:?} (before serialization)");
			let b1 = DecimalLexEncoder::encode(*n1);
			let b2 = DecimalLexEncoder::encode(*n2);
			assert!(b1 < b2, "{n1:?} < {n2:?} (after serialization) - {b1:?} < {b2:?}");
		}
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
}
