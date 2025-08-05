//! Decimal functionality and extension traits.

use std::str::FromStr;

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
		let neg = dec.is_sign_negative();
		let t = dec.abs().normalize(); // Remove trailing zeros for canonical form
		let s = t.scale() as i32; // Number of digits after decimal point
		let c = t.mantissa() as u128; // The significant digits as an integer
		debug_assert!(c > 0); // Should never be zero after the check above

		// Calculate number of digits in the mantissa
		let nd = {
			let mut tmp = c;
			let mut d = 0;
			while tmp > 0 {
				tmp /= 10;
				d += 1;
			}
			d
		};

		// Calculate the exponent: position of most significant digit relative to decimal point
		// For 123.45 (mantissa=12345, scale=2): nd=5, s=2, e=5-2-1=2
		// For 0.00123 (mantissa=123, scale=5): nd=3, s=5, e=3-5-1=-3
		let e = nd - s - 1;

		// Bias the exponent by 128 to make it unsigned for lexicographic ordering
		// This ensures larger exponents sort after smaller ones
		let bex = (e + 128) as u8;

		// Extract individual decimal digits from mantissa, most significant first
		// We need digits in order for the nibble packing that follows
		let mut digs = [0u8; 29]; // Max 29 digits for Decimal
		{
			let mut v = c;
			let mut k = 0usize;
			let mut rev = [0u8; 29]; // Temporary buffer for digits in reverse order

			// Extract digits least significant first (easier with modulo)
			while v > 0 {
				rev[k] = (v % 10) as u8;
				v /= 10;
				k += 1;
			}

			// Reverse the digits to get most significant first
			for i in 0..k {
				digs[i] = rev[k - 1 - i];
			}
		}

		// Pack digits into bytes using nibbles (4-bit values)
		// Each digit is stored as (digit + 1) to avoid zero values in the encoding
		// Terminator nibble = 0 marks the end of the digit sequence
		let mut out = Vec::with_capacity(3 + (nd as usize + 1).div_ceil(2));

		if !neg {
			// === POSITIVE NUMBER ENCODING ===
			// Sign marker: 0xFF ensures positive numbers sort after negative ones
			out.push(0xFF);

			// Biased exponent: larger exponents sort later (correct for positive numbers)
			out.push(bex);

			// Pack digits two per byte as nibbles
			let mut i = 0usize;
			while i < nd as usize {
				let hi = digs[i] + 1; // Add 1 to avoid zero nibbles (1-10 range)
				i += 1;

				if i < nd as usize {
					// We have a pair of digits - pack both into one byte
					let lo = digs[i] + 1;
					i += 1;
					out.push((hi << 4) | lo); // High nibble | low nibble
				} else {
					// Odd number of digits - last digit goes in high nibble,
					// low nibble is 0 (terminator)
					out.push(hi << 4);
				}
			}

			// If we had an even number of digits, we need a separate terminator byte
			// since there was no free nibble for the terminator
			if (nd & 1) == 0 {
				out.push(0x00); // Both nibbles are 0 (terminator)
			}
		} else {
			// === NEGATIVE NUMBER ENCODING ===
			// Sign marker: 0x00 ensures negative numbers sort before positive ones
			out.push(0x00);

			// Complement of biased exponent: larger magnitude (more negative) sorts first
			// This reverses the ordering for negative numbers
			out.push(0xFF - bex);

			// Pack digits with bitwise complement to reverse lexicographic order
			// This ensures more negative numbers sort before less negative ones
			let mut i = 0usize;
			while i < nd as usize {
				let hi = digs[i] + 1;
				i += 1;

				if i < nd as usize {
					let lo = digs[i] + 1;
					i += 1;
					// Complement the entire byte to reverse ordering
					out.push(!((hi << 4) | lo));
				} else {
					// Last digit with terminator nibble, then complement
					// After complement, terminator 0 becomes 0xF
					out.push(!(hi << 4));
				}
			}

			if (nd & 1) == 0 {
				// Complement of terminator byte: !0x00 = 0xFF
				out.push(!0x00);
			}
		}
		out
	}

	/// Decodes a lexicographically encoded byte sequence back to a Decimal value.
	///
	/// This reverses the encoding process, reconstructing the original Decimal
	/// from its byte representation while handling all the encoding transformations.
	pub fn decode(bytes: &[u8]) -> Decimal {
		match bytes[0] {
			// Special case: zero has its own fixed encoding
			0x80 => Decimal::ZERO,

			// Non-zero values: 0xFF for positive, 0x00 for negative
			0xFF | 0x00 => {
				// Determine sign from the first byte
				let neg = bytes[0] == 0x00;

				// Extract and unbias the exponent
				let bex = if !neg {
					// Positive: exponent is stored directly
					bytes[1]
				} else {
					// Negative: exponent was complemented, so reverse it
					0xFF - bytes[1]
				};
				// Remove the bias to get the actual exponent
				let e = (bex as i32) - 128;

				// Reconstruct the mantissa by reading nibble-packed digits
				// Continue until we encounter a terminator nibble (0 for positive, 0xF for negative after complement)
				let mut idx = 2usize; // Start after sign and exponent bytes
				let mut c: u128 = 0; // Accumulator for the mantissa

				loop {
					let b = *bytes.get(idx).expect("truncated");
					idx += 1;

					// For negative numbers, complement the byte to reverse the encoding transformation
					let v = if !neg {
						b // Positive: use byte as-is
					} else {
						!b // Negative: complement to undo the encoding complement
					};

					// Process high nibble (upper 4 bits)
					let hi = v >> 4;
					if hi == 0 {
						// Terminator nibble found - end of digits
						break;
					}
					// Convert back from encoded form: subtract 1 to get original digit (0-9)
					let d = (hi - 1) as u128;
					debug_assert!(d < 10);
					// Accumulate digit into mantissa (shift left by multiplying by 10)
					c = c * 10 + d;

					// Process low nibble (lower 4 bits)
					let lo = v & 0x0F;
					if lo == 0 {
						// Terminator nibble found - end of digits
						break;
					}
					// Convert back from encoded form and accumulate
					let d = (lo - 1) as u128;
					debug_assert!(d < 10);
					c = c * 10 + d;
				}

				// Calculate the number of digits in the reconstructed mantissa
				// This is needed to determine the scale (decimal places)
				let mut tmp = c;
				let mut nd = 0i32;
				while tmp > 0 {
					tmp /= 10;
					nd += 1;
				}

				// Reconstruct the scale from exponent and digit count
				// Reverse of: e = nd - s - 1, so s = nd - e - 1
				let s_i32 = nd - e - 1;

				// Handle cases where the calculated scale would be negative
				// This happens when the number needs more integer digits than we have
				let (coeff, scale) = if s_i32 >= 0 {
					// Normal case: scale is non-negative
					(c as i128, s_i32 as u32)
				} else {
					// Scale would be negative: multiply mantissa by appropriate power of 10
					// to shift decimal point and set scale to 0
					let k = (-s_i32) as usize; // How many powers of 10 to multiply by
					let coeff = c.checked_mul(Self::POW10[k]).expect("overflow");
					(coeff as i128, 0u32) // Scale becomes 0 after adjustment
				};

				// Create the final Decimal with the reconstructed coefficient and scale
				let dec = Decimal::from_i128_with_scale(coeff, scale);

				// Apply the sign
				if neg {
					-dec
				} else {
					dec
				}
			}
			_ => unreachable!("bad sentinel"),
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
			let decoded = DecimalLexEncoder::decode(&encoded);
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
}
