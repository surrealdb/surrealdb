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

/// Fixed-width lexicographic encoding for Decimal values
/// Uses 16 bytes to handle the full 96-bit mantissa plus scale, sign,
/// and an external marker.
pub struct DecimalLexEncoder;

impl DecimalLexEncoder {
	const ZERO: u8 = 0x80;
	const POSITIVE: u8 = 0xFF; // NEGATIVE = 0x00

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

	/// Encodes a Decimal to a fixed-width byte array where lexicographic order == numeric order
	/// Returns a 16-byte array that preserves total ordering of all finite decimals
	pub(crate) fn encode(dec: Decimal, external_marker: u8) -> [u8; 16] {
		// Output
		let mut out = [0u8; 16];
		out[15] = external_marker;

		// fast-path zero so it sits between negatives and positives
		if dec.is_zero() {
			out[0] = Self::ZERO;
			return out;
		}

		let neg = dec.is_sign_negative();
		let y = dec.abs();

		// Extract canonical (c, s). Ensure no trailing zeros in c.
		let t = y.normalize(); // or `.normalized()`
		let s = t.scale() as i32;
		// coefficient (fits; decimal uses 96-bit integer coeff)
		let c = t.mantissa() as u128;

		let nd = Self::digits10(c);
		debug_assert!((1..=29).contains(&nd), "dec: {dec} nd: {nd}");
		let e = nd as i32 - s - 1;
		debug_assert!((-128..=127).contains(&e));

		// Bias exponent
		let bex = (e + 128) as u8;

		// Build fixed-D mantissa: M = c * 10^(D - nd)
		let scale_up = (29 - nd) as usize;
		debug_assert!(scale_up <= 28);
		// Multiply by 10^k safely (k <= 28). Use pow10 table to avoid overflow loops.
		let m = c.checked_mul(Self::POW10[scale_up]).expect("fits in 97 bits");

		// Write positive form: [0]=0xFF, [1]=bex, [2..14]=BE(m), [15]=0
		out[0] = Self::POSITIVE;
		out[1] = bex;
		// write 13-byte big-endian m
		let tmp = m.to_be_bytes();
		// take the lowest 13 bytes of the big-endian u128 (since m < 2^97)
		out[2..15].copy_from_slice(&tmp[3..16]); // low 13 bytes (BE)

		if neg {
			// Invert bytes 0..14 to enter the negative region and reverse order
			for b in &mut out[0..15] {
				*b = !*b;
			}
		}

		out
	}

	// Count base-10 digits of a nonzero i128 (c fits in 96 bits, so i128 is fine).
	fn digits10(mut x: u128) -> u32 {
		let mut d = 0;
		while x > 0 {
			x /= 10;
			d += 1;
		}
		d
	}

	/// Decodes a byte array back to a Decimal
	pub(crate) fn decode(bytes: [u8; 16]) -> Decimal {
		if bytes[0] == Self::ZERO {
			return Decimal::ZERO;
		}

		let (neg, buf) = if bytes[0] == Self::POSITIVE {
			(false, bytes)
		} else {
			// negative: invert 0..14 back to positive form
			let mut b = bytes;
			for i in 0..15 {
				b[i] = !b[i];
			}
			(true, b)
		};

		let e = (buf[1] as i32) - 128;

		// read 13-byte mantissa into u128
		let mut be = [0u8; 16];
		be[3..16].copy_from_slice(&buf[2..15]); // align low 13 bytes
		let m = u128::from_be_bytes(be);

		// Strip ALL trailing decimal zeros from M to get canonical c (no factor 10)
		let mut v = m;
		while v != 0 && v % 10 == 0 {
			v /= 10;
		}
		let c = v; // canonical coefficient (no trailing zeros)
		let nd = Self::digits10(c) as i32;

		// Compute "virtual" scale; may be negative for integers with trailing zeros.
		let s_i32 = nd - e - 1;

		// Build a Decimal with a non-negative scale by shifting 10^{-s} into the coeff when needed.
		let (coeff_i128, scale_u32) = if s_i32 >= 0 {
			(c as i128, s_i32 as u32)
		} else {
			let k = (-s_i32) as usize; // how many 10s to move into the coefficient
			let coeff = c
				.checked_mul(Self::POW10[k])
				.expect("coefficient overflow when adjusting negative scale");
			(coeff as i128, 0u32)
		};

		let dec = Decimal::from_i128_with_scale(coeff_i128, scale_u32);
		if neg {
			-dec
		} else {
			dec
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use num_traits::FromPrimitive;
	use rust_decimal::prelude::ToPrimitive;

	fn test_cases() -> [Decimal; 15] {
		[
			Decimal::MIN,
			Decimal::from(i64::MIN),
			Decimal::from(-10),
			Decimal::from_f64(-std::f64::consts::PI).unwrap(),
			Decimal::from_f64(-3.14).unwrap(),
			Decimal::NEGATIVE_ONE,
			Decimal::ZERO,
			Decimal::ONE,
			Decimal::TWO,
			Decimal::from_f64(3.14).unwrap(),
			Decimal::from_f64(std::f64::consts::PI).unwrap(),
			Decimal::ONE_HUNDRED,
			Decimal::ONE_THOUSAND,
			Decimal::from(i64::MAX),
			Decimal::MAX,
		]
	}

	#[test]
	fn test_encode_decode_roundtrip() {
		let cases = test_cases();
		for (i, case) in cases.into_iter().enumerate() {
			let encoded = DecimalLexEncoder::encode(case, 0);
			let decoded = DecimalLexEncoder::decode(encoded);
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
			let b1 = DecimalLexEncoder::encode(*n1, 0);
			let b2 = DecimalLexEncoder::encode(*n2, 0);
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
