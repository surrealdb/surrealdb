//! Decimal functionality and extension traits.

use std::str::FromStr;

use anyhow::{Result, bail};
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

const P: u32 = 28; // max precision of rust_decimal
const AE_BIAS: i16 = 128;

pub(crate) fn encode_decimal_lex(d: Decimal, i: u8) -> [u8; 15] {
	// Canonicalize: remove trailing zeros; treat -0 as +0
	let mut d = d.normalize();
	let mut out = [0u8; 15];
	out[14] = i;
	if d.is_zero() {
		out[0] = 0x80;
		return out;
	}

	let is_neg = d.is_sign_negative();
	if is_neg {
		d.set_sign_positive(true);
	}

	// Obtain coefficient digits (no decimal point), ndigits, and scale
	// We use string parsing for clarity; this stays within 28 digits.
	let s = d.to_string(); // e.g., "123.45"
	let scale = d.scale() as i16;

	let mut coeff_digits = Vec::<u8>::with_capacity(32);
	for ch in s.bytes() {
		match ch {
			b'0'..=b'9' => coeff_digits.push(ch - b'0'),
			b'.' => {}
			b'-' => {}
			_ => unreachable!(),
		}
	}
	// Remove any leading zeros in coefficient (normalize() should have prevented them)
	while coeff_digits.len() > 1 && coeff_digits[0] == 0 {
		coeff_digits.remove(0);
	}
	
	// Truncate to maximum precision if needed
	if coeff_digits.len() > P as usize {
		coeff_digits.truncate(P as usize);
	}
	
	let nd = coeff_digits.len() as i16;

	// Adjusted exponent: AE = ndigits - 1 - scale
	let ae = nd - 1 - scale;
	let ae_biased = (ae + AE_BIAS) as i16;
	assert!((0..=255).contains(&ae_biased));
	let ae_byte = ae_biased as u8;

	// Build 96-bit normalized significand S = coeff * 10^(P - nd)
	let pad_zeros = (P as i16 - nd) as u32;
	let mut s_norm: u128 = 0;
	for dgt in coeff_digits {
		s_norm = s_norm * 10 + (dgt as u128);
	}
	s_norm *= pow10_u128(pad_zeros);
	// Must fit in 96 bits for rust_decimal domain
	assert!(s_norm <= ((1u128 << 96) - 1), "sig doesn't fit 96 bits");

	// Emit bytes
	out[0] = if is_neg {
		0x00
	} else {
		0xFF
	}; // region
	out[1] = ae_byte;

	// Write 12-byte big-endian significand
	let mut tmp = s_norm;
	for i in (2..14).rev() {
		out[i] = (tmp & 0xFF) as u8;
		tmp >>= 8;
	}

	// For negatives, invert AE and SIG (not the region)
	if is_neg {
		out[1] = !out[1];
		for i in 2..14 {
			out[i] ^= 0xFF;
		}
	}

	out
}

pub(crate) fn decode_decimal_lex(bytes: [u8; 15]) -> Result<Decimal> {
	match bytes[0] {
		0x80 => return Ok(Decimal::ZERO),
		0xFF | 0x00 => {}
		r => bail!("Invalid region: {r}"),
	}
	let is_neg = bytes[0] == 0x00;

	// Recover AE and S (uninvert for negatives)
	let ae_byte = if is_neg {
		!bytes[1]
	} else {
		bytes[1]
	};
	let ae = (ae_byte as i16) - AE_BIAS;

	// Read 96-bit big-endian significand
	let mut s_norm: u128 = 0;
	for &b in &bytes[2..14] {
		let v = if is_neg {
			(!b) as u8
		} else {
			b
		};
		s_norm = (s_norm << 8) | (v as u128);
	}
	if s_norm == 0 {
		// Only the zero code should carry zero SIG
		bail!("BadSig");
	}

	// Determine nd from trailing decimal zeros in S (base-10)
	// S = coeff * 10^(P - nd)  ⇒ tz = P - nd
	let mut tz = 0u32;
	let mut s_tmp = s_norm;
	while tz < P && s_tmp % 10 == 0 {
		s_tmp /= 10;
		tz += 1;
	}
	let nd = (P - tz) as i16;
	if nd <= 0 || nd > P as i16 {
		bail!("BadSig");
	}

	// scale = nd - 1 - AE
	let scale = (nd - 1 - ae) as i32;
	if !(0..=P as i32).contains(&scale) {
		bail!("BadAdjustedExponent");
	}

	// coeff = S / 10^(P - nd) = S / 10^tz
	let coeff = s_norm / pow10_u128(tz);
	debug_assert_eq!(s_tmp, coeff);

	// Build Decimal: value = sign * coeff * 10^(-scale)
	let mut mant = coeff as i128;
	if is_neg {
		mant = -mant;
	}
	let dec = Decimal::from_i128_with_scale(mant, scale as u32);
	Ok(dec.normalize())
}

// ---- helpers ----

const POW10_U128: [u128; 29] = {
	// 10^0 .. 10^28
	let mut a = [0u128; 29];
	let mut i = 0;
	while i < 29 {
		a[i] = if i == 0 {
			1
		} else {
			a[i - 1] * 10
		};
		i += 1;
	}
	a
};

#[inline]
fn pow10_u128(e: u32) -> u128 {
	POW10_U128[e as usize]
}

#[cfg(test)]
mod tests {
	use super::*;
	use rust_decimal::prelude::ToPrimitive;

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
	fn test_encode_decode_roundtrip() {
		let test_cases = vec![
			"0",
			"1",
			"-1", 
			"123.456",
			"-123.456",
			"0.001",
			"-0.001",
			"1000000",
			"-1000000",
			"123.000", // Trailing zeros
			"0.100", // Trailing zeros after decimal
		];

		for case in test_cases {
			let original = Decimal::from_str(case).unwrap();
			let encoded = encode_decimal_lex(original, 0);
			let decoded = decode_decimal_lex(encoded).unwrap();
			
			assert_eq!(original.normalize(), decoded.normalize(), 
					  "Roundtrip failed for {}: {} != {}", case, original, decoded);
		}
	}

	#[test]
	fn test_lexicographic_ordering() {
		let test_pairs = vec![
			("0", "1"),
			("-1", "0"),
			("-1", "1"),
			("123", "124"),
			("123.4", "123.5"),
			("-123.5", "-123.4"),
			("0.001", "0.002"),
		];

		for (smaller, larger) in test_pairs {
			let dec_smaller = Decimal::from_str(smaller).unwrap();
			let dec_larger = Decimal::from_str(larger).unwrap();
			
			let encoded_smaller = encode_decimal_lex(dec_smaller, 0);
			let encoded_larger = encode_decimal_lex(dec_larger, 0);
			
			assert!(encoded_smaller < encoded_larger,
				   "Lexicographic ordering failed: {} should be < {} but encoded bytes don't reflect this",
				   smaller, larger);
		}
	}
}
