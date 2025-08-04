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

pub(crate) fn encode_decimal_lex(d: Decimal, marker: u8) -> [u8; 15] {
	// Canonicalize: remove trailing zeros; treat -0 as +0
	let decimal = d.normalize();

	let (negative, scale, coeff) = {
		let s = decimal.serialize();
		let negative = s[12] & 0x80 != 0;
		let scale = s[12] & 0x7F;
		let mut coeff = [0u8; 12];
		coeff.copy_from_slice(&s[0..12]);
		(negative, scale, coeff)
	};

	// Compose: [sign+scale][coefficient: 12 bytes][pad][marker]
	let mut out = [0u8; 15];
	out[14] = marker;

	// First byte: sign and scale
	// 0x00-0x7F: negative, decreasing scale
	// 0x80: zero
	// 0x81-0xFF: positive, increasing scale
	let prefix = if decimal.is_zero() {
		0x80
	} else if negative {
		0x7F - scale
	} else {
		0x81 + scale
	};
	out[0] = prefix;

	// Negatives: invert the coefficient for total order
	if negative && !decimal.is_zero() {
		for i in 0..12 {
			out[i + 1] = !coeff[i];
		}
	} else {
		out[1..13].copy_from_slice(&coeff);
	}

	// Last byte: sign bit for positives/negatives or zero marker
	// This isn't strictly necessary, but helps guarantee round-trip
	out[13] = if negative && !decimal.is_zero() {
		0x00
	} else if decimal.is_zero() {
		0x80
	} else {
		0xFF
	};

	out
}

pub(crate) fn decode_decimal_lex(data: [u8; 15]) -> Result<Decimal> {
	let prefix = data[0];
	let mut coeff = [0u8; 12];

	let (negative, scale) = if prefix == 0x80 {
		(false, 0) // zero
	} else if prefix <= 0x7F {
		(true, 0x7F - prefix)
	} else {
		(false, prefix - 0x81)
	};

	if prefix == 0x80 {
		// Zero
		return Ok(Decimal::ZERO);
	} else if negative {
		// Negatives: invert coefficient
		for i in 0..12 {
			coeff[i] = !data[i + 1];
		}
	} else {
		coeff.copy_from_slice(&data[1..13]);
	}

	// Compose the bytes for Decimal::deserialize
	let mut bytes = [0u8; 16];
	bytes[0..12].copy_from_slice(&coeff);
	bytes[12] = scale
		| if negative {
			0x80
		} else {
			0
		};
	// The rest are already zero
	Ok(Decimal::deserialize(bytes))
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
			"0", "1", "-1", "123.456", "-123.456", "0.001", "-0.001", "1000000", "-1000000",
			"123.000", // Trailing zeros
			"0.100",   // Trailing zeros after decimal
		];

		for case in test_cases {
			let original = Decimal::from_str(case).unwrap();
			let encoded = encode_decimal_lex(original, 0);
			let decoded = decode_decimal_lex(encoded).unwrap();

			assert_eq!(
				original.normalize(),
				decoded.normalize(),
				"Roundtrip failed for {}: {} != {}",
				case,
				original,
				decoded
			);
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

			assert!(
				encoded_smaller < encoded_larger,
				"Lexicographic ordering failed: {} should be < {} but encoded bytes don't reflect this",
				smaller,
				larger
			);
		}
	}
}
