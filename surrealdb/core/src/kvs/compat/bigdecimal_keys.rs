//! Frozen bytes for the `storekey` encodings of a `bigdecimal`.
//!
//! An index entry encodes a number under `IndexFormat` as its order-preserving decimal buffer, and
//! a record-id component under `()` as that buffer followed by the number's kind tag. A
//! `bigdecimal` is spelled in the buffer every number kind shares, from its coefficient's digits
//! as written and the scale of its leading digit, so its bytes are those of the `decimal` with the
//! same coefficient and scale. A coefficient past 39 digits, or a scale outside the two-byte
//! range, takes the buffer's wide forms, which no other kind reaches. No released binary writes a
//! `bigdecimal`. These bytes pin the layout from both sides: each case is written as exactly these
//! bytes and decodes from them to the value it was written from, scale included under `()`.
//!
//! Never edit the frozen bytes and never regenerate them to make a failing test pass.

use std::str::FromStr;

use surrealdb_types::BigDecimal;

use crate::val::{IndexFormat, Number};

/// The decimal text of each case.
const TEXTS: &[&str] = &[
	"0",
	"1.5",
	"1.50",
	"-1.5",
	"1500",
	"1234567890123456789012345678901234567890",
	"-12345678901234567890123456789012345678901234567890123456789012345",
	"0.000000000000000000000000000000000000000000000000000000000000000000001",
];

/// The cases whose scale lies below the two-byte range: the digits and the power of ten of the
/// coefficient.
const WIDE: &[(bool, &[u8], i64)] = &[(false, b"1", -6_000), (true, b"7", -5_379)];

fn bigdecimal(text: &str) -> Number {
	Number::BigDecimal(Box::new(BigDecimal::from_str(text).expect("parses")))
}

fn cases() -> Vec<(String, Number)> {
	let mut cases: Vec<(String, Number)> =
		TEXTS.iter().map(|text| ((*text).to_owned(), bigdecimal(text))).collect();
	for (negative, digits, exponent) in WIDE {
		let value = BigDecimal::from_digits(*negative, digits, *exponent).expect("in range");
		cases.push((
			format!(
				"{}{}e{exponent}",
				if *negative {
					"-"
				} else {
					""
				},
				str_of(digits)
			),
			Number::BigDecimal(Box::new(value)),
		));
	}
	cases
}

fn str_of(digits: &[u8]) -> &str {
	std::str::from_utf8(digits).expect("ascii digits")
}

fn keys(n: &Number) -> [Vec<u8>; 2] {
	[
		storekey::encode_vec_format::<IndexFormat, _>(n).expect("encodes"),
		storekey::encode_vec(n).expect("encodes"),
	]
}

/// FROZEN. For each case, its key under `IndexFormat` and under `()`.
#[rustfmt::skip]
const KEYS: &[(&str, [&[u8]; 2])] = &[
	("0", [
		&[128, 1, 0, 0],
		&[128, 1, 0, 0, 14],
	]),
	("1.5", [
		&[160, 25, 25, 38, 15, 1, 0, 0],
		&[160, 25, 25, 38, 15, 1, 0, 0, 14],
	]),
	("1.50", [
		&[160, 25, 25, 38, 16, 1, 0, 0],
		&[160, 25, 25, 38, 16, 1, 0, 0, 14],
	]),
	("-1.5", [
		&[64, 233, 232, 217, 240, 1, 0, 0],
		&[64, 233, 232, 217, 240, 1, 0, 0, 14],
	]),
	("1500", [
		&[160, 25, 28, 38, 17, 15, 1, 0, 0],
		&[160, 25, 28, 38, 17, 15, 1, 0, 0, 14],
	]),
	("1234567890123456789012345678901234567890", [
		&[160, 25, 64, 35, 69, 103, 137, 161, 35, 69, 103, 137, 161, 35, 69, 103, 137, 161, 35, 69, 103, 137, 161, 15, 1, 0, 0],
		&[160, 25, 64, 35, 69, 103, 137, 161, 35, 69, 103, 137, 161, 35, 69, 103, 137, 161, 35, 69, 103, 137, 161, 15, 1, 0, 0, 14],
	]),
	("-12345678901234567890123456789012345678901234567890123456789012345", [
		&[64, 233, 168, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 159, 1, 0, 0],
		&[64, 233, 168, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 152, 118, 94, 220, 186, 159, 1, 0, 0, 14],
	]),
	("0.000000000000000000000000000000000000000000000000000000000000000000001", [
		&[160, 24, 211, 32, 1, 0, 0],
		&[160, 24, 211, 32, 1, 0, 0, 14],
	]),
	("1e-6000", [
		&[160, 1, 1, 255, 255, 255, 255, 255, 255, 253, 144, 32, 1, 0, 0],
		&[160, 1, 1, 255, 255, 255, 255, 255, 255, 253, 144, 32, 1, 0, 0, 14],
	]),
	("-7e-5379", [
		&[64, 255, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 127, 1, 0, 0],
		&[64, 255, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 127, 1, 0, 0, 14],
	]),
];

#[test]
fn bigdecimal_key_bytes_are_frozen() {
	let cases = cases();
	assert_eq!(cases.len(), KEYS.len(), "every case has frozen keys");
	for ((name, n), (frozen_name, frozen)) in cases.iter().zip(KEYS) {
		assert_eq!(name, frozen_name);
		let [index, record] = keys(n);
		assert_eq!(index, frozen[0], "the index key of {name} changed");
		assert_eq!(record, frozen[1], "the record-id key of {name} changed");
	}
}

#[test]
fn bigdecimal_frozen_keys_decode_to_their_values() {
	for ((name, n), (_, frozen)) in cases().iter().zip(KEYS) {
		let indexed: Number = storekey::decode_borrow_format::<IndexFormat, Number>(frozen[0])
			.unwrap_or_else(|e| panic!("the index key of {name} fails to decode: {e}"));
		// The index format carries no kind: a value beyond `decimal` reads back as the nearest
		// float, which no query path reads, and every other value as itself.
		if !matches!(indexed, Number::Float(_)) {
			assert_eq!(&indexed, n, "the index key of {name}");
		}
		let record: Number = storekey::decode_borrow(frozen[1])
			.unwrap_or_else(|e| panic!("the record-id key of {name} fails to decode: {e}"));
		let (Number::BigDecimal(record), Number::BigDecimal(written)) = (&record, n) else {
			panic!("the record-id key of {name} decodes to {record:?}");
		};
		assert_eq!(record, written, "the record-id key of {name}");
		if !written.is_zero() {
			assert_eq!(record.scale(), written.scale(), "the scale of {name}");
		}
	}
}

/// A bigdecimal within a decimal's range has the index key of the decimal written with the same
/// digits, so an index holds the two as one value once both are respelled without trailing zeros.
#[test]
fn bigdecimal_index_keys_are_the_keys_of_their_decimals() {
	for text in ["0", "1.5", "1.50", "-1.5", "1500"] {
		let decimal = Number::Decimal(rust_decimal::Decimal::from_str(text).expect("a decimal"));
		assert_eq!(keys(&bigdecimal(text))[0], keys(&decimal)[0], "{text}");
	}
}

/// The frozen index keys order as their values.
#[test]
fn bigdecimal_frozen_index_keys_order_as_their_values() {
	let mut keyed: Vec<(Number, &[u8])> =
		cases().into_iter().zip(KEYS).map(|((_, n), (_, keys))| (n, keys[0])).collect();
	keyed.sort_by(|a, b| a.0.cmp(&b.0));
	for pair in keyed.windows(2) {
		if pair[0].0 < pair[1].0 {
			assert!(pair[0].1 < pair[1].1, "{} < {}", pair[0].0, pair[1].0);
		}
	}
}

/// Prints [`KEYS`]. Ignored so it never runs in CI.
#[test]
#[ignore = "prints the frozen keys; run by hand when a case is added"]
fn generate() {
	for (name, n) in cases() {
		let [index, record] = keys(&n);
		println!("\t(\"{name}\", [\n\t\t&{index:?},\n\t\t&{record:?},\n\t]),");
	}
}
