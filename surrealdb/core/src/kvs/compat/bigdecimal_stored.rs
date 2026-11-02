//! Frozen bytes for the stored BIGDECIMAL tag (`0x36`).
//!
//! A bigdecimal is written as its tag, a sign byte, the scale as a canonical varint and the
//! magnitude of the coefficient, little-endian. The tag is new in the stored-value grammar, so no
//! released binary writes it and no released binary reads it. These bytes pin the layout from
//! both sides, so a change to the payload of the tag fails here: each case must decode to the
//! value it was written from, and the encoder must write each byte for byte.

use std::str::FromStr;

use surrealdb_types::BigDecimal;

use crate::val::stored::{decode, encode};
use crate::val::{Number, Value};

/// The decimal text each case was written from, and the bytes the encoder writes for it.
const CASES: &[(&str, &[u8])] = &[
	("0", &[0x36, 0x00, 0x00]),
	("0.000", &[0x36, 0x00, 0x03]),
	("1.50", &[0x36, 0x00, 0x02, 0x96]),
	("-0.5", &[0x36, 0x01, 0x01, 0x05]),
	// A scale of 200 takes two varint bytes: 0xC8 0x01.
	("1e-200", &[0x36, 0x00, 0xC8, 0x01, 0x01]),
	// 2^96 - 1 at scale 28 is the largest Decimal magnitude: twelve 0xFF bytes, scale 0x1C.
	(
		"-7.9228162514264337593543950335",
		&[0x36, 0x01, 0x1C, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
	),
];

fn bigdecimal(text: &str) -> Value {
	Value::Number(Number::BigDecimal(Box::new(BigDecimal::from_str(text).expect("parses"))))
}

#[test]
fn bigdecimal_stored_bytes_are_frozen() {
	for (text, frozen) in CASES {
		let written = encode(&bigdecimal(text)).expect("a bigdecimal encodes");
		assert_eq!(written, *frozen, "the encoding of {text} changed");
	}
}

#[test]
fn bigdecimal_frozen_bytes_decode_to_their_values() {
	for (text, frozen) in CASES {
		let decoded = decode(frozen).unwrap_or_else(|e| panic!("{text} fails to decode: {e}"));
		let Value::Number(Number::BigDecimal(decoded)) = decoded else {
			panic!("{text} decodes to {decoded:?}, not a bigdecimal");
		};
		let expected = BigDecimal::from_str(text).expect("parses");
		assert_eq!(*decoded, expected, "{text}");
		assert_eq!(decoded.scale(), expected.scale(), "the scale of {text}");
	}
}
