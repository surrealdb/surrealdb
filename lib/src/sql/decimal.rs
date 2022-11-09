use std::str::FromStr;

use bigdecimal::{num_bigint::Sign, BigDecimal};
use serde::{
	de::{self, Visitor},
	ser::SerializeTuple,
	Deserializer, Serialize,
};

#[derive(Clone, Debug)]
pub struct Decimal<'a>(&'a BigDecimal);

impl<'a, 'b: 'a> From<&'b BigDecimal> for Decimal<'a> {
	fn from(v: &'b BigDecimal) -> Self {
		Self(v)
	}
}

// 9999999 (len: 7) => 0b00000000_10011000_10010110_01111111
// reserved 1 word width for storekey deserialize_seq end marker
const DIGITS_PER_WORD: usize = 7;

fn parse_big_decimal(v: &BigDecimal) -> (Sign, u8, String, i64) {
	let sign = v.sign();
	if sign == Sign::NoSign {
		return (sign, 0, "".to_owned(), 0);
	}

	let v = v.abs();

	let (big_int, scale) = v.as_bigint_and_exponent();
	let precision_str = big_int.to_string();
	let mut chars = precision_str.chars();

	let (first, fraction) = chars.next().map(|c| (c, chars.as_str())).unwrap();
	// integer part 1 - 9
	let int = u8::from_str_radix(&first.to_string(), 10).unwrap();

	// get exp
	let digits = v.digits();
	let exp = digits as i64 - scale - 1;

	(sign, int, fraction.to_owned(), exp)
}

impl<'a> Serialize for Decimal<'a> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let (sign, mut int, mut fraction, exp) = parse_big_decimal(&self.0);
		if sign == Sign::NoSign {
			let mut seq = serializer.serialize_tuple(4)?;
			seq.serialize_element(&0x80u8)?;
			seq.serialize_element(&0u64)?;
			seq.serialize_element(&0u8)?;
			seq.serialize_element(&[0u8; 0])?;
			return seq.end();
		}
		if sign == Sign::Minus {
			let val = BigDecimal::from_str(&format!("{}.{}", int, fraction)).unwrap();
			let val = BigDecimal::from(10) - val;
			let s = val.to_string();
			let split = s.split(".").collect::<Vec<&str>>();
			int = u8::from_str(&split[0]).unwrap();
			fraction = split.get(1).unwrap_or(&"").to_string();
		}
		let exp_is_negative = exp < 0;
		let mut exp_part = exp.abs() as u64;
		let sign_part: u8 = match (sign, exp_is_negative) {
			(Sign::Plus, true) => {
				exp_part = u64::MAX - exp_part;
				0xa0
			}
			(Sign::Plus, false) => 0xc0,
			(Sign::Minus, true) => 0x40,
			(Sign::Minus, false) => {
				exp_part = u64::MAX - exp_part;
				0x20
			}
			_ => unreachable!(),
		};

		// fill 0 left align
		let pad = (DIGITS_PER_WORD - fraction.len() % DIGITS_PER_WORD) % DIGITS_PER_WORD;
		let width = fraction.len() + pad;
		let fraction = &format!("{:0<1$}", fraction, width);
		let fractional_part = fraction
			.chars()
			.collect::<Vec<char>>()
			.chunks(DIGITS_PER_WORD)
			.map(|c| u32::from_str(&c.iter().collect::<String>()).unwrap())
			.collect::<Vec<u32>>();

		let mut seq = serializer.serialize_tuple(4)?;
		seq.serialize_element(&sign_part)?;
		seq.serialize_element(&exp_part)?;
		seq.serialize_element(&int)?;
		seq.serialize_element(&fractional_part)?;
		seq.end()
	}
}

pub fn lexical_decode<'de, D>(deserializer: D) -> Result<BigDecimal, D::Error>
where
	D: Deserializer<'de>,
{
	struct BigDecimalVisitor;

	impl<'de> Visitor<'de> for BigDecimalVisitor {
		type Value = BigDecimal;
		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("expecting lexicographical order bytes!")
		}

		fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
		where
			A: serde::de::SeqAccess<'de>,
		{
			let sign_part = seq.next_element::<u8>()?.unwrap();
			let mut exp_part = seq.next_element::<u64>()?.unwrap();
			let (sign, exp_is_negative) = match sign_part {
				0x80 => (Sign::NoSign, false /* ignore */),
				0xc0 => (Sign::Plus, false),
				0xa0 => {
					exp_part = u64::MAX - exp_part;
					(Sign::Plus, true)
				}
				0x40 => (Sign::Minus, true),
				0x20 => {
					exp_part = u64::MAX - exp_part;
					(Sign::Minus, false)
				}
				_ => return Err(de::Error::custom(format!("Unexpected sign part {}", sign_part))),
			};

			let mut int_part = seq.next_element::<u8>()?.unwrap();
			let v32 = seq.next_element::<Vec<u32>>()?.unwrap();

			if sign == Sign::NoSign {
				return Ok(BigDecimal::from(0));
			}

			let mut fractional_part: String = v32
				.into_iter()
				.map(|v| format!("{:01$}", v, DIGITS_PER_WORD))
				.fold("".to_owned(), |acc, cur| acc + &cur);

			if sign == Sign::Minus {
				let decimal_str = format!("{}.{}", int_part, fractional_part);
				let decimal = BigDecimal::from_str(&decimal_str).map_err(de::Error::custom)?;
				let decimal = decimal - BigDecimal::from(10);
				(_, int_part, fractional_part, _) = parse_big_decimal(&decimal);
			}

			#[rustfmt::skip]
			let exp = if exp_is_negative { -(exp_part as i64) } else { exp_part as i64};
			#[rustfmt::skip]
			let negative_sign = if sign == Sign::Minus { "-" } else { "" };

			let decimal_str = format!("{}{}.{}E{}", negative_sign, int_part, fractional_part, exp);
			let decimal = BigDecimal::from_str(&decimal_str).map_err(de::Error::custom)?;

			Ok(decimal)
		}
	}

	deserializer.deserialize_tuple(4, BigDecimalVisitor)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::number::{number, Number};
	use bigdecimal::{num_bigint::Sign, BigDecimal};
	use serde::Deserialize;
	use std::str::FromStr;

	#[test]
	fn parse_decimal() {
		let d = BigDecimal::from_str("0.00").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::NoSign, 0, "".to_owned(), 0));

		let d = BigDecimal::from_str("1.02e12").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::Plus, 1, "02".to_owned(), 12));

		let d = BigDecimal::from_str("1.9999999999999999").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::Plus, 1, "9999999999999999".to_owned(), 0));

		let d = BigDecimal::from_str("-1.9999999999999999").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::Minus, 1, "9999999999999999".to_owned(), 0));

		let d = BigDecimal::from_str("-2.9999999999999999").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::Minus, 2, "9999999999999999".to_owned(), 0));

		let d = BigDecimal::from_str("-0.9999999999999999").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::Minus, 9, "999999999999999".to_owned(), -1));

		let d = BigDecimal::from_str("-9.999999999999999e-1").unwrap();
		assert_eq!(parse_big_decimal(&d), (Sign::Minus, 9, "999999999999999".to_owned(), -1));
	}

	#[test]
	fn lexicographical_serialize() {
		use crate::sql::serde::{beg_internal_serialization, end_internal_serialization};
		use storekey::serialize;

		fn parse_and_encode(s: &str) -> (Number, Vec<u8>) {
			let (_, n1) = number(s).unwrap();
			beg_internal_serialization();
			let buf = serialize(&n1).unwrap();
			end_internal_serialization();
			return (n1, buf);
		}

		let (_n1, b1) = parse_and_encode("-100.1111111112222222221e100");
		let (_n2, b2) = parse_and_encode("-100.1e100");
		let (_n3, b3) = parse_and_encode("-100e-100");
		let (_n4, b4) = parse_and_encode("-100e-101");
		let (_n5, b5) = parse_and_encode("0.0");
		let (_n6, b6) = parse_and_encode("100e-100");
		let (_n7, b7) = parse_and_encode("100e-99");
		let (_n8, b8) = parse_and_encode("100e99");
		let (_n9, b9) = parse_and_encode("100e100");
		let (_n10, b10) = parse_and_encode("1.1111111110167772172222222221e102");

		assert_eq!(b5, vec![0, 0, 0, 2, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

		// lexicographical_sort
		let mut v = vec![&b6, &b7, &b8, &b9, &b10, &b1, &b2, &b3, &b4, &b5];
		v.sort();
		assert_eq!(v, vec![&b1, &b2, &b3, &b4, &b5, &b6, &b7, &b8, &b9, &b10]);
	}

	#[test]
	fn storekey_serialize_deserialize() {
		use derive::Key;

		#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Key)]
		struct Val {
			a: i32,
			b: Number,
			c: f32,
		}

		let val = Val {
			a: 10,
			b: Number::Decimal(BigDecimal::from(200)),
			c: 1.2,
		};
		let enc = Val::encode(&val).unwrap();
		let dec = Val::decode(&enc).unwrap();
		assert_eq!(val, dec);

		let val = Val {
			a: 10,
			b: Number::Decimal(BigDecimal::from_str("1.1111111110167772172222222221e102").unwrap()),
			c: 1.2,
		};
		let enc = Val::encode(&val).unwrap();
		let dec = Val::decode(&enc).unwrap();
		assert_eq!(val, dec);

		let val = Val {
			a: 10,
			b: Number::Decimal(BigDecimal::from_str("-0.9999999999999999").unwrap()),
			c: 1.2,
		};
		let enc = Val::encode(&val).unwrap();
		let dec = Val::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn msgpack_serialize_deserialize() {
		use crate::sql::Value;

		let v = Value::from(BigDecimal::from(0));
		let enc = Vec::<u8>::from(&v);
		assert_eq!(v, Value::from(enc));

		let v = Value::from(BigDecimal::from(200));
		let enc = Vec::<u8>::from(&v);
		assert_eq!(v, Value::from(enc));

		let v = Value::from(BigDecimal::from_str("1.1111111110167772172222222221e102").unwrap());
		let enc = Vec::<u8>::from(&v);
		assert_eq!(v, Value::from(enc));

		let v = Value::from(BigDecimal::from_str("-0.9999999999999999").unwrap());
		let enc = Vec::<u8>::from(&v);
		assert_eq!(v, Value::from(enc));
	}
}
