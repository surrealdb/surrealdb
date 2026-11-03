pub use surrealdb_types::cbor::{
	TAG_BIGDECIMAL, TAG_DATE, TAG_F16, TAG_F32, TAG_I8, TAG_I16, TAG_I32, TAG_INTERVAL, TAG_REGEX,
	TAG_TIME, TAG_TIMESTAMP, TAG_U8, TAG_U16, TAG_U32, TAG_U64, encode, encode_v2,
};
use surrealdb_types::{RecordId, Value, cbor as convert};

/// A record id a client wrote as text, read with the SurrealQL parser.
fn record_id(input: &str) -> anyhow::Result<RecordId> {
	crate::syn::record_id(input)
}

const RECORD_ID: Option<convert::RecordIdParser> = Some(record_id);

/// A decoded CBOR item as a public value.
pub(crate) fn to_value(item: ciborium::Value) -> anyhow::Result<Value> {
	convert::to_value(item, RECORD_ID)
}

pub(crate) fn to_value_v2(item: ciborium::Value) -> anyhow::Result<Value> {
	convert::to_value_v2(item, RECORD_ID)
}

pub fn decode(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	convert::decode(bytes, recursion_limit, RECORD_ID)
}

pub fn decode_v2(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	convert::decode_v2(bytes, recursion_limit, RECORD_ID)
}

/// A client request: the CBOR item at the start of `bytes` (trailing bytes as `decode`), where
/// every shaped block (tag 57) at a value position decodes to the array of its rows.
///
/// A block decodes to a value a plain CBOR request could carry, through the same conversion, so
/// everything after the decode treats it as one. The decode is bounded: all blocks together
/// materialise at most `128 * bytes.len() + 1 MiB` budget units (about bytes of memory, twice what
/// the densest plain CBOR decodes to), and nest within `recursion_limit`.
/// A build without the `shaped` feature refuses blocks as `decode` does.
pub fn decode_request(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	#[cfg(feature = "shaped")]
	{
		let encoding = ciborium::de::from_reader_with_recursion_limit(bytes, recursion_limit)
			.map_err(|e| anyhow::anyhow!(e.to_string()))?;
		let mut blocks = convert::RequestBlocks {
			budget: surrealdb_types::shaped::Budget::new(128 * bytes.len() as u64 + (1 << 20)),
			limit: recursion_limit,
		};
		convert::to_request_value(
			encoding,
			&mut convert::Ctx::requests(&mut blocks, false, RECORD_ID),
		)
	}
	#[cfg(not(feature = "shaped"))]
	decode(bytes, recursion_limit)
}

pub fn decode_request_v2(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	#[cfg(feature = "shaped")]
	{
		let encoding = ciborium::de::from_reader_with_recursion_limit(bytes, recursion_limit)
			.map_err(|e| anyhow::anyhow!(e.to_string()))?;
		let mut blocks = convert::RequestBlocks {
			budget: surrealdb_types::shaped::Budget::new(128 * bytes.len() as u64 + (1 << 20)),
			limit: recursion_limit,
		};
		convert::to_request_value(
			encoding,
			&mut convert::Ctx::requests(&mut blocks, true, RECORD_ID),
		)
	}
	#[cfg(not(feature = "shaped"))]
	decode_v2(bytes, recursion_limit)
}

/// Decode `bytes` as exactly one CBOR item: bytes after the item are an error.
pub fn decode_exact(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	convert::decode_exact(bytes, recursion_limit, RECORD_ID)
}

pub fn decode_exact_v2(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	convert::decode_exact_v2(bytes, recursion_limit, RECORD_ID)
}

/// The `id` of a request envelope whose decoding failed.
///
/// A client matches a response to its request by this `id`, so a failure that
/// answers without it is never delivered to the call that caused it. The
/// envelope's other entries are skipped structurally, without being decoded,
/// so neither a parameter that cannot be converted to a [`Value`] nor one
/// nested deeper than `recursion_limit` hides the `id`; only the `id` value
/// itself is decoded, under that limit. `None` when the bytes are not a CBOR
/// map, are malformed before the `id`, or carry no usable `id`.
pub fn request_id(bytes: &[u8], recursion_limit: usize) -> Option<Value> {
	let mut scan = Scan {
		bytes,
		pos: 0,
	};
	let (major, _, entries) = scan.head()?;
	if major != 5 {
		return None;
	}
	let mut remaining = entries;
	loop {
		match remaining {
			Some(0) => return None,
			Some(n) => remaining = Some(n - 1),
			None if scan.peek()? == 0xff => return None,
			None => {}
		}
		let is_id = scan.key_is("id")?;
		scan.skip_item()?;
		let start = scan.pos;
		scan.skip_item()?;
		if is_id {
			let id: ciborium::Value = ciborium::de::from_reader_with_recursion_limit(
				&bytes[start..scan.pos],
				recursion_limit,
			)
			.ok()?;
			let val = to_value_v2(id).ok()?;
			return if matches!(
				val,
				Value::Null
					| Value::Uuid(_)
					| Value::Number(_)
					| Value::String(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
			) {
				Some(val)
			} else {
				None
			};
		}
	}
}

/// A cursor over CBOR bytes that walks item boundaries without building values.
struct Scan<'a> {
	bytes: &'a [u8],
	pos: usize,
}

impl Scan<'_> {
	fn peek(&self) -> Option<u8> {
		self.bytes.get(self.pos).copied()
	}

	fn take(&mut self, len: usize) -> Option<&[u8]> {
		let end = self.pos.checked_add(len)?;
		let out = self.bytes.get(self.pos..end)?;
		self.pos = end;
		Some(out)
	}

	/// An item's major type, additional info, and argument; the argument is
	/// `None` for the indefinite-length marker.
	fn head(&mut self) -> Option<(u8, u8, Option<u64>)> {
		let first = self.take(1)?[0];
		let (major, info) = (first >> 5, first & 31);
		let arg = match info {
			0..=23 => Some(u64::from(info)),
			24 => Some(u64::from(self.take(1)?[0])),
			25 => Some(u64::from(u16::from_be_bytes(self.take(2)?.try_into().ok()?))),
			26 => Some(u64::from(u32::from_be_bytes(self.take(4)?.try_into().ok()?))),
			27 => Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?)),
			31 => None,
			_ => return None,
		};
		Some((major, info, arg))
	}

	/// Whether the next item is the definite-length text `name`, leaving the
	/// cursor on that item either way.
	fn key_is(&mut self, name: &str) -> Option<bool> {
		let start = self.pos;
		let (major, _, len) = self.head()?;
		let is = major == 3
			&& len == Some(name.len() as u64)
			&& self.bytes.get(self.pos..self.pos + name.len())? == name.as_bytes();
		self.pos = start;
		Some(is)
	}

	/// Step over one complete item, however deeply it nests.
	///
	/// Iterative, with an explicit stack of how many children each open
	/// container still expects (`None` while it is indefinite-length), so the
	/// depth is bounded by the input length rather than the call stack.
	fn skip_item(&mut self) -> Option<()> {
		let mut stack: Vec<Option<u64>> = vec![Some(1)];
		fn complete(stack: &mut [Option<u64>]) {
			if let Some(Some(n)) = stack.last_mut() {
				*n -= 1;
			}
		}
		while let Some(top) = stack.last() {
			if *top == Some(0) {
				stack.pop();
				complete(&mut stack);
				continue;
			}
			if self.peek()? == 0xff {
				self.pos += 1;
				if stack.pop()?.is_some() {
					return None;
				}
				complete(&mut stack);
				continue;
			}
			let (major, info, arg) = self.head()?;
			match major {
				0 | 1 | 7 => {
					if info == 31 {
						return None;
					}
					complete(&mut stack);
				}
				2 | 3 => match arg {
					Some(len) => {
						self.take(usize::try_from(len).ok()?)?;
						complete(&mut stack);
					}
					None => stack.push(None),
				},
				4 => stack.push(arg),
				5 => stack.push(match arg {
					Some(n) => Some(n.checked_mul(2)?),
					None => None,
				}),
				_ => stack.push(Some(1)),
			}
		}
		Some(())
	}
}

#[cfg(test)]
mod tests {
	use ciborium::Value as Cbor;

	use super::*;

	fn encode_cbor(value: &Cbor) -> Vec<u8> {
		let mut bytes = Vec::new();
		ciborium::into_writer(value, &mut bytes).unwrap();
		bytes
	}

	fn text(value: &str) -> Cbor {
		Cbor::Text(value.to_owned())
	}

	#[test]
	fn the_id_survives_a_parameter_that_cannot_be_converted() {
		// A record id that is not one: tag 8 over text that does not parse.
		let bad_param = Cbor::Tag(8, Box::new(text("abc")));
		let envelope = Cbor::Map(vec![
			(text("id"), text("7")),
			(text("method"), text("query")),
			(
				text("params"),
				Cbor::Array(vec![text("RETURN $id"), Cbor::Map(vec![(text("id"), bad_param)])]),
			),
		]);
		let bytes = encode_cbor(&envelope);
		assert!(
			decode(&bytes, 64).is_err(),
			"the envelope must not decode, or this proves nothing"
		);
		assert_eq!(request_id(&bytes, 64), Some(Value::String("7".to_owned())));
	}

	#[test]
	fn the_id_survives_a_datetime_that_is_not_one() {
		// An invalid date, as a client sends one: tag 0 over text that is no
		// datetime, and tag 12 over seconds that are no number of seconds.
		for bad_date in [
			Cbor::Tag(0, Box::new(text("ffff"))),
			Cbor::Tag(12, Box::new(Cbor::Array(vec![Cbor::Float(f64::NAN), Cbor::Float(0.0)]))),
		] {
			let envelope = Cbor::Map(vec![
				(text("id"), text("7")),
				(text("method"), text("query")),
				(
					text("params"),
					Cbor::Array(vec![
						text("RETURN $cake"),
						Cbor::Map(vec![(text("cake"), bad_date)]),
					]),
				),
			]);
			let bytes = encode_cbor(&envelope);
			assert!(
				decode(&bytes, 64).is_err(),
				"the envelope must not decode, or this proves nothing"
			);
			assert_eq!(request_id(&bytes, 64), Some(Value::String("7".to_owned())));
		}
	}

	#[test]
	fn the_id_survives_parameters_nested_past_the_recursion_limit() {
		// `params` first, then `id`: a map `{params: [[[...]]], id: "7"}` whose
		// nesting is far beyond the limit, built by hand since the encoder
		// would recurse too.
		let mut bytes = vec![0xa2, 0x66];
		bytes.extend_from_slice(b"params");
		bytes.extend(std::iter::repeat_n(0x81, 100_000));
		bytes.push(0x80);
		bytes.extend_from_slice(&[0x62, b'i', b'd', 0x61, b'7']);
		assert_eq!(request_id(&bytes, 256), Some(Value::String("7".to_owned())));
	}

	#[test]
	fn the_id_is_found_past_indefinite_length_entries() {
		// {_ "params": [_ h'0102' (_ "a" "b")], "id": "x"}
		let mut bytes = vec![0xbf, 0x66];
		bytes.extend_from_slice(b"params");
		bytes.extend_from_slice(&[0x9f, 0x42, 1, 2, 0x7f, 0x61, b'a', 0x61, b'b', 0xff, 0xff]);
		bytes.extend_from_slice(&[0x62, b'i', b'd', 0x61, b'x', 0xff]);
		assert_eq!(request_id(&bytes, 256), Some(Value::String("x".to_owned())));
	}

	#[test]
	fn no_id_is_recovered_from_something_that_is_not_a_request() {
		assert_eq!(request_id(&encode_cbor(&text("id")), 64), None);
		assert_eq!(
			request_id(&encode_cbor(&Cbor::Map(vec![(text("method"), text("ping"))])), 64),
			None
		);
		assert_eq!(
			request_id(&encode_cbor(&Cbor::Map(vec![(text("id"), Cbor::Array(vec![]))])), 64),
			None
		);
		assert_eq!(request_id(b"\xff\xff", 64), None);
	}

	fn refusal(item: &Cbor) -> String {
		decode(&encode_cbor(item), 64).expect_err("is refused").to_string()
	}

	#[test]
	fn text_forms_of_datetime_uuid_and_duration_are_refused_with_their_replacement() {
		assert_eq!(
			refusal(&Cbor::Tag(0, Box::new(text("2020-01-01T00:00:00Z")))),
			"CBOR tag 0 (a datetime as text) is not accepted: send tag 12 [seconds, nanos]"
		);
		assert_eq!(
			refusal(&Cbor::Tag(9, Box::new(text("00000000-0000-0000-0000-000000000000")))),
			"CBOR tag 9 (a uuid as text) is not accepted: send tag 37 (16 bytes)"
		);
		assert_eq!(
			refusal(&Cbor::Tag(13, Box::new(text("1s")))),
			"CBOR tag 13 (a duration as text) is not accepted: send tag 14 [seconds, nanos]"
		);
		// Nested, and as a record id key.
		assert!(refusal(&Cbor::Array(vec![Cbor::Tag(0, Box::new(text("x")))])).contains("tag 0"));
		let key =
			Cbor::Tag(8, Box::new(Cbor::Array(vec![text("t"), Cbor::Tag(9, Box::new(text("x")))])));
		assert!(refusal(&key).contains("tag 9"));
	}

	#[test]
	fn the_forms_that_replace_them_decode() {
		let uuid = Cbor::Tag(37, Box::new(Cbor::Bytes(vec![0; 16])));
		assert!(matches!(decode(&encode_cbor(&uuid), 64), Ok(Value::Uuid(_))));
		let datetime = Cbor::Tag(12, Box::new(Cbor::Array(vec![0.into(), 0.into()])));
		assert!(matches!(decode(&encode_cbor(&datetime), 64), Ok(Value::Datetime(_))));
		let duration = Cbor::Tag(14, Box::new(Cbor::Array(vec![1.into(), 5.into()])));
		assert!(matches!(decode(&encode_cbor(&duration), 64), Ok(Value::Duration(_))));
	}

	#[test]
	fn duration_overflow_in_nanosecond_carry_is_refused() {
		let overflow = Cbor::Tag(
			14,
			Box::new(Cbor::Array(vec![
				Cbor::Integer(u64::MAX.into()),
				Cbor::Integer(1_000_000_000u32.into()),
			])),
		);
		assert_eq!(refusal(&overflow), "Expected a valid Duration value");

		let raw = [
			0xd8, 0x0e, 0x82, 0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1a, 0x3b,
			0x9a, 0xca, 0x00,
		];
		assert!(decode(&raw, 64).is_err());

		let max_valid = Cbor::Tag(
			14,
			Box::new(Cbor::Array(vec![
				Cbor::Integer(u64::MAX.into()),
				Cbor::Integer(999_999_999u32.into()),
			])),
		);
		assert!(matches!(decode(&encode_cbor(&max_valid), 64), Ok(Value::Duration(_))));
	}

	#[test]
	fn integers_outside_the_signed_64_bit_range_are_refused() {
		let big = Cbor::Integer(u64::MAX.into());
		assert_eq!(
			refusal(&big),
			"integer 18446744073709551615 is outside the 64-bit signed range"
		);
		let small = Cbor::Integer(ciborium::value::Integer::try_from(-(1i128 << 64)).unwrap());
		assert!(refusal(&small).contains("outside the 64-bit signed range"));
		let key = Cbor::Tag(8, Box::new(Cbor::Array(vec![text("t"), big])));
		assert!(refusal(&key).contains("outside the 64-bit signed range"));
		let edge = Cbor::Integer(i64::MIN.into());
		assert!(decode(&encode_cbor(&edge), 64).is_ok());
	}

	#[test]
	fn duplicate_map_keys_are_refused() {
		let map = Cbor::Map(vec![(text("a"), 1.into()), (text("a"), 2.into())]);
		assert_eq!(refusal(&map), "duplicate key a in a CBOR map");
		let key = Cbor::Tag(8, Box::new(Cbor::Array(vec![text("t"), map])));
		assert!(refusal(&key).contains("duplicate key"));
	}

	#[test]
	fn none_takes_only_a_null_payload() {
		assert!(matches!(
			decode(&encode_cbor(&Cbor::Tag(6, Box::new(Cbor::Null))), 64),
			Ok(Value::None)
		));
		assert!(refusal(&Cbor::Tag(6, Box::new(1.into()))).contains("NONE"));
	}

	#[test]
	fn sized_number_tags_refused_in_v1_and_accepted_in_v2() {
		use surrealdb_types::{Number, f16};

		let cases: &[(u64, Number, Value)] = &[
			(TAG_I8, Number::I8(-128), Value::Number(Number::Int(-128))),
			(TAG_U8, Number::U8(255), Value::Number(Number::Int(255))),
			(TAG_I16, Number::I16(-32768), Value::Number(Number::Int(-32768))),
			(TAG_U16, Number::U16(65535), Value::Number(Number::Int(65535))),
			(TAG_I32, Number::I32(-2_147_483_648), Value::Number(Number::Int(-2_147_483_648))),
			(TAG_U32, Number::U32(4_294_967_295), Value::Number(Number::Int(4_294_967_295))),
			(TAG_U64, Number::U64(42), Value::Number(Number::Int(42))),
			(TAG_F16, Number::F16(f16::from_f32(1.5)), Value::Number(Number::Float(1.5))),
			(TAG_F32, Number::F32(2.5), Value::Number(Number::Float(2.5))),
		];

		for (tag, num, widened) in cases {
			let v = Value::Number(num.clone());
			let v2_bytes = encode_v2(v.clone()).unwrap();
			assert_eq!(
				decode(&v2_bytes, 64).expect_err("v1 must refuse sized tag").to_string(),
				format!("CBOR tag {tag} is not accepted here: use cbor.2")
			);
			let decoded_v2 = decode_v2(&v2_bytes, 64).unwrap();
			assert_eq!(decoded_v2, v);
			if let (Value::Number(got), Value::Number(exp)) = (&decoded_v2, &v) {
				assert_eq!(got.kind(), exp.kind());
			}

			let v1_bytes = encode(v).unwrap();
			let decoded_v1 = decode(&v1_bytes, 64).unwrap();
			assert_eq!(&decoded_v1, widened);
			if let (Value::Number(got), Value::Number(exp)) = (&decoded_v1, widened) {
				assert_eq!(got.kind(), exp.kind());
			}
		}

		// u64::MAX round-trips in v2 with its width intact, and widens to Decimal in v1.
		let max_u64 = Value::Number(Number::U64(u64::MAX));
		let max_u64_bytes = encode_v2(max_u64.clone()).unwrap();
		let decoded_max = decode_v2(&max_u64_bytes, 64).unwrap();
		assert_eq!(decoded_max, max_u64);
		if let Value::Number(n) = decoded_max {
			assert_eq!(n.kind(), surrealdb_types::Kind::U64);
		}
		let max_u64_v1_bytes = encode(max_u64).unwrap();
		let decoded_max_v1 = decode(&max_u64_v1_bytes, 64).unwrap();
		assert_eq!(
			decoded_max_v1,
			Value::Number(Number::Decimal(rust_decimal::Decimal::from(u64::MAX)))
		);
	}

	#[test]
	fn temporal_tags_round_trip_in_v2_and_degrade_in_v1() {
		use surrealdb_types::{Date, Datetime, Duration, Interval, Time, Timestamp};

		let date = Value::Date(Date::from_days(20_000).unwrap());
		let time = Value::Time(Time::from_nanos(45_000_000_123).unwrap());
		let midnight_end = Value::Time(Time::MAX);
		let timestamp = Value::Timestamp(Timestamp::from_parts(-5, 7).unwrap());
		let interval = Value::Interval(Interval::new(-14, 3, -4_000_001));

		for (tag, v) in [
			(TAG_DATE, &date),
			(TAG_TIME, &time),
			(TAG_TIME, &midnight_end),
			(TAG_TIMESTAMP, &timestamp),
			(TAG_INTERVAL, &interval),
		] {
			let v2_bytes = encode_v2(v.clone()).unwrap();
			assert_eq!(decode_v2(&v2_bytes, 64).unwrap(), *v);
			assert_eq!(
				decode(&v2_bytes, 64).expect_err("v1 must refuse temporal tag").to_string(),
				format!("CBOR tag {tag} is not accepted here: use cbor.2")
			);
		}
		// The exact interval components survive.
		let Value::Interval(back) = decode_v2(&encode_v2(interval.clone()).unwrap(), 64).unwrap()
		else {
			unreachable!()
		};
		assert!(back.months() == -14 && back.days() == 3 && back.micros() == -4_000_001);
		// Tag 100 is an integer count of days since the epoch.
		assert_eq!(
			ciborium::from_reader::<Cbor, _>(encode_v2(date.clone()).unwrap().as_slice()).unwrap(),
			Cbor::Tag(TAG_DATE, Box::new(Cbor::Integer(20_000.into())))
		);

		let datetime = |secs, nanos| Value::Datetime(Datetime::try_from((secs, nanos)).unwrap());
		assert_eq!(decode(&encode(date).unwrap(), 64).unwrap(), datetime(20_000 * 86_400, 0));
		assert_eq!(decode(&encode(timestamp).unwrap(), 64).unwrap(), datetime(-5, 7));
		assert_eq!(
			decode(&encode(time).unwrap(), 64).unwrap(),
			Value::Duration(Duration::new(45, 123))
		);
		assert_eq!(
			decode(&encode(midnight_end).unwrap(), 64).unwrap(),
			Value::Duration(Duration::new(86_400, 0))
		);
		assert_eq!(
			decode(&encode(interval).unwrap(), 64).unwrap(),
			Value::String("P-1Y-2M3DT-4.000001S".to_owned())
		);
	}

	/// A bigdecimal takes the form each protocol carries: tag 192 over its text under `cbor.2`,
	/// keeping its scale; under `cbor`, which refuses tag 192, tag 10 when it fits a decimal and
	/// plain text otherwise.
	#[test]
	fn bigdecimal_takes_the_form_each_protocol_carries() {
		use surrealdb_types::{BigDecimal, Number};

		let fits: BigDecimal = "1.50".parse().unwrap();
		let fits = Value::Number(Number::BigDecimal(Box::new(fits)));
		assert_eq!(
			ciborium::from_reader::<Cbor, _>(encode_v2(fits.clone()).unwrap().as_slice()).unwrap(),
			Cbor::Tag(TAG_BIGDECIMAL, Box::new(text("1.50")))
		);
		let decimal = Value::Number(Number::Decimal(rust_decimal::Decimal::new(15, 1)));
		assert_eq!(encode(fits.clone()).unwrap(), encode(decimal).unwrap());
		assert_eq!(decode_v2(&encode_v2(fits.clone()).unwrap(), 64).unwrap(), fits);

		let wide = "1234567890123456789012345678901234567890.5";
		let value = Value::Number(Number::BigDecimal(Box::new(wide.parse().unwrap())));
		let v2_bytes = encode_v2(value.clone()).unwrap();
		assert_eq!(
			ciborium::from_reader::<Cbor, _>(v2_bytes.as_slice()).unwrap(),
			Cbor::Tag(TAG_BIGDECIMAL, Box::new(text(wide)))
		);
		assert_eq!(decode_v2(&v2_bytes, 64).unwrap(), value);
		assert_eq!(decode(&encode(value).unwrap(), 64).unwrap(), Value::String(wide.into()));
		assert_eq!(
			decode(&encode_cbor(&Cbor::Tag(TAG_BIGDECIMAL, Box::new(text(wide)))), 64)
				.expect_err("v1 must refuse tag 192")
				.to_string(),
			format!("CBOR tag {TAG_BIGDECIMAL} is not accepted here: use cbor.2")
		);
		assert!(
			decode_v2(&encode_cbor(&Cbor::Tag(TAG_BIGDECIMAL, Box::new(text("1.5.5")))), 64)
				.is_err()
		);
	}

	/// A regex is tag 193 over its pattern under `cbor.2`; `cbor` neither writes nor reads one.
	#[test]
	fn regex_round_trips_in_v2_and_is_refused_in_v1() {
		use surrealdb_types::{Array, Regex, regex_engine};

		// Patterns both regex engines compile, so the test holds with or without `regex-unicode`.
		let patterns = ["^[a-z]+$", "a/b", r"a\/b", r"(?i)ab\d{2,}", "", r"\x{e9}+"];
		for pattern in patterns {
			let compiled = regex_engine::compile(pattern, None).unwrap();
			let value = Value::Regex(Regex::from(compiled));
			let bytes = encode_v2(value.clone()).unwrap();
			assert_eq!(
				ciborium::from_reader::<Cbor, _>(bytes.as_slice()).unwrap(),
				Cbor::Tag(TAG_REGEX, Box::new(text(pattern)))
			);
			let Value::Regex(back) = decode_v2(&bytes, 64).unwrap() else {
				panic!("{pattern} decodes as a regex");
			};
			assert_eq!(back.as_str(), pattern);
			assert_eq!(
				decode(&bytes, 64).expect_err("v1 must refuse tag 193").to_string(),
				format!("CBOR tag {TAG_REGEX} is not accepted here: use cbor.2")
			);
			assert!(
				encode(value).expect_err("v1 has no regex form").to_string().contains("tag 193")
			);
		}
		// Nested, and as a client sends one in a request's parameters.
		let regex = Value::Regex("x+".parse().unwrap());
		let nested = Value::Array(Array::from(vec![regex.clone()]));
		assert_eq!(decode_v2(&encode_v2(nested.clone()).unwrap(), 64).unwrap(), nested);
		let request = encode_cbor(&Cbor::Map(vec![
			(text("method"), text("query")),
			(
				text("params"),
				Cbor::Array(vec![text("RETURN $r"), Cbor::Tag(TAG_REGEX, Box::new(text("x+")))]),
			),
		]));
		let Value::Object(request) = decode_request_v2(&request, 64).unwrap() else {
			panic!("a request decodes as an object");
		};
		assert_eq!(
			request["params"],
			Value::Array(Array::from(vec![Value::String("RETURN $r".into()), regex]))
		);
	}

	#[test]
	fn regex_tag_refuses_an_invalid_pattern_and_a_non_text_payload() {
		for (item, expected) in [
			(
				Cbor::Tag(TAG_REGEX, Box::new(text("(unclosed"))),
				"Expected a valid regex under tag 193",
			),
			(
				Cbor::Tag(TAG_REGEX, Box::new(text("a{99999999}"))),
				"Expected a valid regex under tag 193",
			),
			(
				Cbor::Tag(TAG_REGEX, Box::new(Cbor::Integer(1.into()))),
				"Expected a CBOR text under tag 193",
			),
			(
				Cbor::Tag(TAG_REGEX, Box::new(Cbor::Bytes(b"abc".to_vec()))),
				"Expected a CBOR text under tag 193",
			),
		] {
			let err = decode_v2(&encode_cbor(&item), 64).expect_err("must be refused").to_string();
			assert!(err.contains(expected), "expected {expected:?} in {err:?}");
		}
	}

	#[test]
	fn temporal_tags_refuse_out_of_range_and_malformed_payloads() {
		let bad = [
			Cbor::Tag(TAG_DATE, Box::new(Cbor::Integer(i64::from(i32::MAX).into()))),
			Cbor::Tag(TAG_DATE, Box::new(Cbor::Integer(5_000_000_000i64.into()))),
			Cbor::Tag(TAG_DATE, Box::new(text("2026-10-27"))),
			Cbor::Tag(TAG_TIME, Box::new(Cbor::Integer(86_400_000_000_001u64.into()))),
			Cbor::Tag(TAG_TIME, Box::new(Cbor::Integer((-1).into()))),
			Cbor::Tag(TAG_TIMESTAMP, Box::new(Cbor::Array(vec![1.into()]))),
			Cbor::Tag(
				TAG_TIMESTAMP,
				Box::new(Cbor::Array(vec![Cbor::Integer(i64::MAX.into()), 0.into()])),
			),
			Cbor::Tag(TAG_INTERVAL, Box::new(Cbor::Array(vec![1.into(), 2.into()]))),
			Cbor::Tag(
				TAG_INTERVAL,
				Box::new(Cbor::Array(vec![
					Cbor::Integer(5_000_000_000i64.into()),
					0.into(),
					0.into(),
				])),
			),
		];
		for item in bad {
			assert!(decode_v2(&encode_cbor(&item), 64).is_err(), "{item:?}");
		}
	}

	#[test]
	fn sized_number_tags_refuse_out_of_range_and_inexact_payloads() {
		let bad = [
			(
				Cbor::Tag(TAG_I8, Box::new(Cbor::Integer(128.into()))),
				"outside the 8-bit signed range",
			),
			(
				Cbor::Tag(TAG_I8, Box::new(Cbor::Integer((-129).into()))),
				"outside the 8-bit signed range",
			),
			(
				Cbor::Tag(TAG_U8, Box::new(Cbor::Integer(256.into()))),
				"outside the 8-bit unsigned range",
			),
			(
				Cbor::Tag(TAG_U8, Box::new(Cbor::Integer((-1).into()))),
				"outside the 8-bit unsigned range",
			),
			(
				Cbor::Tag(TAG_I16, Box::new(Cbor::Integer(32768.into()))),
				"outside the 16-bit signed range",
			),
			(
				Cbor::Tag(TAG_U16, Box::new(Cbor::Integer(65536.into()))),
				"outside the 16-bit unsigned range",
			),
			(
				Cbor::Tag(TAG_I32, Box::new(Cbor::Integer(2_147_483_648i64.into()))),
				"outside the 32-bit signed range",
			),
			(
				Cbor::Tag(TAG_U32, Box::new(Cbor::Integer(4_294_967_296i64.into()))),
				"outside the 32-bit unsigned range",
			),
			(
				Cbor::Tag(TAG_U64, Box::new(Cbor::Integer((-1).into()))),
				"outside the 64-bit unsigned range",
			),
			(
				Cbor::Tag(TAG_F16, Box::new(Cbor::Float(0.1))),
				"cannot be losslessly represented as f16",
			),
			(
				Cbor::Tag(TAG_F32, Box::new(Cbor::Float(0.1))),
				"cannot be losslessly represented as f32",
			),
		];
		for (item, expected_msg) in bad {
			let err = decode_v2(&encode_cbor(&item), 64).expect_err("must be refused").to_string();
			assert!(err.contains(expected_msg), "expected {expected_msg:?} in {err:?}");
		}
	}

	#[test]
	fn frozen_cbor_and_cbor_v2_are_byte_identical_for_pre_existing_kinds() {
		use std::collections::{BTreeMap, BTreeSet};
		use std::ops::Bound;

		use rust_decimal::Decimal;
		use surrealdb_types::{
			Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
			RecordIdKey, Set, Table, Uuid,
		};

		let samples = vec![
			Value::None,
			Value::Null,
			Value::Bool(true),
			Value::Bool(false),
			Value::Number(Number::Int(-9_007_199_254_740_991)),
			Value::Number(Number::Float(3.125)),
			Value::Number(Number::Decimal(Decimal::new(12345, 2))),
			Value::String("surrealdb".to_owned()),
			Value::Bytes(Bytes::from(vec![1u8, 2, 3, 4])),
			Value::Duration(Duration::new(60, 500)),
			Value::Datetime(Datetime::try_from((1_700_000_000, 123_456_789)).unwrap()),
			Value::Uuid(Uuid::nil()),
			Value::Table(Table::new("person")),
			Value::RecordId(RecordId {
				table: Table::new("person"),
				key: RecordIdKey::Number(42),
			}),
			Value::Geometry(Geometry::Point((1.25, -2.5).into())),
			Value::Range(Box::new(Range::new(
				Bound::Included(Value::Number(Number::Int(1))),
				Bound::Excluded(Value::Number(Number::Int(10))),
			))),
			Value::File(File::new("bucket".to_owned(), "key".to_owned())),
			Value::Set(Set::from(BTreeSet::from([
				Value::Number(Number::Int(1)),
				Value::String("a".to_owned()),
			]))),
			Value::Array(Array::from(vec![Value::Bool(true), Value::Number(Number::Int(7))])),
			Value::Object(Object::from(BTreeMap::from([(
				"k".to_owned(),
				Value::Number(Number::Float(0.5)),
			)]))),
		];

		for v in samples {
			let v1 = encode(v.clone()).unwrap();
			let v2 = encode_v2(v.clone()).unwrap();
			assert_eq!(v1, v2, "pre-existing value {v:?} changed bytes between cbor and cbor.2");
		}
	}

	#[cfg(not(feature = "shaped"))]
	#[test]
	fn a_build_without_shaped_frames_refuses_a_block_in_a_request() {
		let block = Cbor::Tag(57, Box::new(Cbor::Bytes(vec![1])));
		let bytes = encode_cbor(&Cbor::Array(vec![block]));
		let err = decode_request(&bytes, 64).expect_err("refused").to_string();
		assert!(err.contains("tag 57"), "{err}");
	}
}
