mod collections;
mod geometry;
mod kind;
mod misc;
mod primitives;
mod record;
mod surrealism;
mod table;
mod value;

use anyhow::Context;

pub use self::surrealism::*;
use crate::{Kind, SurrealValue, Value};

/// Trait for converting a type to a flatbuffers builder type.
pub trait ToFlatbuffers {
	/// The output type for the flatbuffers builder
	type Output<'bldr>;

	/// Convert the type to a flatbuffers builder type.
	fn to_fb<'bldr>(
		&self,
		builder: &mut ::flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>>;
}

/// Trait for converting a flatbuffers builder type to a type.
pub trait FromFlatbuffers {
	/// The input type from the flatbuffers builder
	type Input<'a>;

	/// Convert a flatbuffers builder type to a type.
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
	where
		Self: Sized;
}

/// Encode a value to a flatbuffers vector.
pub fn encode(value: &Value) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let value = value.to_fb(&mut fbb)?;
	fbb.finish(value, None);
	let data = fbb.finished_data().to_vec();
	Ok(data)
}

/// Maximum nesting depth accepted by the flatbuffers verifier.
///
/// Unlike `max_tables`, this bounds *recursion* in both the verifier and our
/// own recursive `from_fb` decode, so it must stay a small constant: tying it
/// to the payload length would let a deeply-nested message (still within the
/// transport size limit) recurse until the thread stack overflows, crashing
/// the process. Legitimate values never nest this deeply — the server caps
/// object/expression parsing and computation depth at ~100-128 — and each
/// logical level maps to a couple of flatbuffers tables, so 512 leaves ample
/// headroom above the default of 64 (too low — see issue #7037) while staying
/// far below any stack-overflow threshold.
const MAX_VERIFIER_DEPTH: usize = 512;

/// Build verifier options sized to the payload.
///
/// Every [`Value`] (and its inner union payload) is its own flatbuffers table,
/// so large results blow straight past the verifier default `max_tables` of
/// 1,000,000 and decoding fails with "Failed to decode fb value" — see
/// <https://github.com/surrealdb/surrealdb/issues/7037>.
///
/// A table occupies at least its 4-byte offset in the buffer, so the payload
/// length is a safe upper bound for the table count: it can never reject a
/// structurally valid buffer, yet stays finite and proportional to the
/// (transport-bounded) input rather than disabling the limit outright. The
/// nesting depth is bounded separately by [`MAX_VERIFIER_DEPTH`] — it must not
/// scale with the payload, or deep nesting becomes a stack-overflow DoS.
fn verifier_options(len: usize) -> flatbuffers::VerifierOptions {
	flatbuffers::VerifierOptions {
		max_tables: len,
		max_depth: MAX_VERIFIER_DEPTH,
		..Default::default()
	}
}

/// Decode a flatbuffers vector to a public value.
pub fn decode<T: SurrealValue>(value: &[u8]) -> anyhow::Result<T> {
	let opts = verifier_options(value.len());
	let value_fb = flatbuffers::root_with_opts::<surrealdb_protocol::fb::v1::Value>(&opts, value)
		.context("Failed to decode fb value")?;
	let value = Value::from_fb(value_fb).context("Failed to decode value from fb value")?;
	T::from_value(value).context("Failed to decode T from value")
}

/// Encode a kind to a flatbuffers vector.
pub fn encode_kind(kind: &Kind) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let value = kind.to_fb(&mut fbb)?;
	fbb.finish(value, None);
	let data = fbb.finished_data().to_vec();
	Ok(data)
}

/// Decode a flatbuffers vector to a public kind.
pub fn decode_kind(value: &[u8]) -> anyhow::Result<Kind> {
	let opts = verifier_options(value.len());
	let value_fb = flatbuffers::root_with_opts::<surrealdb_protocol::fb::v1::Kind>(&opts, value)
		.context("Failed to decode fb kind")?;
	let kind = Kind::from_fb(value_fb).context("Failed to decode kind from fb kind")?;
	Ok(kind)
}

#[cfg(test)]
mod tests {
	use chrono::{DateTime, Utc};
	use rstest::rstest;
	use rust_decimal::Decimal;

	use super::*;
	use crate::{
		Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
		RecordIdKey, Table, Uuid, object,
	};

	#[rstest]
	#[case::none(Value::None)]
	#[case::null(Value::Null)]
	#[case::bool(Value::Bool(true))]
	#[case::bool(Value::Bool(false))]
	// Numbers
	#[case::int(Value::Number(Number::Int(42)))]
	#[case::int(Value::Number(Number::Int(i64::MIN)))]
	#[case::int(Value::Number(Number::Int(i64::MAX)))]
	#[case::float(Value::Number(Number::Float(1.23)))]
	#[case::float(Value::Number(Number::Float(f64::MIN)))]
	#[case::float(Value::Number(Number::Float(f64::MAX)))]
	#[case::float(Value::Number(Number::Float(f64::NAN)))]
	#[case::float(Value::Number(Number::Float(f64::INFINITY)))]
	#[case::float(Value::Number(Number::Float(f64::NEG_INFINITY)))]
	#[case::decimal(Value::Number(Number::Decimal(Decimal::new(123, 2))))]
	// Duration
	#[case::duration(Value::Duration(Duration::default()))]
	// Datetime
	#[case::datetime(Value::Datetime(Datetime(DateTime::<Utc>::from_timestamp(1_000_000_000, 0).unwrap())))]
	// UUID
	#[case::uuid(Value::Uuid(Uuid::default()))]
	// String
	#[case::string(Value::String("".to_string()))]
	#[case::string(Value::String("Hello, World!".to_string()))]
	// Bytes
	#[case::bytes(Value::Bytes(Bytes(::bytes::Bytes::from(vec![1_u8, 2, 3, 4, 5]))))]
	#[case::bytes(Value::Bytes(Bytes(::bytes::Bytes::from(vec![0_u8; 1024]))))]
	// Table
	#[case::table(Value::Table(Table::new("test_table")))]
	// RecordId
	#[case::record_id(Value::RecordId(RecordId::new("test_table", 42)))]
	#[case::record_id(Value::RecordId(RecordId::new("test_table", "test_key")))]
	#[case::record_id(Value::RecordId(RecordId::new(
		"test_table", 
		RecordIdKey::Object(Object(std::collections::BTreeMap::from([
			("key".to_string(), Value::String("value".to_string()))
		])))
	)))]
	#[case::record_id(Value::RecordId(RecordId::new(
		"test_table", 
		RecordIdKey::Array(Array(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
		]))
	)))]
	// File
	#[case::file(Value::File(File::new("test_file", "test_file.txt")))]
	// Range
	#[case::range(Value::Range(Box::new(Range::new(
		std::collections::Bound::Included(Value::Number(Number::Int(42))),
		std::collections::Bound::Included(Value::Number(Number::Int(43)))
	))))]
	// Regex
	#[case::regex(Value::Regex("".parse().unwrap()))]
	#[case::regex(Value::Regex("test_regex".parse().unwrap()))]
	// Array
	#[case::array(Value::Array(Array::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
	// Object
	#[case::object(Value::Object(object! { "key": "value".to_string() }))]
	// Geometry
	#[case::geometry(Value::Geometry(Geometry::Point(geo_types::Point::new(1.0, 2.0))))]
	fn test_encode_decode(#[case] input: Value) {
		let encoded = encode(&input).expect("Failed to encode");
		let decoded = decode::<Value>(&encoded).expect("Failed to decode");
		assert_eq!(input, decoded);
	}

	/// A large result set produces more than the verifier's default
	/// `max_tables` (1,000,000) flatbuffers tables — every element is a table,
	/// and so is its inner union payload — which used to fail decoding with
	/// "Failed to decode fb value". Regression test for
	/// <https://github.com/surrealdb/surrealdb/issues/7037>.
	#[test]
	fn decode_large_array_exceeding_default_max_tables() {
		// > 500k elements => > 1,000,000 tables once inner payloads are counted.
		let input = Value::Array(Array::from(
			(0..600_000).map(|i| Value::Number(Number::Int(i))).collect::<Vec<_>>(),
		));
		let encoded = encode(&input).expect("Failed to encode");
		let decoded = decode::<Value>(&encoded).expect("Failed to decode large array");
		assert_eq!(input, decoded);
	}

	/// Build a value nested `depth` arrays deep around an integer leaf.
	fn nested_array(depth: usize) -> Value {
		let mut value = Value::Number(Number::Int(1));
		for _ in 0..depth {
			value = Value::Array(Array::from(vec![value]));
		}
		value
	}

	/// The verifier's nesting-depth limit must stay a fixed constant rather than
	/// scale with the payload length: otherwise a deeply-nested message that is
	/// still within the transport size limit would recurse (in the verifier and
	/// in `from_fb`) until the thread stack overflows — a denial-of-service.
	/// Values nested within [`MAX_VERIFIER_DEPTH`] decode; deeper ones are
	/// rejected rather than crashing the process.
	#[test]
	fn decode_rejects_excessive_nesting_depth() {
		// Comfortably nested values round-trip — well above the old default
		// `max_depth` of 64 that used to reject legitimate nested records.
		let ok = nested_array(100);
		let encoded = encode(&ok).expect("Failed to encode nested value");
		let decoded = decode::<Value>(&encoded).expect("Failed to decode nested value");
		assert_eq!(ok, decoded);

		// Excessively nested values are rejected by the verifier.
		let deep = nested_array(600);
		let encoded = encode(&deep).expect("Failed to encode deeply-nested value");
		assert!(decode::<Value>(&encoded).is_err(), "expected deep nesting to be rejected");
	}
}
