mod geometry;
mod record;
mod value;

use surrealdb_protocol::proto::v1 as proto;
use surrealdb_protocol::{TryFromValue, TryIntoValue};

use crate::Value;

impl TryFromValue for Value {
	fn try_from_value(value: proto::Value) -> anyhow::Result<Self> {
		Value::try_from(value)
	}
}

impl TryIntoValue for Value {
	fn try_into_value(self) -> anyhow::Result<proto::Value> {
		proto::Value::try_from(self)
	}
}

/// Encode a value as a protobuf-encoded byte vector.
pub fn encode(value: &Value) -> anyhow::Result<Vec<u8>> {
	use prost::Message;
	let proto_value = proto::Value::try_from(value.clone())?;
	Ok(proto_value.encode_to_vec())
}

/// Decode a protobuf-encoded byte vector into a value.
pub fn decode(bytes: &[u8]) -> anyhow::Result<Value> {
	use prost::Message;
	let proto_value = proto::Value::decode(bytes)?;
	Value::try_from(proto_value)
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::ops::Bound;

	use chrono::{DateTime, Utc};
	use rstest::rstest;
	use rust_decimal::Decimal;

	use super::*;
	use crate::{
		Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
		RecordIdKey, RecordIdKeyRange, Regex, Set, Table, Uuid, object,
	};

	#[rstest]
	#[case::none(Value::None)]
	#[case::null(Value::Null)]
	#[case::bool(Value::Bool(true))]
	#[case::bool(Value::Bool(false))]
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
	#[case::duration(Value::Duration(Duration::default()))]
	#[case::datetime(Value::Datetime(Datetime(DateTime::<Utc>::from_timestamp(1_000_000_000, 0).unwrap())))]
	#[case::uuid(Value::Uuid(Uuid::default()))]
	#[case::string(Value::String("".to_string()))]
	#[case::string(Value::String("Hello, World!".to_string()))]
	#[case::bytes(Value::Bytes(Bytes(::bytes::Bytes::from(vec![1_u8, 2, 3, 4, 5]))))]
	#[case::bytes(Value::Bytes(Bytes(::bytes::Bytes::from(vec![0_u8; 1024]))))]
	#[case::table(Value::Table(Table::new("test_table")))]
	#[case::record_id(Value::RecordId(RecordId::new("test_table", 42)))]
	#[case::record_id(Value::RecordId(RecordId::new("test_table", "test_key")))]
	#[case::record_id(Value::RecordId(RecordId::new(
		"test_table",
		RecordIdKey::Object(Object(BTreeMap::from([
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
	#[case::record_id_range(Value::RecordId(RecordId::new(
		"test_table",
		RecordIdKey::Range(Box::new(RecordIdKeyRange {
			start: Bound::Included(RecordIdKey::String("a".to_string())),
			end: Bound::Unbounded,
		}))
	)))]
	#[case::file(Value::File(File::new("test_file", "test_file.txt")))]
	#[case::range(Value::Range(Box::new(Range::new(
		Bound::Included(Value::Number(Number::Int(42))),
		Bound::Included(Value::Number(Number::Int(43)))
	))))]
	#[case::range(Value::Range(Box::new(Range::new(Bound::Unbounded, Bound::Unbounded))))]
	#[case::regex(Value::Regex(Regex(regex::Regex::new("").unwrap())))]
	#[case::regex(Value::Regex(Regex(regex::Regex::new("test_regex").unwrap())))]
	#[case::array(Value::Array(Array::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
	#[case::object(Value::Object(object! { "key": "value".to_string() }))]
	#[case::set(Value::Set(Set::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))])))]
	#[case::geometry(Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))))]
	#[case::geometry(Value::Geometry(Geometry::Line(geo::LineString(vec![geo::Coord { x: 1.0, y: 2.0 }, geo::Coord { x: 3.0, y: 4.0 }]))))]
	#[case::geometry(Value::Geometry(Geometry::Collection(vec![Geometry::Point(geo::Point::new(1.0, 2.0))])))]
	fn test_proto_encode_decode(#[case] input: Value) {
		let encoded = encode(&input).expect("Failed to encode");
		let decoded = decode(&encoded).expect("Failed to decode");
		assert_eq!(input, decoded, "proto roundtrip failed for input: {input:?}");

		// Both wire codecs must agree on the value a given `Value` round-trips
		// to, since a gRPC engine and a WS/HTTP engine can observe the same
		// value.
		let fb_encoded = crate::encode(&input).expect("Failed to fb-encode");
		let fb_decoded: Value = crate::decode(&fb_encoded).expect("Failed to fb-decode");
		assert_eq!(fb_decoded, decoded, "fb and proto codecs disagree for input: {input:?}");
	}
}
