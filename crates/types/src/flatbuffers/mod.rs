mod collections;
mod geometry;
mod kind;
mod misc;
mod primitives;
mod record;
mod table;
mod value;

use anyhow::Context;

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

/// Decode a flatbuffers vector to a public value.
pub fn decode<T: SurrealValue>(value: &[u8]) -> anyhow::Result<T> {
	let value_fb = flatbuffers::root::<surrealdb_protocol::fb::v1::Value>(value)
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
	let value_fb = flatbuffers::root::<surrealdb_protocol::fb::v1::Kind>(value)
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
		Array, Bytes, Datetime, Duration, File, Geometry, Number, Range, RecordId, Regex, Table,
		Uuid, object,
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
	#[case::bytes(Value::Bytes(Bytes::from(vec![1, 2, 3, 4, 5])))]
	#[case::bytes(Value::Bytes(Bytes::from(vec![0; 1024])))]
	// Table
	#[case::table(Value::Table(Table::new("test_table")))]
	// RecordId
	#[case::record_id(Value::RecordId(RecordId::new("test_table", 42)))]
	#[case::record_id(Value::RecordId(RecordId::new("test_table", "test_key")))]
	// File
	#[case::file(Value::File(File::new("test_file", "test_file.txt")))]
	// Range
	#[case::range(Value::Range(Box::new(Range::new(
		std::collections::Bound::Included(Value::Number(Number::Int(42))),
		std::collections::Bound::Included(Value::Number(Number::Int(43)))
	))))]
	// Regex
	#[case::regex(Value::Regex(Regex(regex::Regex::new("").unwrap())))]
	#[case::regex(Value::Regex(Regex(regex::Regex::new("test_regex").unwrap())))]
	// Array
	#[case::array(Value::Array(Array::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
	// Object
	#[case::object(Value::Object(object! { "key": "value".to_string() }))]
	// Geometry
	#[case::geometry(Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))))]
	fn test_encode_decode(#[case] input: Value) {
		let encoded = encode(&input).expect("Failed to encode");
		let decoded = decode::<Value>(&encoded).expect("Failed to decode");
		assert_eq!(input, decoded);
	}
}
