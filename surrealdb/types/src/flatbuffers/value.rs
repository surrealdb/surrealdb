use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{
	Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId, Regex, Set,
	Table, Uuid, Value,
};

impl ToFlatbuffers for Value {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let args = match self {
			Self::Date(_) | Self::Time(_) | Self::Timestamp(_) | Self::Interval(_) => {
				return self.degrade_temporal().to_fb(builder);
			}
			Self::None => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::NONE,
				value: None,
			},
			Self::Null => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Null,
				value: Some(
					proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {})
						.as_union_value(),
				),
			},
			Self::Bool(b) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Bool,
				value: Some(b.to_fb(builder)?.as_union_value()),
			},
			Self::Number(n) => match n.wide() {
				crate::Wide::Int(i) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Int64,
					value: Some(i.to_fb(builder)?.as_union_value()),
				},
				crate::Wide::Float(f) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Float64,
					value: Some(f.to_fb(builder)?.as_union_value()),
				},
				crate::Wide::Decimal(d) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Decimal,
					value: Some(d.to_fb(builder)?.as_union_value()),
				},
				// A bigdecimal that fits a decimal exactly travels as one. Any other value
				// travels as its text, since the wire has no arbitrary-precision decimal.
				crate::Wide::BigDecimal(b) => match Decimal::try_from(b) {
					Ok(d) => proto_fb::ValueArgs {
						value_type: proto_fb::ValueType::Decimal,
						value: Some(d.to_fb(builder)?.as_union_value()),
					},
					Err(_) => proto_fb::ValueArgs {
						value_type: proto_fb::ValueType::String,
						value: Some(b.to_string().to_fb(builder)?.as_union_value()),
					},
				},
			},
			Self::String(s) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::String,
				value: Some(s.to_fb(builder)?.as_union_value()),
			},
			Self::Bytes(b) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Bytes,
				value: Some(b.to_fb(builder)?.as_union_value()),
			},
			Self::Table(t) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Table,
				value: Some(t.to_fb(builder)?.as_union_value()),
			},
			Self::RecordId(record_id) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::RecordId,
				value: Some(record_id.to_fb(builder)?.as_union_value()),
			},
			Self::Duration(d) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Duration,
				value: Some(d.to_fb(builder)?.as_union_value()),
			},
			Self::Datetime(dt) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Datetime,
				value: Some(dt.0.to_fb(builder)?.as_union_value()),
			},
			Self::Uuid(uuid) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Uuid,
				value: Some(uuid.to_fb(builder)?.as_union_value()),
			},
			Self::Object(obj) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Object,
				value: Some(obj.to_fb(builder)?.as_union_value()),
			},
			Self::Array(arr) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Array,
				value: Some(arr.to_fb(builder)?.as_union_value()),
			},
			Self::Set(set) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Set,
				value: Some(set.to_fb(builder)?.as_union_value()),
			},
			Self::Geometry(geometry) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Geometry,
				value: Some(geometry.to_fb(builder)?.as_union_value()),
			},
			Self::File(file) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::File,
				value: Some(file.to_fb(builder)?.as_union_value()),
			},
			Self::Regex(regex) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Regex,
				value: Some(regex.to_fb(builder)?.as_union_value()),
			},
			Self::Range(range) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Range,
				value: Some(range.to_fb(builder)?.as_union_value()),
			},
		};

		Ok(proto_fb::Value::create(builder, &args))
	}
}

impl FromFlatbuffers for Value {
	type Input<'a> = proto_fb::Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.value_type() {
			// `NONE` is the union's implicit-unset sentinel; `None` (tag 21)
			// is the explicit variant a newer peer may send instead. Both
			// read as `Value::None`; only encoding chooses between them.
			proto_fb::ValueType::NONE | proto_fb::ValueType::None => Ok(Value::None),
			proto_fb::ValueType::Null => Ok(Value::Null),
			proto_fb::ValueType::Bool => Ok(Value::Bool(
				input.value_as_bool().ok_or_else(|| anyhow!("Missing Bool in Value"))?.value(),
			)),
			proto_fb::ValueType::Int64 => Ok(Value::Number(Number::Int(
				input.value_as_int_64().ok_or_else(|| anyhow!("Missing Int64 in Value"))?.value(),
			))),
			proto_fb::ValueType::Float64 => Ok(Value::Number(Number::Float(
				input
					.value_as_float_64()
					.ok_or_else(|| anyhow!("Missing Float64 in Value"))?
					.value(),
			))),
			proto_fb::ValueType::Decimal => {
				let decimal_value =
					input.value_as_decimal().ok_or_else(|| anyhow!("Missing Decimal in Value"))?;
				Ok(Value::Number(Number::Decimal(Decimal::from_fb(decimal_value)?)))
			}
			proto_fb::ValueType::String => {
				let string_value =
					input.value_as_string().ok_or_else(|| anyhow!("Missing String in Value"))?;
				Ok(Value::String(String::from_fb(string_value)?))
			}
			proto_fb::ValueType::Bytes => {
				let bytes_value =
					input.value_as_bytes().ok_or_else(|| anyhow!("Missing Bytes in Value"))?;
				Ok(Value::Bytes(Bytes::from_fb(bytes_value)?))
			}
			proto_fb::ValueType::Table => {
				let table_value =
					input.value_as_table().ok_or_else(|| anyhow!("Missing Table in Value"))?;
				let table = Table::from_fb(table_value)?;
				Ok(Value::Table(table))
			}
			proto_fb::ValueType::RecordId => {
				let record_id_value = input
					.value_as_record_id()
					.ok_or_else(|| anyhow!("Missing RecordId in Value"))?;
				let thing = RecordId::from_fb(record_id_value)?;
				Ok(Value::RecordId(thing))
			}
			proto_fb::ValueType::Duration => {
				let duration_value = input
					.value_as_duration()
					.ok_or_else(|| anyhow!("Missing Duration in Value"))?;
				let duration = Duration::from_fb(duration_value)?;
				Ok(Value::Duration(duration))
			}
			proto_fb::ValueType::Datetime => {
				let datetime_value = input
					.value_as_datetime()
					.ok_or_else(|| anyhow!("Missing Datetime in Value"))?;
				let dt = DateTime::<Utc>::from_fb(datetime_value)?;
				Ok(Value::Datetime(Datetime(dt)))
			}
			proto_fb::ValueType::Uuid => {
				let uuid_value =
					input.value_as_uuid().ok_or_else(|| anyhow!("Missing Uuid in Value"))?;
				let uuid = Uuid::from_fb(uuid_value)?;
				Ok(Value::Uuid(uuid))
			}
			proto_fb::ValueType::Object => {
				let object_value =
					input.value_as_object().ok_or_else(|| anyhow!("Missing Object in Value"))?;
				let object = Object::from_fb(object_value)?;
				Ok(Value::Object(object))
			}
			proto_fb::ValueType::Array => {
				let array_value =
					input.value_as_array().ok_or_else(|| anyhow!("Missing Array in Value"))?;
				let array = Array::from_fb(array_value)?;
				Ok(Value::Array(array))
			}
			proto_fb::ValueType::Set => {
				let set_value =
					input.value_as_set().ok_or_else(|| anyhow!("Missing Set in Value"))?;
				let set = Set::from_fb(set_value)?;
				Ok(Value::Set(set))
			}
			proto_fb::ValueType::Geometry => {
				let geometry_value = input
					.value_as_geometry()
					.ok_or_else(|| anyhow!("Missing Geometry in Value"))?;
				let geometry = Geometry::from_fb(geometry_value)?;
				Ok(Value::Geometry(geometry))
			}
			proto_fb::ValueType::File => {
				let file_value =
					input.value_as_file().ok_or_else(|| anyhow!("Missing File in Value"))?;
				let file = File::from_fb(file_value)?;
				Ok(Value::File(file))
			}
			proto_fb::ValueType::Regex => {
				let regex_value =
					input.value_as_regex().ok_or_else(|| anyhow!("Missing Regex in Value"))?;
				let regex = Regex::from_fb(regex_value)?;
				Ok(Value::Regex(regex))
			}
			proto_fb::ValueType::Range => {
				let range_value =
					input.value_as_range().ok_or_else(|| anyhow!("Missing Range in Value"))?;
				let range = Range::from_fb(range_value)?;
				Ok(Value::Range(Box::new(range)))
			}
			_ => Err(anyhow!(
				"Unsupported value type for Flatbuffers deserialization: {:?}",
				input.value_type()
			)),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::ops::Bound;
	use std::str::FromStr;

	use chrono::{DateTime, Utc};
	use rstest::rstest;
	use rust_decimal::Decimal;
	use surrealdb_protocol::fb::v1 as proto_fb;

	use super::*;
	use crate::*;

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
	#[case::string(Value::String("Hello, World!".to_string()))]
	#[case::bytes(Value::Bytes(Bytes(::bytes::Bytes::from(vec![1_u8, 2, 3, 4, 5]))))]
	#[case::thing(Value::RecordId(RecordId::new("test_table", RecordIdKey::Number(42))))] // Example Thing
	#[case::thing_range(Value::RecordId(RecordId::new("test_table", RecordIdKey::Range(Box::new(RecordIdKeyRange { start: Bound::Included(RecordIdKey::String("a".to_string())), end: Bound::Unbounded })))))]
	#[case::object(Value::Object(Object::from(BTreeMap::from([("key".to_string(), Value::String("value".to_owned()))]))))]
	#[case::array(Value::Array(Array::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
	#[case::geometry::point(Value::Geometry(Geometry::Point(geo_types::Point::new(1.0, 2.0))))]
	#[case::geometry::line(Value::Geometry(Geometry::Line(geo_types::LineString(vec![geo_types::Coord { x: 1.0, y: 2.0 }, geo_types::Coord { x: 3.0, y: 4.0 }]))))]
	#[case::geometry::polygon(Value::Geometry(Geometry::Polygon(geo_types::Polygon::new(
		geo_types::LineString(vec![geo_types::Coord { x: 0.0, y: 0.0 }, geo_types::Coord { x: 1.0, y: 1.0 }, geo_types::Coord { x: 0.0, y: 1.0 }]),
		vec![geo_types::LineString(vec![geo_types::Coord { x: 0.5, y: 0.5 }, geo_types::Coord { x: 0.75, y: 0.75 }])]
	))))]
	#[case::geometry::multipoint(Value::Geometry(Geometry::MultiPoint(geo_types::MultiPoint(vec![geo_types::Point::new(1.0, 2.0), geo_types::Point::new(3.0, 4.0)]))))]
	#[case::geometry::multiline(Value::Geometry(Geometry::MultiLine(geo_types::MultiLineString(vec![geo_types::LineString(vec![geo_types::Coord { x: 1.0, y: 2.0 }, geo_types::Coord { x: 3.0, y: 4.0 }])] ))))]
	#[case::geometry::multipolygon(Value::Geometry(Geometry::MultiPolygon(geo_types::MultiPolygon(vec![geo_types::Polygon::new(
		geo_types::LineString(vec![geo_types::Coord { x: 0.0, y: 0.0 }, geo_types::Coord { x: 1.0, y: 1.0 }, geo_types::Coord { x: 0.0, y: 1.0 }]),
		vec![geo_types::LineString(vec![geo_types::Coord { x: 0.5, y: 0.5 }, geo_types::Coord { x: 0.75, y: 0.75 }])]
	)]))))]
	#[case::file(Value::File(File { bucket: "test_bucket".to_string(), key: "/test_key".to_string() }))]
	#[case::range(Value::Range(Box::new(Range { start: Bound::Included(Value::String("Hello, World!".to_string())), end: Bound::Excluded(Value::String("Hello, World!".to_string())) })))]
	#[case::range(Value::Range(Box::new(Range { start: Bound::Unbounded, end: Bound::Excluded(Value::String("Hello, World!".to_string())) })))]
	#[case::range(Value::Range(Box::new(Range { start: Bound::Included(Value::String("Hello, World!".to_string())), end: Bound::Unbounded })))]
	#[case::regex(Value::Regex(Regex::from_str("/^[a-z]+$/").unwrap()))]
	fn test_flatbuffers_roundtrip_value(#[case] input: Value) {
		let mut builder = ::flatbuffers::FlatBufferBuilder::new();
		let input_fb = input.to_fb(&mut builder).expect("Failed to convert to FlatBuffer");
		builder.finish_minimal(input_fb);
		let buf = builder.finished_data();
		let value_fb =
			::flatbuffers::root::<proto_fb::Value>(buf).expect("Failed to read FlatBuffer");
		let value = Value::from_fb(value_fb).expect("Failed to convert from FlatBuffer");
		assert_eq!(input, value, "Roundtrip conversion failed for input: {:?}", input);
	}

	#[test]
	fn test_decode_explicit_none_variant() {
		// A peer using the explicit `ValueType::None` union member (protocol
		// 0.11+) rather than the implicit unset sentinel this crate still
		// emits. Both must decode to `Value::None`.
		let mut builder = ::flatbuffers::FlatBufferBuilder::new();
		let none_value =
			proto_fb::NoneValue::create(&mut builder, &proto_fb::NoneValueArgs {}).as_union_value();
		let value_fb = proto_fb::Value::create(
			&mut builder,
			&proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::None,
				value: Some(none_value),
			},
		);
		builder.finish_minimal(value_fb);
		let buf = builder.finished_data();
		let value_fb =
			::flatbuffers::root::<proto_fb::Value>(buf).expect("Failed to read FlatBuffer");
		let value = Value::from_fb(value_fb).expect("Failed to convert from FlatBuffer");
		assert_eq!(value, Value::None);
	}

	#[test]
	fn test_decode_string_value_without_content_returns_err() {
		let mut builder = ::flatbuffers::FlatBufferBuilder::new();
		let string_value = proto_fb::StringValue::create(
			&mut builder,
			&proto_fb::StringValueArgs {
				value: None,
			},
		)
		.as_union_value();
		let value_fb = proto_fb::Value::create(
			&mut builder,
			&proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::String,
				value: Some(string_value),
			},
		);
		builder.finish_minimal(value_fb);
		let buf = builder.finished_data();
		let value_fb =
			::flatbuffers::root::<proto_fb::Value>(buf).expect("Failed to read FlatBuffer");
		assert!(Value::from_fb(value_fb).is_err());
	}

	#[test]
	fn test_decode_value_with_missing_union_payload_returns_err() {
		for value_type in [
			proto_fb::ValueType::Bool,
			proto_fb::ValueType::Int64,
			proto_fb::ValueType::Float64,
			proto_fb::ValueType::Decimal,
			proto_fb::ValueType::String,
			proto_fb::ValueType::Bytes,
			proto_fb::ValueType::Table,
			proto_fb::ValueType::RecordId,
			proto_fb::ValueType::Duration,
			proto_fb::ValueType::Datetime,
			proto_fb::ValueType::Uuid,
			proto_fb::ValueType::Object,
			proto_fb::ValueType::Array,
			proto_fb::ValueType::Set,
			proto_fb::ValueType::Geometry,
			proto_fb::ValueType::File,
			proto_fb::ValueType::Regex,
			proto_fb::ValueType::Range,
		] {
			let mut builder = ::flatbuffers::FlatBufferBuilder::new();
			let value_fb = proto_fb::Value::create(
				&mut builder,
				&proto_fb::ValueArgs {
					value_type,
					value: None,
				},
			);
			builder.finish_minimal(value_fb);
			let buf = builder.finished_data();
			assert!(crate::flatbuffers::decode::<Value>(buf).is_err());
			let value_fb = unsafe { ::flatbuffers::root_unchecked::<proto_fb::Value>(buf) };
			assert!(
				Value::from_fb(value_fb).is_err(),
				"expected missing union payload for {value_type:?} to return an error"
			);
		}
	}
}
