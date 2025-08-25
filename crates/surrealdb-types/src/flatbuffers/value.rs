use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{
	Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId, Regex, Uuid,
	Value,
};

impl ToFlatbuffers for Value {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let args = match self {
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
			Self::Number(n) => match n {
				crate::Number::Int(i) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Int64,
					value: Some(i.to_fb(builder)?.as_union_value()),
				},
				crate::Number::Float(f) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Float64,
					value: Some(f.to_fb(builder)?.as_union_value()),
				},
				crate::Number::Decimal(d) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Decimal,
					value: Some(d.to_fb(builder)?.as_union_value()),
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
			Self::RecordId(thing) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::RecordId,
				value: Some(thing.to_fb(builder)?.as_union_value()),
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
			proto_fb::ValueType::NONE => Ok(Value::None),
			proto_fb::ValueType::Null => Ok(Value::Null),
			proto_fb::ValueType::Bool => {
				Ok(Value::Bool(input.value_as_bool().expect("Guaranteed to be a Bool").value()))
			}
			proto_fb::ValueType::Int64 => Ok(Value::Number(Number::Int(
				input.value_as_int_64().expect("Guaranteed to be an Int64").value(),
			))),
			proto_fb::ValueType::Float64 => Ok(Value::Number(Number::Float(
				input.value_as_float_64().expect("Guaranteed to be a Float64").value(),
			))),
			proto_fb::ValueType::Decimal => {
				let decimal_value = input.value_as_decimal().expect("Guaranteed to be a Decimal");
				Ok(Value::Number(Number::Decimal(Decimal::from_fb(decimal_value)?)))
			}
			proto_fb::ValueType::String => {
				let string_value = input.value_as_string().expect("Guaranteed to be a String");
				let value = string_value
					.value()
					.expect("String value is guaranteed to be present")
					.to_string();
				Ok(Value::String(value))
			}
			proto_fb::ValueType::Bytes => {
				let bytes_value = input.value_as_bytes().expect("Guaranteed to be Bytes");
				Ok(Value::Bytes(Bytes::from_fb(bytes_value)?))
			}
			proto_fb::ValueType::RecordId => {
				let record_id_value =
					input.value_as_record_id().expect("Guaranteed to be a RecordId");
				let thing = RecordId::from_fb(record_id_value)?;
				Ok(Value::RecordId(thing))
			}
			proto_fb::ValueType::Duration => {
				let duration_value =
					input.value_as_duration().expect("Guaranteed to be a Duration");
				let duration = Duration::from_fb(duration_value)?;
				Ok(Value::Duration(duration))
			}
			proto_fb::ValueType::Datetime => {
				let datetime_value =
					input.value_as_datetime().expect("Guaranteed to be a Datetime");
				let dt = DateTime::<Utc>::from_fb(datetime_value)?;
				Ok(Value::Datetime(Datetime(dt)))
			}
			proto_fb::ValueType::Uuid => {
				let uuid_value = input.value_as_uuid().expect("Guaranteed to be a Uuid");
				let uuid = Uuid::from_fb(uuid_value)?;
				Ok(Value::Uuid(uuid))
			}
			proto_fb::ValueType::Object => {
				let object_value = input.value_as_object().expect("Guaranteed to be an Object");
				let object = Object::from_fb(object_value)?;
				Ok(Value::Object(object))
			}
			proto_fb::ValueType::Array => {
				let array_value = input.value_as_array().expect("Guaranteed to be an Array");
				let array = Array::from_fb(array_value)?;
				Ok(Value::Array(array))
			}
			proto_fb::ValueType::Geometry => {
				let geometry_value =
					input.value_as_geometry().expect("Guaranteed to be a Geometry");
				let geometry = Geometry::from_fb(geometry_value)?;
				Ok(Value::Geometry(geometry))
			}
			proto_fb::ValueType::File => {
				let file_value = input.value_as_file().expect("Guaranteed to be a File");
				let file = File::from_fb(file_value)?;
				Ok(Value::File(file))
			}
			proto_fb::ValueType::Regex => {
				let regex_value = input.value_as_regex().expect("Guaranteed to be a Regex");
				let regex = Regex::from_fb(regex_value)?;
				Ok(Value::Regex(regex))
			}
			proto_fb::ValueType::Range => {
				let range_value = input.value_as_range().expect("Guaranteed to be a Range");
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
	#[case::bytes(Value::Bytes(Bytes(vec![1, 2, 3, 4, 5])))]
	#[case::thing(Value::RecordId(RecordId{ table: "test_table".to_string(), key: RecordIdKey::Number(42) }))] // Example Thing
	#[case::thing_range(Value::RecordId(RecordId{ table: "test_table".to_string(), key: RecordIdKey::Range(Box::new(RecordIdKeyRange { start: Bound::Included(RecordIdKey::String("a".to_string())), end: Bound::Unbounded })) }))]
	#[case::object(Value::Object(Object(BTreeMap::from([("key".to_string(), Value::String("value".to_owned()))]))))]
	#[case::array(Value::Array(Array::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
	#[case::geometry::point(Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))))]
	#[case::geometry::line(Value::Geometry(Geometry::Line(geo::LineString(vec![geo::Coord { x: 1.0, y: 2.0 }, geo::Coord { x: 3.0, y: 4.0 }]))))]
	#[case::geometry::polygon(Value::Geometry(Geometry::Polygon(geo::Polygon::new(
		geo::LineString(vec![geo::Coord { x: 0.0, y: 0.0 }, geo::Coord { x: 1.0, y: 1.0 }, geo::Coord { x: 0.0, y: 1.0 }]),
		vec![geo::LineString(vec![geo::Coord { x: 0.5, y: 0.5 }, geo::Coord { x: 0.75, y: 0.75 }])]
	))))]
	#[case::geometry::multipoint(Value::Geometry(Geometry::MultiPoint(geo::MultiPoint(vec![geo::Point::new(1.0, 2.0), geo::Point::new(3.0, 4.0)]))))]
	#[case::geometry::multiline(Value::Geometry(Geometry::MultiLine(geo::MultiLineString(vec![geo::LineString(vec![geo::Coord { x: 1.0, y: 2.0 }, geo::Coord { x: 3.0, y: 4.0 }])] ))))]
	#[case::geometry::multipolygon(Value::Geometry(Geometry::MultiPolygon(geo::MultiPolygon(vec![geo::Polygon::new(
		geo::LineString(vec![geo::Coord { x: 0.0, y: 0.0 }, geo::Coord { x: 1.0, y: 1.0 }, geo::Coord { x: 0.0, y: 1.0 }]),
		vec![geo::LineString(vec![geo::Coord { x: 0.5, y: 0.5 }, geo::Coord { x: 0.75, y: 0.75 }])]
	)]))))]
	#[case::file(Value::File(File { bucket: "test_bucket".to_string(), key: "test_key".to_string() }))]
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
}
