use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use crate::expr::{Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Strand, Thing, Uuid, Value};
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_protocol::fb::v1 as proto_fb;

impl ToFlatbuffers for Value {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let args = match self {
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
				crate::expr::Number::Int(i) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Int64,
					value: Some(i.to_fb(builder)?.as_union_value()),
				},
				crate::expr::Number::Float(f) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Float64,
					value: Some(f.to_fb(builder)?.as_union_value()),
				},
				crate::expr::Number::Decimal(d) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Decimal,
					value: Some(d.to_fb(builder)?.as_union_value()),
				},
			},
			Self::Strand(s) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::String,
				value: Some(s.to_fb(builder)?.as_union_value()),
			},
			Self::Bytes(b) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Bytes,
				value: Some(b.to_fb(builder)?.as_union_value()),
			},
			Self::Thing(thing) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::RecordId,
				value: Some(thing.to_fb(builder)?.as_union_value()),
			},
			Self::Duration(d) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Duration,
				value: Some(d.to_fb(builder)?.as_union_value()),
			},
			Self::Datetime(dt) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Datetime,
				value: Some(dt.to_fb(builder)?.as_union_value()),
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
			_ => {
				// TODO: DO NOT PANIC, we just need to modify the Value enum which Mees is currently working on.
				panic!("Unsupported value type for Flatbuffers serialization: {:?}", self);
			}
		};

		Ok(proto_fb::Value::create(builder, &args))
	}
}

impl FromFlatbuffers for Value {
	type Input<'a> = proto_fb::Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.value_type() {
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
				Ok(Value::Strand(Strand(value)))
			}
			proto_fb::ValueType::Bytes => {
				let bytes_value = input.value_as_bytes().expect("Guaranteed to be Bytes");
				Ok(Value::Bytes(Bytes::from_fb(bytes_value)?))
			}
			proto_fb::ValueType::RecordId => {
				let record_id_value =
					input.value_as_record_id().expect("Guaranteed to be a RecordId");
				let thing = Thing::from_fb(record_id_value)?;
				Ok(Value::Thing(thing))
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
			_ => Err(anyhow!(
				"Unsupported value type for Flatbuffers deserialization: {:?}",
				input.value_type()
			)),
		}
	}
}