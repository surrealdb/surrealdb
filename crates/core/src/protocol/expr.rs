use crate::dbs::Variables;
use crate::expr::graph::{GraphSubject, GraphSubjects};
use crate::expr::order::{OrderList, Ordering};
use crate::expr::part::{DestructurePart, Recurse, RecurseInstruction};
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use surrealdb_protocol::proto::prost_types;

use crate::expr::{
	Array, Cond, Data, Datetime, Dir, Duration, Fetch, Fetchs, Field, Fields, File, Geometry,
	Graph, Group, Groups, Id, IdRange, Ident, Idiom, Limit, Number, Object, Operator, Order, Part,
	Split, Splits, Start, Strand, Table, Thing, Timeout, Uuid, Value, idiom,
};
use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use core::panic;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::ops::Bound;
use surrealdb_protocol::proto::v1 as proto;

use surrealdb_protocol::fb::v1 as proto_fb;

impl ToFlatbuffers for Value {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
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
				value: Some(
					proto_fb::BoolValue::create(
						builder,
						&proto_fb::BoolValueArgs {
							value: *b,
						},
					)
					.as_union_value(),
				),
			},
			Self::Number(n) => match n {
				crate::expr::Number::Int(i) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Int64,
					value: Some(
						proto_fb::Int64Value::create(
							builder,
							&proto_fb::Int64ValueArgs {
								value: *i,
							},
						)
						.as_union_value(),
					),
				},
				crate::expr::Number::Float(f) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Float64,
					value: Some(
						proto_fb::Float64Value::create(
							builder,
							&proto_fb::Float64ValueArgs {
								value: *f,
							},
						)
						.as_union_value(),
					),
				},
				crate::expr::Number::Decimal(d) => proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Decimal,
					value: Some(d.to_fb(builder).as_union_value()),
				},
			},
			Self::Strand(s) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::String,
				value: Some(s.to_fb(builder).as_union_value()),
			},
			Self::Bytes(b) => {
				let bytes = builder.create_vector(b.as_slice());
				proto_fb::ValueArgs {
					value_type: proto_fb::ValueType::Bytes,
					value: Some(
						proto_fb::Bytes::create(
							builder,
							&proto_fb::BytesArgs {
								value: Some(bytes),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Thing(thing) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::RecordId,
				value: Some(thing.to_fb(builder).as_union_value()),
			},
			Self::Duration(d) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Duration,
				value: Some(d.to_fb(builder).as_union_value()),
			},
			Self::Datetime(dt) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Datetime,
				value: Some(dt.to_fb(builder).as_union_value()),
			},
			Self::Uuid(uuid) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Uuid,
				value: Some(uuid.to_fb(builder).as_union_value()),
			},
			Self::Object(obj) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Object,
				value: Some(obj.to_fb(builder).as_union_value()),
			},
			Self::Array(arr) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Array,
				value: Some(arr.to_fb(builder).as_union_value()),
			},
			Self::Geometry(geometry) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::Geometry,
				value: Some(geometry.to_fb(builder).as_union_value()),
			},
			Self::File(file) => proto_fb::ValueArgs {
				value_type: proto_fb::ValueType::File,
				value: Some(file.to_fb(builder).as_union_value()),
			},
			_ => {
				// TODO: DO NOT PANIC, we just need to modify the Value enum which Mees is currently working on.
				panic!("Unsupported value type for Flatbuffers serialization: {:?}", self);
			}
		};

		proto_fb::Value::create(builder, &args)
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
				let decimal = decimal_value
					.value()
					.expect("Decimal value is guaranteed to be present")
					.parse::<Decimal>()
					.map_err(|_| anyhow!("Invalid decimal format"))?;
				Ok(Value::Number(Number::Decimal(decimal)))
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
				let value = Vec::<u8>::from_fb(
					bytes_value.value().expect("Bytes value is guaranteed to be present"),
				)?;
				Ok(Value::Bytes(crate::expr::Bytes(value)))
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

impl ToFlatbuffers for i64 {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Int64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		proto_fb::Int64Value::create(
			builder,
			&proto_fb::Int64ValueArgs {
				value: *self,
			},
		)
	}
}

impl FromFlatbuffers for i64 {
	type Input<'a> = proto_fb::Int64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for f64 {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Float64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		proto_fb::Float64Value::create(
			builder,
			&proto_fb::Float64ValueArgs {
				value: *self,
			},
		)
	}
}

impl FromFlatbuffers for f64 {
	type Input<'a> = proto_fb::Float64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for String {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::StringValue<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = builder.create_string(self);
		proto_fb::StringValue::create(
			builder,
			&proto_fb::StringValueArgs {
				value: Some(value),
			},
		)
	}
}

impl ToFlatbuffers for Decimal {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Decimal<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = builder.create_string(&self.to_string());
		proto_fb::Decimal::create(
			builder,
			&proto_fb::DecimalArgs {
				value: Some(value),
			},
		)
	}
}

impl ToFlatbuffers for std::time::Duration {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Duration<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		proto_fb::Duration::create(
			builder,
			&proto_fb::DurationArgs {
				seconds: self.as_secs(),
				nanos: self.subsec_nanos(),
			},
		)
	}
}

impl FromFlatbuffers for std::time::Duration {
	type Input<'a> = proto_fb::Duration<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let seconds = input.seconds();
		let nanos = input.nanos();
		Ok(std::time::Duration::new(seconds, nanos))
	}
}

impl ToFlatbuffers for Duration {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Duration<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Duration {
	type Input<'a> = proto_fb::Duration<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let duration = std::time::Duration::from_fb(input)?;
		Ok(Duration(duration))
	}
}

impl ToFlatbuffers for DateTime<Utc> {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Timestamp<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		proto_fb::Timestamp::create(
			builder,
			&proto_fb::TimestampArgs {
				seconds: self.timestamp(),
				nanos: self.timestamp_subsec_nanos(),
			},
		)
	}
}

impl FromFlatbuffers for DateTime<Utc> {
	type Input<'a> = proto_fb::Timestamp<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let seconds = input.seconds();
		let nanos = input.nanos();
		DateTime::<Utc>::from_timestamp(seconds, nanos)
			.ok_or_else(|| anyhow::anyhow!("Invalid timestamp format"))
	}
}

impl ToFlatbuffers for Uuid {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Uuid<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let bytes = builder.create_vector(self.as_bytes());
		proto_fb::Uuid::create(
			builder,
			&proto_fb::UuidArgs {
				bytes: Some(bytes),
			},
		)
	}
}

impl FromFlatbuffers for Uuid {
	type Input<'a> = proto_fb::Uuid<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let bytes_vector = input.bytes().ok_or_else(|| anyhow::anyhow!("Missing bytes in Uuid"))?;
		Uuid::from_slice(bytes_vector.bytes()).map_err(|_| anyhow::anyhow!("Invalid UUID format"))
	}
}

impl ToFlatbuffers for Thing {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordId<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let table = builder.create_string(&self.tb);
		let id = self.id.to_fb(builder);
		proto_fb::RecordId::create(
			builder,
			&proto_fb::RecordIdArgs {
				table: Some(table),
				id: Some(id),
			},
		)
	}
}

impl FromFlatbuffers for Thing {
	type Input<'a> = proto_fb::RecordId<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let table = input.table().ok_or_else(|| anyhow::anyhow!("Missing table in RecordId"))?;
		let id = Id::from_fb(input.id().ok_or_else(|| anyhow::anyhow!("Missing id in RecordId"))?)?;
		Ok(Thing {
			tb: table.to_string(),
			id,
		})
	}
}

impl FromFlatbuffers for Vec<u8> {
	type Input<'a> = flatbuffers::Vector<'a, u8>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.bytes().to_vec())
	}
}

impl ToFlatbuffers for Id {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Id<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Id::Number(n) => {
				let id = n.to_fb(builder).as_union_value();
				proto_fb::Id::create(
					builder,
					&proto_fb::IdArgs {
						id_type: proto_fb::IdType::Int64,
						id: Some(id),
					},
				)
			}
			Id::String(s) => {
				let id = s.to_fb(builder).as_union_value();
				proto_fb::Id::create(
					builder,
					&proto_fb::IdArgs {
						id_type: proto_fb::IdType::String,
						id: Some(id),
					},
				)
			}
			Id::Uuid(uuid) => {
				let id = uuid.to_fb(builder).as_union_value();
				proto_fb::Id::create(
					builder,
					&proto_fb::IdArgs {
						id_type: proto_fb::IdType::Uuid,
						id: Some(id),
					},
				)
			}
			Id::Array(arr) => {
				let id = arr.to_fb(builder).as_union_value();
				proto_fb::Id::create(
					builder,
					&proto_fb::IdArgs {
						id_type: proto_fb::IdType::Array,
						id: Some(id),
					},
				)
			}
			_ => panic!("Unsupported Id type for FlatBuffers serialization: {:?}", self),
		}
	}
}

impl FromFlatbuffers for Id {
	type Input<'a> = proto_fb::Id<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.id_type() {
			proto_fb::IdType::Int64 => {
				let id_value =
					input.id_as_int_64().ok_or_else(|| anyhow::anyhow!("Expected Int64 Id"))?;
				Ok(Id::Number(id_value.value()))
			}
			proto_fb::IdType::String => {
				let id_value =
					input.id_as_string().ok_or_else(|| anyhow::anyhow!("Expected String Id"))?;
				Ok(Id::String(
					id_value
						.value()
						.ok_or_else(|| anyhow::anyhow!("Missing String value"))?
						.to_string(),
				))
			}
			proto_fb::IdType::Uuid => {
				let id_value =
					input.id_as_uuid().ok_or_else(|| anyhow::anyhow!("Expected Uuid Id"))?;
				let uuid = Uuid::from_fb(id_value)?;
				Ok(Id::Uuid(uuid))
			}
			proto_fb::IdType::Array => {
				let id_value =
					input.id_as_array().ok_or_else(|| anyhow::anyhow!("Expected Array Id"))?;
				let array = Array::from_fb(id_value)?;
				Ok(Id::Array(array))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported Id type for FlatBuffers deserialization: {:?}",
				input.id_type()
			)),
		}
	}
}

impl ToFlatbuffers for File {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::File<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let bucket = builder.create_string(&self.bucket);
		let key = builder.create_string(&self.key);
		proto_fb::File::create(
			builder,
			&proto_fb::FileArgs {
				bucket: Some(bucket),
				key: Some(key),
			},
		)
	}
}

impl FromFlatbuffers for File {
	type Input<'a> = proto_fb::File<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let bucket = input.bucket().ok_or_else(|| anyhow::anyhow!("Missing bucket in File"))?;
		let key = input.key().ok_or_else(|| anyhow::anyhow!("Missing key in File"))?;
		Ok(File {
			bucket: bucket.to_string(),
			key: key.to_string(),
		})
	}
}

impl ToFlatbuffers for Object {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Object<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut entries = Vec::with_capacity(self.0.len());
		for (key, value) in &self.0 {
			let key_fb = builder.create_string(key);
			let value_fb = value.to_fb(builder);

			let object_item = proto_fb::KeyValue::create(
				builder,
				&proto_fb::KeyValueArgs {
					key: Some(key_fb),
					value: Some(value_fb),
				},
			);

			entries.push(object_item);
		}
		let entries_vector = builder.create_vector(&entries);
		proto_fb::Object::create(
			builder,
			&proto_fb::ObjectArgs {
				items: Some(entries_vector),
			},
		)
	}
}

impl FromFlatbuffers for Object {
	type Input<'a> = proto_fb::Object<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut map = BTreeMap::new();
		let items = input.items().ok_or_else(|| anyhow::anyhow!("Missing items in Object"))?;
		if items.is_empty() {
			return Ok(Object(map));
		}
		for entry in items {
			let key = entry.key().context("Missing key in Object entry")?.to_string();
			let value = entry.value().context("Missing value in Object entry")?;
			map.insert(key, Value::from_fb(value)?);
		}
		Ok(Object(map))
	}
}

impl ToFlatbuffers for Array {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Array<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut values = Vec::with_capacity(self.0.len());
		for value in &self.0 {
			values.push(value.to_fb(builder));
		}
		let values_vector = builder.create_vector(&values);
		proto_fb::Array::create(
			builder,
			&proto_fb::ArrayArgs {
				values: Some(values_vector),
			},
		)
	}
}

impl FromFlatbuffers for Array {
	type Input<'a> = proto_fb::Array<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut vec = Vec::new();
		let values = input.values().context("Values is not set")?;
		for value in values {
			vec.push(Value::from_fb(value)?);
		}
		Ok(Array(vec))
	}
}

impl ToFlatbuffers for Geometry {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Geometry<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Geometry::Point(point) => {
				let geometry = point.to_fb(builder);
				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::Point,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::Line(line_string) => {
				let geometry = line_string.to_fb(builder);
				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::LineString,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::Polygon(polygon) => {
				let geometry = polygon.to_fb(builder);
				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::Polygon,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::MultiPoint(multi_point) => {
				let geometry = multi_point.to_fb(builder);
				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::MultiPoint,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::MultiLine(multi_line_string) => {
				let geometry = multi_line_string.to_fb(builder);
				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::MultiLineString,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::MultiPolygon(multi_polygon) => {
				let geometry = multi_polygon.to_fb(builder);
				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::MultiPolygon,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::Collection(geometries) => {
				let mut geometries_vec = Vec::with_capacity(geometries.len());
				for geometry in geometries {
					geometries_vec.push(geometry.to_fb(builder));
				}
				let geometries_vector = builder.create_vector(&geometries_vec);

				let collection = proto_fb::GeometryCollection::create(
					builder,
					&proto_fb::GeometryCollectionArgs {
						geometries: Some(geometries_vector),
					},
				);

				proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::Collection,
						geometry: Some(collection.as_union_value()),
					},
				)
			}
		}
	}
}

impl FromFlatbuffers for Geometry {
	type Input<'a> = proto_fb::Geometry<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.geometry_type() {
			proto_fb::GeometryType::Point => {
				let point = input
					.geometry_as_point()
					.ok_or_else(|| anyhow::anyhow!("Expected Point geometry"))?;
				Ok(Geometry::Point(geo::Point::from_fb(point)?))
			}
			proto_fb::GeometryType::LineString => {
				let line_string = input
					.geometry_as_line_string()
					.ok_or_else(|| anyhow::anyhow!("Expected LineString geometry"))?;
				Ok(Geometry::Line(geo::LineString::from_fb(line_string)?))
			}
			proto_fb::GeometryType::Polygon => {
				let polygon = input
					.geometry_as_polygon()
					.ok_or_else(|| anyhow::anyhow!("Expected Polygon geometry"))?;
				Ok(Geometry::Polygon(geo::Polygon::from_fb(polygon)?))
			}
			proto_fb::GeometryType::MultiPoint => {
				let multi_point = input
					.geometry_as_multi_point()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiPoint geometry"))?;
				Ok(Geometry::MultiPoint(geo::MultiPoint::from_fb(multi_point)?))
			}
			proto_fb::GeometryType::MultiLineString => {
				let multi_line_string = input
					.geometry_as_multi_line_string()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiLineString geometry"))?;
				Ok(Geometry::MultiLine(geo::MultiLineString::from_fb(multi_line_string)?))
			}
			proto_fb::GeometryType::MultiPolygon => {
				let multi_polygon = input
					.geometry_as_multi_polygon()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiPolygon geometry"))?;
				Ok(Geometry::MultiPolygon(geo::MultiPolygon::from_fb(multi_polygon)?))
			}
			proto_fb::GeometryType::Collection => {
				let collection = input
					.geometry_as_collection()
					.ok_or_else(|| anyhow::anyhow!("Expected GeometryCollection"))?;
				let geometries_reader = collection.geometries().context("Geometries is not set")?;
				let mut geometries = Vec::with_capacity(geometries_reader.len());
				for geometry in geometries_reader {
					geometries.push(Geometry::from_fb(geometry)?);
				}
				Ok(Geometry::Collection(geometries))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported geometry type for FlatBuffers deserialization: {:?}",
				input.geometry_type()
			)),
		}
	}
}

impl ToFlatbuffers for geo::Point {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Point<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		proto_fb::Point::create(
			builder,
			&proto_fb::PointArgs {
				x: self.x(),
				y: self.y(),
			},
		)
	}
}

impl FromFlatbuffers for geo::Point {
	type Input<'a> = proto_fb::Point<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(geo::Point::new(input.x(), input.y()))
	}
}

impl ToFlatbuffers for geo::Coord {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Point<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		proto_fb::Point::create(
			builder,
			&proto_fb::PointArgs {
				x: self.x,
				y: self.y,
			},
		)
	}
}

impl FromFlatbuffers for geo::Coord {
	type Input<'a> = proto_fb::Point<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(geo::Coord {
			x: input.x(),
			y: input.y(),
		})
	}
}

impl ToFlatbuffers for geo::LineString {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::LineString<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut points = Vec::with_capacity(self.0.len());
		for point in &self.0 {
			points.push(point.to_fb(builder));
		}
		let points_vector = builder.create_vector(&points);
		proto_fb::LineString::create(
			builder,
			&proto_fb::LineStringArgs {
				points: Some(points_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::LineString {
	type Input<'a> = proto_fb::LineString<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut points = Vec::new();
		for point in input.points().context("Points is not set")? {
			points.push(geo::Coord::from_fb(point)?);
		}
		Ok(Self(points))
	}
}

impl ToFlatbuffers for geo::Polygon {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Polygon<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let exterior = self.exterior().to_fb(builder);
		let mut interiors = Vec::with_capacity(self.interiors().len());
		for interior in self.interiors() {
			interiors.push(interior.to_fb(builder));
		}
		let interiors_vector = builder.create_vector(&interiors);
		proto_fb::Polygon::create(
			builder,
			&proto_fb::PolygonArgs {
				exterior: Some(exterior),
				interiors: Some(interiors_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::Polygon {
	type Input<'a> = proto_fb::Polygon<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let exterior =
			input.exterior().ok_or_else(|| anyhow::anyhow!("Missing exterior in Polygon"))?;
		let exterior = geo::LineString::from_fb(exterior)?;

		let mut interiors = Vec::new();
		if let Some(interiors_reader) = input.interiors() {
			for interior in interiors_reader {
				interiors.push(geo::LineString::from_fb(interior)?);
			}
		}

		Ok(Self::new(exterior, interiors))
	}
}

impl ToFlatbuffers for geo::MultiPoint {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::MultiPoint<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut points = Vec::with_capacity(self.0.len());
		for point in &self.0 {
			points.push(point.to_fb(builder));
		}
		let points_vector = builder.create_vector(&points);
		proto_fb::MultiPoint::create(
			builder,
			&proto_fb::MultiPointArgs {
				points: Some(points_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::MultiPoint {
	type Input<'a> = proto_fb::MultiPoint<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut points = Vec::new();
		for point in input.points().context("Points is not set")? {
			points.push(geo::Point::from_fb(point)?);
		}
		Ok(Self(points))
	}
}

impl ToFlatbuffers for geo::MultiLineString {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::MultiLineString<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut lines = Vec::with_capacity(self.0.len());
		for line in &self.0 {
			lines.push(line.to_fb(builder));
		}
		let lines_vector = builder.create_vector(&lines);
		proto_fb::MultiLineString::create(
			builder,
			&proto_fb::MultiLineStringArgs {
				lines: Some(lines_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::MultiLineString {
	type Input<'a> = proto_fb::MultiLineString<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut lines = Vec::new();
		for line in input.lines().context("Lines is not set")? {
			lines.push(geo::LineString::from_fb(line)?);
		}
		Ok(Self(lines))
	}
}

impl ToFlatbuffers for geo::MultiPolygon {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::MultiPolygon<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut polygons = Vec::with_capacity(self.0.len());
		for polygon in &self.0 {
			polygons.push(polygon.to_fb(builder));
		}
		let polygons_vector = builder.create_vector(&polygons);
		proto_fb::MultiPolygon::create(
			builder,
			&proto_fb::MultiPolygonArgs {
				polygons: Some(polygons_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::MultiPolygon {
	type Input<'a> = proto_fb::MultiPolygon<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut polygons = Vec::new();
		for polygon in input.polygons().context("Polygons is not set")? {
			polygons.push(geo::Polygon::from_fb(polygon)?);
		}
		Ok(Self(polygons))
	}
}

impl ToFlatbuffers for Idiom {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Idiom<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut parts = Vec::with_capacity(self.0.len());
		for part in &self.0 {
			parts.push(part.to_fb(builder));
		}
		let parts_vector = builder.create_vector(&parts);
		proto_fb::Idiom::create(
			builder,
			&proto_fb::IdiomArgs {
				parts: Some(parts_vector),
			},
		)
	}
}

impl FromFlatbuffers for Idiom {
	type Input<'a> = proto_fb::Idiom<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut parts = Vec::new();
		let parts_reader = input.parts().context("Parts is not set")?;
		for part in parts_reader {
			parts.push(Part::from_fb(part)?);
		}
		Ok(Idiom(parts))
	}
}

impl ToFlatbuffers for Part {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Part<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::All => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::All,
					part: Some(null.as_union_value()),
				}
			}
			Self::Flatten => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Flatten,
					part: Some(null.as_union_value()),
				}
			}
			Self::Last => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Last,
					part: Some(null.as_union_value()),
				}
			}
			Self::First => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::First,
					part: Some(null.as_union_value()),
				}
			}
			Self::Field(ident) => {
				let ident = ident.to_fb(builder);
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Field,
					part: Some(ident.as_union_value()),
				}
			}
			Self::Index(index) => {
				let index: i64 = index.as_int();
				let index_value = index.to_fb(builder);
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Index,
					part: Some(index_value.as_union_value()),
				}
			}
			Self::Where(value) => {
				let value_fb = value.to_fb(builder).as_union_value();
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Where,
					part: Some(value_fb),
				}
			}
			Self::Graph(graph) => {
				let graph_fb = graph.to_fb(builder).as_union_value();
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Graph,
					part: Some(graph_fb),
				}
			}
			Self::Value(value) => {
				let value_fb = value.to_fb(builder).as_union_value();
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Value,
					part: Some(value_fb),
				}
			}
			Self::Start(value) => {
				let value_fb = value.to_fb(builder).as_union_value();
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Start,
					part: Some(value_fb),
				}
			}
			Self::Method(name, args) => {
				let name = builder.create_string(name);
				let mut args_vec = Vec::with_capacity(args.len());
				for arg in args {
					args_vec.push(arg.to_fb(builder));
				}
				let args = builder.create_vector(&args_vec);

				let method = proto_fb::MethodPart::create(
					builder,
					&proto_fb::MethodPartArgs {
						name: Some(name),
						args: Some(args),
					},
				);

				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Method,
					part: Some(method.as_union_value()),
				}
			}
			Self::Destructure(parts) => {
				let mut parts_vec = Vec::with_capacity(parts.len());
				for part in parts {
					parts_vec.push(part.to_fb(builder));
				}
				let parts = builder.create_vector(&parts_vec);

				let part = proto_fb::DestructureParts::create(
					builder,
					&proto_fb::DestructurePartsArgs {
						parts: Some(parts),
					},
				);

				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Destructure,
					part: Some(part.as_union_value()),
				}
			}
			Self::Optional => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Optional,
					part: Some(null.as_union_value()),
				}
			}
			Self::Recurse(recurse, idiom, instruction) => {
				let spec = recurse.to_fb(builder);
				let idiom = idiom.as_ref().map(|i| i.to_fb(builder));
				let recurse_operation = instruction.as_ref().map(|op| op.to_fb(builder));

				let recurse_fb = proto_fb::RecursePart::create(
					builder,
					&proto_fb::RecursePartArgs {
						spec: Some(spec),
						idiom,
						recurse_operation,
					},
				);

				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Recurse,
					part: Some(recurse_fb.as_union_value()),
				}
			}
			Self::Doc => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::Doc,
					part: Some(null.as_union_value()),
				}
			}
			Self::RepeatRecurse => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::PartArgs {
					part_type: proto_fb::PartType::RepeatRecurse,
					part: Some(null.as_union_value()),
				}
			}
		};

		proto_fb::Part::create(builder, &args)
	}
}

impl FromFlatbuffers for Part {
	type Input<'a> = proto_fb::Part<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.part_type() {
			proto_fb::PartType::All => Ok(Self::All),
			proto_fb::PartType::Flatten => Ok(Self::Flatten),
			proto_fb::PartType::Last => Ok(Self::Last),
			proto_fb::PartType::First => Ok(Self::First),
			proto_fb::PartType::Field => {
				let ident =
					input.part_as_field().ok_or_else(|| anyhow::anyhow!("Expected Field part"))?;
				let ident =
					ident.value().ok_or_else(|| anyhow::anyhow!("Missing value in Field part"))?;
				Ok(Self::Field(Ident(ident.to_string())))
			}
			proto_fb::PartType::Index => {
				let index =
					input.part_as_index().ok_or_else(|| anyhow::anyhow!("Expected Index part"))?;
				let index = index.value();
				Ok(Self::Index(Number::Int(index)))
			}
			proto_fb::PartType::Where => {
				let value =
					input.part_as_where().ok_or_else(|| anyhow::anyhow!("Expected Where part"))?;
				Ok(Self::Where(Value::from_fb(value)?))
			}
			proto_fb::PartType::Graph => {
				let graph =
					input.part_as_graph().ok_or_else(|| anyhow::anyhow!("Expected Graph part"))?;
				Ok(Self::Graph(Graph::from_fb(graph)?))
			}
			proto_fb::PartType::Value => {
				let value =
					input.part_as_value().ok_or_else(|| anyhow::anyhow!("Expected Value part"))?;
				Ok(Self::Value(Value::from_fb(value)?))
			}
			proto_fb::PartType::Start => {
				let value =
					input.part_as_start().ok_or_else(|| anyhow::anyhow!("Expected Start part"))?;
				Ok(Self::Start(Value::from_fb(value)?))
			}
			proto_fb::PartType::Method => {
				let method_part = input
					.part_as_method()
					.ok_or_else(|| anyhow::anyhow!("Expected Method part"))?;
				let name = method_part.name().context("Missing name in Method part")?.to_string();
				let args_reader = method_part.args().context("Missing args in Method part")?;
				let mut args = Vec::new();
				for arg in args_reader {
					args.push(Value::from_fb(arg)?);
				}
				Ok(Self::Method(name, args))
			}
			proto_fb::PartType::Destructure => {
				let destructure_parts = input
					.part_as_destructure()
					.ok_or_else(|| anyhow::anyhow!("Expected Destructure part"))?;
				let parts_reader =
					destructure_parts.parts().context("Missing parts in Destructure part")?;
				let mut parts = Vec::<DestructurePart>::new();
				for part in parts_reader {
					parts.push(DestructurePart::from_fb(part)?);
				}
				Ok(Self::Destructure(parts))
			}
			proto_fb::PartType::Optional => Ok(Self::Optional),
			proto_fb::PartType::Recurse => {
				let recurse_part = input
					.part_as_recurse()
					.ok_or_else(|| anyhow::anyhow!("Expected Recurse part"))?;
				let spec = recurse_part
					.spec()
					.ok_or_else(|| anyhow::anyhow!("Missing spec in Recurse part"))?;
				let recurse = Recurse::from_fb(spec)?;
				let idiom = recurse_part.idiom().map(Idiom::from_fb).transpose()?;
				let instruction = recurse_part
					.recurse_operation()
					.map(RecurseInstruction::from_fb)
					.transpose()?;
				Ok(Self::Recurse(recurse, idiom, instruction))
			}
			proto_fb::PartType::Doc => Ok(Self::Doc),
			proto_fb::PartType::RepeatRecurse => Ok(Self::RepeatRecurse),
			_ => Err(anyhow::anyhow!(
				"Unsupported Part type for FlatBuffers deserialization: {:?}",
				input.part_type()
			)),
		}
	}
}

impl ToFlatbuffers for Ident {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Ident<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = builder.create_string(&self.0);
		proto_fb::Ident::create(
			builder,
			&proto_fb::IdentArgs {
				value: Some(value),
			},
		)
	}
}

impl FromFlatbuffers for Ident {
	type Input<'a> = proto_fb::Ident<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let value = input.value().ok_or_else(|| anyhow::anyhow!("Missing value in Ident"))?;
		Ok(Ident(value.to_string()))
	}
}

impl ToFlatbuffers for Recurse {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecurseSpec<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Fixed(fixed) => {
				let fixed_value = proto_fb::FixedSpec::create(
					builder,
					&proto_fb::FixedSpecArgs {
						value: *fixed,
					},
				);

				proto_fb::RecurseSpecArgs {
					spec_type: proto_fb::RecurseSpecType::Fixed,
					spec: Some(fixed_value.as_union_value()),
				}
			}
			Self::Range(start, end) => {
				let range_value = proto_fb::RangeSpec::create(
					builder,
					&proto_fb::RangeSpecArgs {
						start: *start,
						end: *end,
					},
				);

				proto_fb::RecurseSpecArgs {
					spec_type: proto_fb::RecurseSpecType::Range,
					spec: Some(range_value.as_union_value()),
				}
			}
		};

		proto_fb::RecurseSpec::create(builder, &args)
	}
}

impl FromFlatbuffers for Recurse {
	type Input<'a> = proto_fb::RecurseSpec<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.spec_type() {
			proto_fb::RecurseSpecType::Fixed => {
				let fixed =
					input.spec_as_fixed().ok_or_else(|| anyhow::anyhow!("Expected Fixed spec"))?;
				Ok(Self::Fixed(fixed.value()))
			}
			proto_fb::RecurseSpecType::Range => {
				let range =
					input.spec_as_range().ok_or_else(|| anyhow::anyhow!("Expected Range spec"))?;
				Ok(Self::Range(range.start(), range.end()))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported Recurse spec type for FlatBuffers deserialization: {:?}",
				input.spec_type()
			)),
		}
	}
}

impl ToFlatbuffers for RecurseInstruction {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecurseOperation<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Path {
				inclusive,
			} => {
				let operation = proto_fb::RecursePath::create(
					builder,
					&proto_fb::RecursePathArgs {
						inclusive: *inclusive,
					},
				);

				proto_fb::RecurseOperationArgs {
					operation_type: proto_fb::RecurseOperationType::Path,
					operation: Some(operation.as_union_value()),
				}
			}
			Self::Collect {
				inclusive,
			} => {
				let operation = proto_fb::RecurseCollect::create(
					builder,
					&proto_fb::RecurseCollectArgs {
						inclusive: *inclusive,
					},
				);

				proto_fb::RecurseOperationArgs {
					operation_type: proto_fb::RecurseOperationType::Collect,
					operation: Some(operation.as_union_value()),
				}
			}
			Self::Shortest {
				expects,
				inclusive,
			} => {
				let expects_value = expects.to_fb(builder);
				let operation = proto_fb::RecurseShortest::create(
					builder,
					&proto_fb::RecurseShortestArgs {
						expects: Some(expects_value),
						inclusive: *inclusive,
					},
				);

				proto_fb::RecurseOperationArgs {
					operation_type: proto_fb::RecurseOperationType::Shortest,
					operation: Some(operation.as_union_value()),
				}
			}
		};

		proto_fb::RecurseOperation::create(builder, &args)
	}
}

impl FromFlatbuffers for RecurseInstruction {
	type Input<'a> = proto_fb::RecurseOperation<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.operation_type() {
			proto_fb::RecurseOperationType::Path => {
				let path = input
					.operation_as_path()
					.ok_or_else(|| anyhow::anyhow!("Expected Path operation"))?;
				Ok(Self::Path {
					inclusive: path.inclusive(),
				})
			}
			proto_fb::RecurseOperationType::Collect => {
				let collect = input
					.operation_as_collect()
					.ok_or_else(|| anyhow::anyhow!("Expected Collect operation"))?;
				Ok(Self::Collect {
					inclusive: collect.inclusive(),
				})
			}
			proto_fb::RecurseOperationType::Shortest => {
				let shortest = input
					.operation_as_shortest()
					.ok_or_else(|| anyhow::anyhow!("Expected Shortest operation"))?;
				let expects = Value::from_fb(
					shortest.expects().context("Missing expects in Shortest operation")?,
				)?;
				Ok(Self::Shortest {
					expects,
					inclusive: shortest.inclusive(),
				})
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported RecurseOperation type for FlatBuffers deserialization: {:?}",
				input.operation_type()
			)),
		}
	}
}

impl ToFlatbuffers for DestructurePart {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::DestructurePart<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::All(ident) => {
				let ident = ident.to_fb(builder);
				proto_fb::DestructurePartArgs {
					part_type: proto_fb::DestructurePartType::All,
					part: Some(ident.as_union_value()),
				}
			}
			Self::Field(ident) => {
				let ident = ident.to_fb(builder);
				proto_fb::DestructurePartArgs {
					part_type: proto_fb::DestructurePartType::Field,
					part: Some(ident.as_union_value()),
				}
			}
			Self::Aliased(ident, idiom) => {
				let value = builder.create_string(&ident.0);
				let alias = idiom.to_fb(builder);
				let alias = proto_fb::Alias::create(
					builder,
					&proto_fb::AliasArgs {
						value: Some(value),
						alias: Some(alias),
					},
				);

				proto_fb::DestructurePartArgs {
					part_type: proto_fb::DestructurePartType::Aliased,
					part: Some(alias.as_union_value()),
				}
			}
			Self::Destructure(name, parts) => {
				let name = builder.create_string(&name.0);
				let mut parts_vec = Vec::with_capacity(parts.len());
				for part in parts {
					parts_vec.push(part.to_fb(builder));
				}
				let parts_vector = builder.create_vector(&parts_vec);
				let destructure_ident_parts = proto_fb::DestructureIdentParts::create(
					builder,
					&proto_fb::DestructureIdentPartsArgs {
						name: Some(name),
						parts: Some(parts_vector),
					},
				);
				proto_fb::DestructurePartArgs {
					part_type: proto_fb::DestructurePartType::Destructure,
					part: Some(destructure_ident_parts.as_union_value()),
				}
			}
		};

		proto_fb::DestructurePart::create(builder, &args)
	}
}

impl FromFlatbuffers for DestructurePart {
	type Input<'a> = proto_fb::DestructurePart<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.part_type() {
			proto_fb::DestructurePartType::All => {
				let ident =
					input.part_as_all().ok_or_else(|| anyhow::anyhow!("Expected All part"))?;
				Ok(Self::All(Ident::from_fb(ident)?))
			}
			proto_fb::DestructurePartType::Field => {
				let ident =
					input.part_as_field().ok_or_else(|| anyhow::anyhow!("Expected Field part"))?;
				Ok(Self::Field(Ident::from_fb(ident)?))
			}
			proto_fb::DestructurePartType::Aliased => {
				let alias = input
					.part_as_aliased()
					.ok_or_else(|| anyhow::anyhow!("Expected Aliased part"))?;
				let value = alias.value().context("Missing value in Aliased part")?.to_string();
				let idiom =
					Idiom::from_fb(alias.alias().context("Missing alias in Aliased part")?)?;
				Ok(Self::Aliased(Ident(value), idiom))
			}
			proto_fb::DestructurePartType::Destructure => {
				let destructure_parts = input
					.part_as_destructure()
					.ok_or_else(|| anyhow::anyhow!("Expected Destructure part"))?;
				let name = destructure_parts
					.name()
					.context("Missing name in Destructure part")?
					.to_string();
				let parts_reader =
					destructure_parts.parts().context("Missing parts in Destructure part")?;
				let mut parts = Vec::<DestructurePart>::new();
				for part in parts_reader {
					parts.push(DestructurePart::from_fb(part)?);
				}
				Ok(Self::Destructure(Ident(name), parts))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported DestructurePart type for FlatBuffers deserialization: {:?}",
				input.part_type()
			)),
		}
	}
}

impl ToFlatbuffers for Graph {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Graph<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let dir = self.dir.to_fb(builder);
		let expr = self.expr.as_ref().map(|e| e.to_fb(builder));
		let what = self.what.to_fb(builder);
		let cond = self.cond.as_ref().map(|c| c.to_fb(builder));
		let split = self.split.as_ref().map(|s| s.to_fb(builder));
		let group = self.group.as_ref().map(|g| g.to_fb(builder));
		let order = self.order.as_ref().map(|o| o.to_fb(builder));
		let limit = match &self.limit {
			Some(limit) => match limit.0 {
				Value::Number(num) => Some(num.as_int() as u64),
				_ => {
					panic!("Limit must be a number")
				}
			},
			None => None,
		};
		let start = self.start.as_ref().map(|s| match s.0 {
			Value::Number(num) => num.as_int() as u64,
			_ => panic!("Start must be a number"),
		});
		let alias = self.alias.as_ref().map(|a| a.to_fb(builder));

		proto_fb::Graph::create(
			builder,
			&proto_fb::GraphArgs {
				dir,
				expr,
				what: Some(what),
				cond,
				split,
				group,
				order,
				limit,
				start,
				alias,
			},
		)
	}
}

impl FromFlatbuffers for Graph {
	type Input<'a> = proto_fb::Graph<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let dir = Dir::from_fb(input.dir())?;
		let expr = input.expr().map(Fields::from_fb).transpose()?;
		let what = GraphSubjects::from_fb(input.what().context("Missing what in Graph")?)?;
		let cond = input.cond().map(Value::from_fb).transpose()?.map(Cond);
		let split = input.split().map(Splits::from_fb).transpose()?;
		let group = input.group().map(Groups::from_fb).transpose()?;
		let order = input.order().map(Ordering::from_fb).transpose()?;
		let limit = input.limit();
		let start = input.start();
		let alias = input.alias().map(Idiom::from_fb).transpose()?;

		Ok(Self {
			dir,
			expr,
			what,
			cond,
			split,
			group,
			order,
			limit: limit.map(|l| Limit(Value::Number(Number::Int(l as i64)))),
			start: start.map(|s| Start(Value::Number(Number::Int(s as i64)))),
			alias,
		})
	}
}

impl ToFlatbuffers for Splits {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Splits<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut splits = Vec::with_capacity(self.0.len());
		for split in &self.0 {
			splits.push(split.to_fb(builder));
		}
		let splits_vector = builder.create_vector(&splits);
		proto_fb::Splits::create(
			builder,
			&proto_fb::SplitsArgs {
				splits: Some(splits_vector),
			},
		)
	}
}

impl FromFlatbuffers for Splits {
	type Input<'a> = proto_fb::Splits<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut splits = Vec::new();
		let splits_reader = input.splits().context("Splits is not set")?;
		for split in splits_reader {
			splits.push(Split::from_fb(split)?);
		}
		Ok(Self(splits))
	}
}

impl ToFlatbuffers for Split {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Idiom<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Split {
	type Input<'a> = proto_fb::Idiom<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let idiom = Idiom::from_fb(input)?;
		Ok(Self(idiom))
	}
}

impl ToFlatbuffers for Groups {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Groups<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut groups = Vec::with_capacity(self.0.len());
		for group in &self.0 {
			groups.push(group.to_fb(builder));
		}
		let groups_vector = builder.create_vector(&groups);
		proto_fb::Groups::create(
			builder,
			&proto_fb::GroupsArgs {
				groups: Some(groups_vector),
			},
		)
	}
}

impl FromFlatbuffers for Groups {
	type Input<'a> = proto_fb::Groups<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut groups = Vec::new();
		let groups_reader = input.groups().context("Groups is not set")?;
		for group in groups_reader {
			groups.push(Group::from_fb(group)?);
		}
		Ok(Self(groups))
	}
}

impl ToFlatbuffers for Group {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Idiom<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Group {
	type Input<'a> = proto_fb::Idiom<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let idiom = Idiom::from_fb(input)?;
		Ok(Self(idiom))
	}
}

impl ToFlatbuffers for Ordering {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::OrderingSpec<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Random => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::OrderingSpecArgs {
					ordering_type: proto_fb::OrderingType::Random,
					ordering: Some(null.as_union_value()),
				}
			}
			Self::Order(order_list) => {
				let order_list = order_list.to_fb(builder);
				proto_fb::OrderingSpecArgs {
					ordering_type: proto_fb::OrderingType::Ordered,
					ordering: Some(order_list.as_union_value()),
				}
			}
		};

		proto_fb::OrderingSpec::create(builder, &args)
	}
}

impl FromFlatbuffers for Ordering {
	type Input<'a> = proto_fb::OrderingSpec<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.ordering_type() {
			proto_fb::OrderingType::Random => Ok(Self::Random),
			proto_fb::OrderingType::Ordered => {
				let order_list = input
					.ordering_as_ordered()
					.ok_or_else(|| anyhow::anyhow!("Expected Ordered ordering"))?;
				let order_list = OrderList::from_fb(order_list)?;
				Ok(Self::Order(order_list))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported OrderingSpec type for FlatBuffers deserialization: {:?}",
				input.ordering_type()
			)),
		}
	}
}

impl ToFlatbuffers for OrderList {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::OrderList<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut orders = Vec::with_capacity(self.0.len());
		for order in &self.0 {
			orders.push(order.to_fb(builder));
		}
		let orders_vector = builder.create_vector(&orders);
		proto_fb::OrderList::create(
			builder,
			&proto_fb::OrderListArgs {
				orders: Some(orders_vector),
			},
		)
	}
}

impl FromFlatbuffers for OrderList {
	type Input<'a> = proto_fb::OrderList<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let orders_reader = input.orders().context("Orders is not set")?;
		let mut orders = Vec::new();
		for order in orders_reader {
			orders.push(Order::from_fb(order)?);
		}
		Ok(Self(orders))
	}
}

impl ToFlatbuffers for Order {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Order<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = self.value.to_fb(builder);

		proto_fb::Order::create(
			builder,
			&proto_fb::OrderArgs {
				value: Some(value),
				collate: self.collate,
				numeric: self.numeric,
				ascending: self.direction,
			},
		)
	}
}

impl FromFlatbuffers for Order {
	type Input<'a> = proto_fb::Order<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let value = Idiom::from_fb(input.value().context("Missing value in Order")?)?;
		let collate = input.collate();
		let numeric = input.numeric();
		let direction = input.ascending();

		Ok(Self {
			value,
			collate,
			numeric,
			direction,
		})
	}
}

impl ToFlatbuffers for Dir {
	type Output<'bldr> = proto_fb::GraphDirection;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		_builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Dir::In => proto_fb::GraphDirection::In,
			Dir::Out => proto_fb::GraphDirection::Out,
			Dir::Both => proto_fb::GraphDirection::Both,
		}
	}
}

impl FromFlatbuffers for Dir {
	type Input<'a> = proto_fb::GraphDirection;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input {
			proto_fb::GraphDirection::In => Ok(Dir::In),
			proto_fb::GraphDirection::Out => Ok(Dir::Out),
			proto_fb::GraphDirection::Both => Ok(Dir::Both),
			_ => Err(anyhow::anyhow!(
				"Unsupported GraphDirection type for FlatBuffers deserialization: {:?}",
				input
			)),
		}
	}
}

impl ToFlatbuffers for GraphSubjects {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::GraphSubjects<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut subjects = Vec::with_capacity(self.0.len());
		for subject in &self.0 {
			subjects.push(subject.to_fb(builder));
		}
		let subjects_vector = builder.create_vector(&subjects);
		proto_fb::GraphSubjects::create(
			builder,
			&proto_fb::GraphSubjectsArgs {
				subjects: Some(subjects_vector),
			},
		)
	}
}

impl FromFlatbuffers for GraphSubjects {
	type Input<'a> = proto_fb::GraphSubjects<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let subjects_reader = input.subjects().context("Missing subjects in GraphSubjects")?;
		let mut subjects = Vec::new();
		for subject in subjects_reader {
			subjects.push(GraphSubject::from_fb(subject)?);
		}
		Ok(GraphSubjects(subjects))
	}
}

impl ToFlatbuffers for GraphSubject {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::GraphSubject<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Table(table) => {
				let table = builder.create_string(&table.0);
				let table_fb = proto_fb::Table::create(
					builder,
					&proto_fb::TableArgs {
						name: Some(table),
					},
				);
				proto_fb::GraphSubjectArgs {
					subject_type: proto_fb::GraphSubjectType::Table,
					subject: Some(table_fb.as_union_value()),
				}
			}
			Self::Range(table, id_range) => {
				let table = builder.create_string(&table.0);
				let start = id_range.beg.to_fb(builder);
				let end = id_range.end.to_fb(builder);
				let range_fb = proto_fb::TableIdRange::create(
					builder,
					&proto_fb::TableIdRangeArgs {
						table: Some(table),
						start: Some(start),
						end: Some(end),
					},
				);

				proto_fb::GraphSubjectArgs {
					subject_type: proto_fb::GraphSubjectType::Range,
					subject: Some(range_fb.as_union_value()),
				}
			}
		};

		proto_fb::GraphSubject::create(builder, &args)
	}
}

impl FromFlatbuffers for GraphSubject {
	type Input<'a> = proto_fb::GraphSubject<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.subject_type() {
			proto_fb::GraphSubjectType::Table => {
				let table = input.subject_as_table().context("Expected Table subject")?;
				let name = table.name().context("Missing name in Table subject")?.to_string();
				Ok(GraphSubject::Table(Table(name)))
			}
			proto_fb::GraphSubjectType::Range => {
				let range = input.subject_as_range().context("Expected Range subject")?;
				let table_name =
					range.table().context("Missing table in Range subject")?.to_string();
				let start =
					Bound::from_fb(range.start().context("Missing start in Range subject")?)?;
				let end = Bound::from_fb(range.end().context("Missing end in Range subject")?)?;
				Ok(GraphSubject::Range(
					Table(table_name),
					IdRange {
						beg: start,
						end,
					},
				))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported GraphSubject type for FlatBuffers deserialization: {:?}",
				input.subject_type()
			)),
		}
	}
}

impl ToFlatbuffers for Bound<Id> {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::IdBound<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Bound::Included(id) => {
				let id_value = id.to_fb(builder);
				proto_fb::IdBoundArgs {
					id: Some(id_value),
					inclusive: true,
				}
			}
			Bound::Excluded(id) => {
				let id_value = id.to_fb(builder);
				proto_fb::IdBoundArgs {
					id: Some(id_value),
					inclusive: false,
				}
			}
			Bound::Unbounded => proto_fb::IdBoundArgs {
				id: None,
				inclusive: false,
			},
		};

		proto_fb::IdBound::create(builder, &args)
	}
}

impl FromFlatbuffers for Bound<Id> {
	type Input<'a> = proto_fb::IdBound<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		if let Some(id) = input.id() {
			let id_value = Id::from_fb(id)?;
			if input.inclusive() {
				Ok(Bound::Included(id_value))
			} else {
				Ok(Bound::Excluded(id_value))
			}
		} else {
			Ok(Bound::Unbounded)
		}
	}
}

impl ToFlatbuffers for Field {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Field<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Field::All => {
				let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				proto_fb::FieldArgs {
					field_type: proto_fb::FieldType::All,
					field: Some(null.as_union_value()),
				}
			}
			Field::Single {
				expr,
				alias,
			} => {
				let expr = expr.to_fb(builder);
				let alias = alias.as_ref().map(|a| a.to_fb(builder));
				let single_field = proto_fb::SingleField::create(
					builder,
					&proto_fb::SingleFieldArgs {
						expr: Some(expr),
						alias,
					},
				);

				proto_fb::FieldArgs {
					field_type: proto_fb::FieldType::Single,
					field: Some(single_field.as_union_value()),
				}
			}
		};

		proto_fb::Field::create(builder, &args)
	}
}

impl FromFlatbuffers for Field {
	type Input<'a> = proto_fb::Field<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.field_type() {
			proto_fb::FieldType::All => Ok(Field::All),
			proto_fb::FieldType::Single => {
				let single_field = input.field_as_single().context("Expected SingleField")?;
				let expr =
					Value::from_fb(single_field.expr().context("Missing expr in SingleField")?)?;
				let alias = single_field.alias().map(Idiom::from_fb).transpose()?;
				Ok(Field::Single {
					expr,
					alias,
				})
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported field type for FlatBuffers deserialization: {:?}",
				input.field_type()
			)),
		}
	}
}

impl ToFlatbuffers for Fields {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Fields<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut fields = Vec::with_capacity(self.0.len());
		for field in &self.0 {
			let args = match field {
				Field::All => {
					let null = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
					proto_fb::FieldArgs {
						field_type: proto_fb::FieldType::All,
						field: Some(null.as_union_value()),
					}
				}
				Field::Single {
					expr,
					alias,
				} => {
					let expr = expr.to_fb(builder);
					let alias = alias.as_ref().map(|a| a.to_fb(builder));
					let single_field = proto_fb::SingleField::create(
						builder,
						&proto_fb::SingleFieldArgs {
							expr: Some(expr),
							alias,
						},
					);
					proto_fb::FieldArgs {
						field_type: proto_fb::FieldType::Single,
						field: Some(single_field.as_union_value()),
					}
				}
			};

			let field_item = proto_fb::Field::create(builder, &args);

			fields.push(field_item);
		}
		let fields_vector = builder.create_vector(&fields);
		proto_fb::Fields::create(
			builder,
			&proto_fb::FieldsArgs {
				single: self.1,
				fields: Some(fields_vector),
			},
		)
	}
}

impl FromFlatbuffers for Fields {
	type Input<'a> = proto_fb::Fields<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let single = input.single();
		let mut fields = Vec::new();
		let fields_reader = input.fields().context("Fields is not set")?;
		for field in fields_reader {
			fields.push(Field::from_fb(field)?);
		}
		Ok(Fields(fields, single))
	}
}

impl ToFlatbuffers for Fetch {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Fetch {
	type Input<'a> = proto_fb::Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let value = Value::from_fb(input)?;
		Ok(Fetch(value))
	}
}

impl ToFlatbuffers for Fetchs {
	type Output<'bldr> = flatbuffers::WIPOffset<
		::flatbuffers::Vector<'bldr, ::flatbuffers::ForwardsUOffset<proto_fb::Value<'bldr>>>,
	>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut values = Vec::with_capacity(self.0.len());
		for value in &self.0 {
			values.push(value.to_fb(builder));
		}
		builder.create_vector(&values)
	}
}

impl FromFlatbuffers for Fetchs {
	type Input<'a> = flatbuffers::Vector<'a, ::flatbuffers::ForwardsUOffset<proto_fb::Value<'a>>>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut fetchs = Vec::new();
		for value in input {
			fetchs.push(Fetch(Value::from_fb(value)?));
		}
		Ok(Fetchs(fetchs))
	}
}

impl ToFlatbuffers for Variables {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Variables<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut vars = Vec::with_capacity(self.len());
		for (key, value) in self.iter() {
			let key_str = builder.create_string(key);
			let value_fb = value.to_fb(builder);
			let var = proto_fb::Variable::create(
				builder,
				&proto_fb::VariableArgs {
					key: Some(key_str),
					value: Some(value_fb),
				},
			);
			vars.push(var);
		}
		let vars_vector = builder.create_vector(&vars);
		proto_fb::Variables::create(
			builder,
			&proto_fb::VariablesArgs {
				items: Some(vars_vector),
			},
		)
	}
}

impl FromFlatbuffers for Variables {
	type Input<'a> = proto_fb::Variables<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let items_reader = input.items().context("Variables is not set")?;
		let mut vars = Variables::new();
		for item in items_reader {
			let key = item.key().context("Missing key in Variable")?.to_string();
			let value = Value::from_fb(item.value().context("Missing value in Variable")?)?;
			vars.insert(key, value);
		}
		Ok(vars)
	}
}

impl ToFlatbuffers for Operator {
	type Output<'bldr> = proto_fb::Operator;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		_builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Operator::Neg => proto_fb::Operator::Neg,
			Operator::Not => proto_fb::Operator::Not,
			Operator::Or => proto_fb::Operator::Or,
			Operator::And => proto_fb::Operator::And,
			Operator::Tco => proto_fb::Operator::Tco,
			Operator::Nco => proto_fb::Operator::Nco,
			Operator::Add => proto_fb::Operator::Add,
			Operator::Sub => proto_fb::Operator::Sub,
			Operator::Mul => proto_fb::Operator::Mul,
			Operator::Div => proto_fb::Operator::Div,
			Operator::Rem => proto_fb::Operator::Rem,
			Operator::Pow => proto_fb::Operator::Pow,
			Operator::Inc => proto_fb::Operator::Inc,
			Operator::Dec => proto_fb::Operator::Dec,
			Operator::Ext => proto_fb::Operator::Ext,
			Operator::Equal => proto_fb::Operator::Equal,
			Operator::Exact => proto_fb::Operator::Exact,
			Operator::NotEqual => proto_fb::Operator::NotEqual,
			Operator::AllEqual => proto_fb::Operator::AllEqual,
			Operator::AnyEqual => proto_fb::Operator::AnyEqual,
			Operator::Like => proto_fb::Operator::Like,
			Operator::NotLike => proto_fb::Operator::NotLike,
			Operator::AllLike => proto_fb::Operator::AllLike,
			Operator::AnyLike => proto_fb::Operator::AnyLike,
			Operator::LessThan => proto_fb::Operator::LessThan,
			Operator::LessThanOrEqual => proto_fb::Operator::LessThanOrEqual,
			Operator::MoreThan => proto_fb::Operator::GreaterThan,
			Operator::MoreThanOrEqual => proto_fb::Operator::GreaterThanOrEqual,
			Operator::Contain => proto_fb::Operator::Contain,
			Operator::NotContain => proto_fb::Operator::NotContain,
			Operator::ContainAll => proto_fb::Operator::ContainAll,
			Operator::ContainAny => proto_fb::Operator::ContainAny,
			Operator::ContainNone => proto_fb::Operator::ContainNone,
			Operator::Inside => proto_fb::Operator::Inside,
			Operator::NotInside => proto_fb::Operator::NotInside,
			Operator::AllInside => proto_fb::Operator::AllInside,
			Operator::AnyInside => proto_fb::Operator::AnyInside,
			Operator::NoneInside => proto_fb::Operator::NoneInside,
			Operator::Outside => proto_fb::Operator::Outside,
			Operator::Intersects => proto_fb::Operator::Intersects,
			Operator::Knn(_, _) => panic!("KNN operator not supported"),
			Operator::Ann(_, _) => panic!("ANN operator not supported"),
			Operator::Matches(_) => panic!("Matches not supported"),
		}
	}
}

impl FromFlatbuffers for Operator {
	type Input<'a> = proto_fb::Operator;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input {
			proto_fb::Operator::Neg => Ok(Operator::Neg),
			proto_fb::Operator::Not => Ok(Operator::Not),
			proto_fb::Operator::Or => Ok(Operator::Or),
			proto_fb::Operator::And => Ok(Operator::And),
			proto_fb::Operator::Tco => Ok(Operator::Tco),
			proto_fb::Operator::Nco => Ok(Operator::Nco),
			proto_fb::Operator::Add => Ok(Operator::Add),
			proto_fb::Operator::Sub => Ok(Operator::Sub),
			proto_fb::Operator::Mul => Ok(Operator::Mul),
			proto_fb::Operator::Div => Ok(Operator::Div),
			proto_fb::Operator::Rem => Ok(Operator::Rem),
			proto_fb::Operator::Pow => Ok(Operator::Pow),
			proto_fb::Operator::Inc => Ok(Operator::Inc),
			proto_fb::Operator::Dec => Ok(Operator::Dec),
			proto_fb::Operator::Ext => Ok(Operator::Ext),
			proto_fb::Operator::Equal => Ok(Operator::Equal),
			proto_fb::Operator::Exact => Ok(Operator::Exact),
			proto_fb::Operator::NotEqual => Ok(Operator::NotEqual),
			proto_fb::Operator::AllEqual => Ok(Operator::AllEqual),
			proto_fb::Operator::AnyEqual => Ok(Operator::AnyEqual),
			proto_fb::Operator::Like => Ok(Operator::Like),
			proto_fb::Operator::NotLike => Ok(Operator::NotLike),
			proto_fb::Operator::AllLike => Ok(Operator::AllLike),
			proto_fb::Operator::AnyLike => Ok(Operator::AnyLike),
			proto_fb::Operator::LessThan => Ok(Operator::LessThan),
			proto_fb::Operator::LessThanOrEqual => Ok(Operator::LessThanOrEqual),
			proto_fb::Operator::GreaterThan => Ok(Operator::MoreThan),
			proto_fb::Operator::GreaterThanOrEqual => Ok(Operator::MoreThanOrEqual),
			proto_fb::Operator::Contain => Ok(Operator::Contain),
			proto_fb::Operator::NotContain => Ok(Operator::NotContain),
			proto_fb::Operator::ContainAll => Ok(Operator::ContainAll),
			proto_fb::Operator::ContainAny => Ok(Operator::ContainAny),
			proto_fb::Operator::ContainNone => Ok(Operator::ContainNone),
			proto_fb::Operator::Inside => Ok(Operator::Inside),
			proto_fb::Operator::NotInside => Ok(Operator::NotInside),
			proto_fb::Operator::AllInside => Ok(Operator::AllInside),
			proto_fb::Operator::AnyInside => Ok(Operator::AnyInside),
			proto_fb::Operator::NoneInside => Ok(Operator::NoneInside),
			proto_fb::Operator::Outside => Ok(Operator::Outside),
			proto_fb::Operator::Intersects => Ok(Operator::Intersects),
			_ => Err(anyhow::anyhow!("Invalid operator: {:?}", input)),
		}
	}
}

impl ToFlatbuffers for Data {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Data<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let (contents_type, contents) = match self {
			Data::EmptyExpression => (
				proto_fb::DataContents::Empty,
				proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {}).as_union_value(),
			),
			Data::SetExpression(set) => {
				let mut items = Vec::with_capacity(set.len());
				for (idiom, operator, value) in set {
					let idiom_fb = idiom.to_fb(builder);
					let operator_fb = operator.to_fb(builder);
					let value_fb = value.to_fb(builder);
					items.push(proto_fb::SetExpr::create(
						builder,
						&proto_fb::SetExprArgs {
							idiom: Some(idiom_fb),
							operator: operator_fb,
							value: Some(value_fb),
						},
					));
				}
				let set_exprs = builder.create_vector(&items);
				(
					proto_fb::DataContents::Set,
					proto_fb::SetMultiExpr::create(
						builder,
						&proto_fb::SetMultiExprArgs {
							items: Some(set_exprs),
						},
					)
					.as_union_value(),
				)
			}
			Data::UnsetExpression(unset) => {
				let mut items = Vec::with_capacity(unset.len());
				for idiom in unset {
					let idiom_fb = idiom.to_fb(builder);
					items.push(idiom_fb);
				}
				let unset_exprs = builder.create_vector(&items);
				(
					proto_fb::DataContents::Unset,
					proto_fb::UnsetMultiExpr::create(
						builder,
						&proto_fb::UnsetMultiExprArgs {
							items: Some(unset_exprs),
						},
					)
					.as_union_value(),
				)
			}
			Data::PatchExpression(patch) => {
				let patch_fb = patch.to_fb(builder);
				(proto_fb::DataContents::Patch, patch_fb.as_union_value())
			}
			Data::MergeExpression(merge) => {
				let merge_fb = merge.to_fb(builder);
				(proto_fb::DataContents::Merge, merge_fb.as_union_value())
			}
			Data::ReplaceExpression(replace) => {
				let replace_fb = replace.to_fb(builder);
				(proto_fb::DataContents::Replace, replace_fb.as_union_value())
			}
			Data::ContentExpression(content) => {
				let content_fb = content.to_fb(builder);
				(proto_fb::DataContents::Content, content_fb.as_union_value())
			}
			Data::SingleExpression(single) => {
				let single_fb = single.to_fb(builder);
				(proto_fb::DataContents::Value, single_fb.as_union_value())
			}
			Data::ValuesExpression(values) => {
				let mut items = Vec::with_capacity(values.len());
				for inner_values in values {
					let mut inner_items = Vec::with_capacity(inner_values.len());
					for (idiom, value) in inner_values {
						let idiom_fb = idiom.to_fb(builder);
						let value_fb = value.to_fb(builder);

						inner_items.push(proto_fb::IdiomValuePair::create(
							builder,
							&proto_fb::IdiomValuePairArgs {
								idiom: Some(idiom_fb),
								value: Some(value_fb),
							},
						));
					}
					let inner_items = builder.create_vector(&inner_items);
					items.push(proto_fb::ValuesExpr::create(
						builder,
						&proto_fb::ValuesExprArgs {
							items: Some(inner_items),
						},
					));
				}

				let values_fb = builder.create_vector(&items);

				(
					proto_fb::DataContents::Values,
					proto_fb::ValuesMultiExpr::create(
						builder,
						&proto_fb::ValuesMultiExprArgs {
							items: Some(values_fb),
						},
					)
					.as_union_value(),
				)
			}
			Data::UpdateExpression(update) => {
				let mut items = Vec::with_capacity(update.len());
				for (idiom, operator, value) in update {
					let idiom_fb = idiom.to_fb(builder);
					let operator_fb = operator.to_fb(builder);
					let value_fb = value.to_fb(builder);
					items.push(proto_fb::SetExpr::create(
						builder,
						&proto_fb::SetExprArgs {
							idiom: Some(idiom_fb),
							operator: operator_fb,
							value: Some(value_fb),
						},
					));
				}
				let update_exprs = builder.create_vector(&items);
				(
					proto_fb::DataContents::Update,
					proto_fb::SetMultiExpr::create(
						builder,
						&proto_fb::SetMultiExprArgs {
							items: Some(update_exprs),
						},
					)
					.as_union_value(),
				)
			}
		};

		proto_fb::Data::create(
			builder,
			&proto_fb::DataArgs {
				contents_type,
				contents: Some(contents),
			},
		)
	}
}

type SetExpr = (Idiom, Operator, Value);
type SetMultiExpr = Vec<SetExpr>;
type UnsetMultiExpr = Vec<Idiom>;
type ValuesExpr = Vec<Vec<(Idiom, Value)>>;

impl FromFlatbuffers for Data {
	type Input<'a> = proto_fb::Data<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.contents_type() {
			proto_fb::DataContents::Empty => Ok(Data::EmptyExpression),
			proto_fb::DataContents::Set => {
				let params = input.contents_as_set().context("Missing set")?;
				Ok(Data::SetExpression(SetMultiExpr::from_fb(params)?))
			}
			proto_fb::DataContents::Unset => {
				let params = input.contents_as_unset().context("Missing unset")?;
				Ok(Data::UnsetExpression(UnsetMultiExpr::from_fb(params)?))
			}
			proto_fb::DataContents::Patch => {
				let params = input.contents_as_patch().context("Missing patch")?;
				Ok(Data::PatchExpression(Value::from_fb(params)?))
			}
			proto_fb::DataContents::Merge => {
				let params = input.contents_as_merge().context("Missing merge")?;
				Ok(Data::MergeExpression(Value::from_fb(params)?))
			}
			proto_fb::DataContents::Replace => {
				let params = input.contents_as_replace().context("Missing replace")?;
				Ok(Data::ReplaceExpression(Value::from_fb(params)?))
			}
			proto_fb::DataContents::Content => {
				let params = input.contents_as_content().context("Missing content")?;
				Ok(Data::ContentExpression(Value::from_fb(params)?))
			}
			proto_fb::DataContents::Value => {
				let params = input.contents_as_value().context("Missing value")?;
				Ok(Data::SingleExpression(Value::from_fb(params)?))
			}
			proto_fb::DataContents::Values => {
				let params = input.contents_as_values().context("Missing values")?;
				Ok(Data::ValuesExpression(ValuesExpr::from_fb(params)?))
			}
			proto_fb::DataContents::Update => {
				let params = input.contents_as_update().context("Missing update")?;
				Ok(Data::UpdateExpression(SetMultiExpr::from_fb(params)?))
			}
			unexpected => {
				Err(anyhow::anyhow!("Unexpected data contents: {unexpected:?}"))
			}
		}
	}
}

impl FromFlatbuffers for SetMultiExpr {
	type Input<'a> = proto_fb::SetMultiExpr<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.items() {
			Some(items) => {
				let mut output = Vec::new();
				for item in items {
					let set_expr = SetExpr::from_fb(item)?;
					output.push(set_expr);
				}
				Ok(output)
			}
			None => Ok(Vec::new()),
		}
	}
}

impl ToFlatbuffers for SetExpr {
	type Output<'a> = flatbuffers::WIPOffset<proto_fb::SetExpr<'a>>;

	#[inline]
	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let idiom = self.0.to_fb(fbb);
		let operator = self.1.to_fb(fbb);
		let value = self.2.to_fb(fbb);
		proto_fb::SetExpr::create(
			fbb,
			&proto_fb::SetExprArgs {
				idiom: Some(idiom),
				operator,
				value: Some(value),
			},
		)
	}
}

impl FromFlatbuffers for SetExpr {
	type Input<'a> = proto_fb::SetExpr<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let idiom = Idiom::from_fb(input.idiom().context("Missing idiom")?)?;
		let operator = Operator::from_fb(input.operator())?;
		let value = Value::from_fb(input.value().context("Missing value")?)?;
		Ok((idiom, operator, value))
	}
}

impl ToFlatbuffers for SetMultiExpr {
	type Output<'a> = flatbuffers::WIPOffset<proto_fb::SetMultiExpr<'a>>;

	#[inline]
	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let items = self.iter().map(|v| v.to_fb(fbb)).collect::<Vec<_>>();
		let items = fbb.create_vector(&items);
		proto_fb::SetMultiExpr::create(
			fbb,
			&proto_fb::SetMultiExprArgs {
				items: Some(items),
			},
		)
	}
}

impl FromFlatbuffers for UnsetMultiExpr {
	type Input<'a> = proto_fb::UnsetMultiExpr<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.items() {
			Some(items) => {
				let mut output = Vec::new();
				for item in items {
					let idiom = Idiom::from_fb(item)?;
					output.push(idiom);
				}
				Ok(output)
			}
			None => Ok(Vec::new()),
		}
	}
}

impl FromFlatbuffers for ValuesExpr {
	type Input<'a> = proto_fb::ValuesMultiExpr<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut output = Vec::new();
		let Some(items) = input.items() else {
			return Ok(Vec::new());
		};

		for values in items {
			let mut inner_items = Vec::new();
			let Some(values) = values.items() else {
				output.push(Vec::new());
				continue;
			};

			for value in values {
				let idiom = Idiom::from_fb(value.idiom().context("Missing idiom")?)?;
				let value = Value::from_fb(value.value().context("Missing value")?)?;
				inner_items.push((idiom, value));
			}
			output.push(inner_items);
		}
		Ok(output)
	}
}

impl TryFrom<proto::Fields> for Fields {
	type Error = anyhow::Error;

	fn try_from(value: proto::Fields) -> Result<Self, Self::Error> {
		let single = value.single;
		let fields =
			value.fields.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
		Ok(Fields(fields, single))
	}
}

impl TryFrom<Fields> for proto::Fields {
	type Error = anyhow::Error;

	fn try_from(value: Fields) -> Result<Self, Self::Error> {
		Ok(proto::Fields {
			single: value.1,
			fields: value.0.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?,
		})
	}
}

impl TryFrom<proto::fields::Field> for Field {
	type Error = anyhow::Error;

	fn try_from(proto: proto::fields::Field) -> Result<Self, Self::Error> {
		let Some(inner_field) = proto.field else {
			return Err(anyhow::anyhow!("Missing field"));
		};
		match inner_field {
			proto::fields::field::Field::All(_) => Ok(Field::All),
			proto::fields::field::Field::Single(single) => Ok(Field::Single {
				expr: single.expr.context("Missing expr")?.try_into()?,
				alias: single.alias.map(TryInto::try_into).transpose()?,
			}),
		}
	}
}

impl TryFrom<Field> for proto::fields::Field {
	type Error = anyhow::Error;

	fn try_from(value: Field) -> Result<Self, Self::Error> {
		let field = match value {
			Field::All => proto::fields::field::Field::All(proto::NullValue::default()),
			Field::Single {
				expr,
				alias,
			} => proto::fields::field::Field::Single(proto::fields::SingleField {
				expr: Some(expr.try_into()?),
				alias: alias.map(TryInto::try_into).transpose()?,
			}),
		};

		Ok(proto::fields::Field {
			field: Some(field),
		})
	}
}

impl TryFrom<proto::Idiom> for Idiom {
	type Error = anyhow::Error;

	fn try_from(proto: proto::Idiom) -> Result<Self, Self::Error> {
		idiom(&proto.value)
	}
}

impl TryFrom<Idiom> for proto::Idiom {
	type Error = anyhow::Error;

	fn try_from(value: Idiom) -> Result<Self, Self::Error> {
		Ok(proto::Idiom {
			value: value.to_string(),
		})
	}
}

impl From<prost_types::Duration> for Timeout {
	fn from(value: prost_types::Duration) -> Self {
		Timeout(Duration::from(value))
	}
}

impl TryFrom<proto::Fetchs> for Fetchs {
	type Error = anyhow::Error;

	fn try_from(value: proto::Fetchs) -> Result<Self, Self::Error> {
		let items =
			value.items.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
		Ok(Fetchs(items))
	}
}

impl TryFrom<proto::Value> for Fetch {
	type Error = anyhow::Error;

	fn try_from(value: proto::Value) -> Result<Self, Self::Error> {
		let value = Value::try_from(value)?;
		Ok(Fetch(value))
	}
}

impl TryFrom<proto::Operator> for Operator {
	type Error = anyhow::Error;

	fn try_from(value: proto::Operator) -> Result<Self, Self::Error> {
		match value {
			proto::Operator::Unspecified => Err(anyhow::anyhow!("operator is required")),
			proto::Operator::Neg => Ok(Operator::Neg),
			proto::Operator::Not => Ok(Operator::Not),
			proto::Operator::Or => Ok(Operator::Or),
			proto::Operator::And => Ok(Operator::And),
			proto::Operator::Tco => Ok(Operator::Tco),
			proto::Operator::Nco => Ok(Operator::Nco),
			proto::Operator::Add => Ok(Operator::Add),
			proto::Operator::Sub => Ok(Operator::Sub),
			proto::Operator::Mul => Ok(Operator::Mul),
			proto::Operator::Div => Ok(Operator::Div),
			proto::Operator::Rem => Ok(Operator::Rem),
			proto::Operator::Pow => Ok(Operator::Pow),
			proto::Operator::Inc => Ok(Operator::Inc),
			proto::Operator::Dec => Ok(Operator::Dec),
			proto::Operator::Ext => Ok(Operator::Ext),
			proto::Operator::Equal => Ok(Operator::Equal),
			proto::Operator::Exact => Ok(Operator::Exact),
			proto::Operator::NotEqual => Ok(Operator::NotEqual),
			proto::Operator::AllEqual => Ok(Operator::AllEqual),
			proto::Operator::AnyEqual => Ok(Operator::AnyEqual),
			proto::Operator::Like => Ok(Operator::Like),
			proto::Operator::NotLike => Ok(Operator::NotLike),
			proto::Operator::AllLike => Ok(Operator::AllLike),
			proto::Operator::AnyLike => Ok(Operator::AnyLike),
			proto::Operator::LessThan => Ok(Operator::LessThan),
			proto::Operator::LessThanOrEqual => Ok(Operator::LessThanOrEqual),
			proto::Operator::GreaterThan => Ok(Operator::MoreThan),
			proto::Operator::GreaterThanOrEqual => Ok(Operator::MoreThanOrEqual),
			proto::Operator::Contain => Ok(Operator::Contain),
			proto::Operator::NotContain => Ok(Operator::NotContain),
			proto::Operator::ContainAll => Ok(Operator::ContainAll),
			proto::Operator::ContainAny => Ok(Operator::ContainAny),
			proto::Operator::ContainNone => Ok(Operator::ContainNone),
			proto::Operator::Inside => Ok(Operator::Inside),
			proto::Operator::NotInside => Ok(Operator::NotInside),
			proto::Operator::AllInside => Ok(Operator::AllInside),
			proto::Operator::AnyInside => Ok(Operator::AnyInside),
			proto::Operator::NoneInside => Ok(Operator::NoneInside),
			proto::Operator::Outside => Ok(Operator::Outside),
			proto::Operator::Intersects => Ok(Operator::Intersects),
		}
	}
}

impl TryFrom<Operator> for proto::Operator {
	type Error = anyhow::Error;

	fn try_from(value: Operator) -> Result<Self, Self::Error> {
		match value {
			Operator::Neg => Ok(proto::Operator::Neg),
			Operator::Not => Ok(proto::Operator::Not),
			Operator::Or => Ok(proto::Operator::Or),
			Operator::And => Ok(proto::Operator::And),
			Operator::Tco => Ok(proto::Operator::Tco),
			Operator::Nco => Ok(proto::Operator::Nco),
			Operator::Add => Ok(proto::Operator::Add),
			Operator::Sub => Ok(proto::Operator::Sub),
			Operator::Mul => Ok(proto::Operator::Mul),
			Operator::Div => Ok(proto::Operator::Div),
			Operator::Rem => Ok(proto::Operator::Rem),
			Operator::Pow => Ok(proto::Operator::Pow),
			Operator::Inc => Ok(proto::Operator::Inc),
			Operator::Dec => Ok(proto::Operator::Dec),
			Operator::Ext => Ok(proto::Operator::Ext),
			Operator::Equal => Ok(proto::Operator::Equal),
			Operator::Exact => Ok(proto::Operator::Exact),
			Operator::NotEqual => Ok(proto::Operator::NotEqual),
			Operator::AllEqual => Ok(proto::Operator::AllEqual),
			Operator::AnyEqual => Ok(proto::Operator::AnyEqual),
			Operator::Like => Ok(proto::Operator::Like),
			Operator::NotLike => Ok(proto::Operator::NotLike),
			Operator::AllLike => Ok(proto::Operator::AllLike),
			Operator::AnyLike => Ok(proto::Operator::AnyLike),
			Operator::LessThan => Ok(proto::Operator::LessThan),
			Operator::LessThanOrEqual => Ok(proto::Operator::LessThanOrEqual),
			Operator::MoreThan => Ok(proto::Operator::GreaterThan),
			Operator::MoreThanOrEqual => Ok(proto::Operator::GreaterThanOrEqual),
			Operator::Contain => Ok(proto::Operator::Contain),
			Operator::NotContain => Ok(proto::Operator::NotContain),
			Operator::ContainAll => Ok(proto::Operator::ContainAll),
			Operator::ContainAny => Ok(proto::Operator::ContainAny),
			Operator::ContainNone => Ok(proto::Operator::ContainNone),
			Operator::Inside => Ok(proto::Operator::Inside),
			Operator::NotInside => Ok(proto::Operator::NotInside),
			Operator::AllInside => Ok(proto::Operator::AllInside),
			Operator::AnyInside => Ok(proto::Operator::AnyInside),
			Operator::NoneInside => Ok(proto::Operator::NoneInside),
			Operator::Outside => Ok(proto::Operator::Outside),
			Operator::Intersects => Ok(proto::Operator::Intersects),
			Operator::Matches(_) => Err(anyhow::anyhow!("matches is not supported")),
			Operator::Knn(_, _) => Err(anyhow::anyhow!("knn is not supported")),
			Operator::Ann(_, _) => Err(anyhow::anyhow!("ann is not supported")),
		}
	}
}

impl TryFrom<proto::Output> for crate::expr::output::Output {
	type Error = anyhow::Error;

	fn try_from(value: proto::Output) -> Result<Self, Self::Error> {
		use proto::output::Output as OutputType;
		let Some(inner) = value.output else {
			return Ok(crate::expr::output::Output::None);
		};

		match inner {
			OutputType::Null(_) => Ok(crate::expr::output::Output::Null),
			OutputType::Diff(_) => Ok(crate::expr::output::Output::Diff),
			OutputType::After(_) => Ok(crate::expr::output::Output::After),
			OutputType::Before(_) => Ok(crate::expr::output::Output::Before),
			OutputType::Fields(fields) => {
				Ok(crate::expr::output::Output::Fields(fields.try_into()?))
			}
		}
	}
}

impl TryFrom<crate::expr::output::Output> for proto::Output {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::output::Output) -> Result<Self, Self::Error> {
		use proto::output::Output as OutputType;
		match value {
			crate::expr::output::Output::None => Ok(proto::Output {
				output: None,
			}),
			crate::expr::output::Output::Null => Ok(proto::Output {
				output: Some(OutputType::Null(proto::NullValue {})),
			}),
			crate::expr::output::Output::Diff => Ok(proto::Output {
				output: Some(OutputType::Diff(proto::NullValue {})),
			}),
			crate::expr::output::Output::After => Ok(proto::Output {
				output: Some(OutputType::After(proto::NullValue {})),
			}),
			crate::expr::output::Output::Before => Ok(proto::Output {
				output: Some(OutputType::Before(proto::NullValue {})),
			}),
			crate::expr::output::Output::Fields(fields) => Ok(proto::Output {
				output: Some(OutputType::Fields(fields.try_into()?)),
			}),
		}
	}
}

impl TryFrom<proto::Data> for Data {
	type Error = anyhow::Error;

	fn try_from(value: proto::Data) -> Result<Self, Self::Error> {
		use proto::data::Data as DataType;
		let Some(inner) = value.data else {
			return Err(anyhow::anyhow!("data is required"));
		};

		match inner {
			DataType::Empty(_) => Ok(Data::EmptyExpression),
			DataType::Set(set_expr) => {
				Ok(Data::SetExpression(try_from_set_multi_expr_proto(set_expr)?))
			}
			DataType::Unset(unset_expr) => {
				Ok(Data::UnsetExpression(try_from_unset_multi_expr_proto(unset_expr)?))
			}
			DataType::Patch(value) => Ok(Data::PatchExpression(value.try_into()?)),
			DataType::Merge(merge_expr) => Ok(Data::MergeExpression(merge_expr.try_into()?)),
			DataType::Replace(replace_expr) => {
				Ok(Data::ReplaceExpression(replace_expr.try_into()?))
			}
			DataType::Content(content_expr) => {
				Ok(Data::ContentExpression(content_expr.try_into()?))
			}
			DataType::Value(value) => Ok(Data::SingleExpression(value.try_into()?)),
			DataType::Values(values) => {
				Ok(Data::ValuesExpression(try_from_values_multi_expr_proto(values)?))
			}
			DataType::Update(update_expr) => {
				Ok(Data::UpdateExpression(try_from_set_multi_expr_proto(update_expr)?))
			}
		}
	}
}

impl TryFrom<crate::expr::data::Data> for proto::Data {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::data::Data) -> Result<Self, Self::Error> {
		use proto::data::Data as DataType;
		match value {
			crate::expr::Data::EmptyExpression => Ok(proto::Data {
				data: Some(DataType::Empty(proto::NullValue {})),
			}),
			crate::expr::Data::SetExpression(set_expr) => Ok(proto::Data {
				data: Some(DataType::Set(try_from_set_multi_expr(set_expr)?)),
			}),
			crate::expr::Data::UnsetExpression(unset_expr) => Ok(proto::Data {
				data: Some(DataType::Unset(try_from_unset_multi_expr(unset_expr)?)),
			}),
			crate::expr::Data::PatchExpression(patch_expr) => Ok(proto::Data {
				data: Some(DataType::Patch(patch_expr.try_into()?)),
			}),
			crate::expr::Data::MergeExpression(merge_expr) => Ok(proto::Data {
				data: Some(DataType::Merge(merge_expr.try_into()?)),
			}),
			crate::expr::Data::ReplaceExpression(replace_expr) => Ok(proto::Data {
				data: Some(DataType::Replace(replace_expr.try_into()?)),
			}),
			crate::expr::Data::ContentExpression(content_expr) => Ok(proto::Data {
				data: Some(DataType::Content(content_expr.try_into()?)),
			}),
			crate::expr::Data::SingleExpression(value) => Ok(proto::Data {
				data: Some(DataType::Value(value.try_into()?)),
			}),
			crate::expr::Data::ValuesExpression(values) => Ok(proto::Data {
				data: Some(DataType::Values(try_from_values_multi_expr(values)?)),
			}),
			crate::expr::Data::UpdateExpression(update_expr) => Ok(proto::Data {
				data: Some(DataType::Update(try_from_set_multi_expr(update_expr)?)),
			}),
		}
	}
}

fn try_from_set_multi_expr_proto(
	proto: proto::data::SetMultiExpr,
) -> Result<Vec<(Idiom, Operator, Value)>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		out.push(try_from_set_expr_proto(item)?);
	}
	Ok(out)
}

fn try_from_set_expr_proto(
	proto::data::SetExpr {
		idiom,
		operator,
		value,
	}: proto::data::SetExpr,
) -> Result<(Idiom, Operator, Value), anyhow::Error> {
	let idiom = idiom.context("idiom is required")?.try_into()?;
	let operator = Operator::try_from(proto::Operator::try_from(operator)?)?;
	let value = value.context("value is required")?.try_into()?;
	Ok((idiom, operator, value))
}

fn try_from_set_multi_expr(
	expr: Vec<(Idiom, Operator, Value)>,
) -> Result<proto::data::SetMultiExpr, anyhow::Error> {
	let mut out = proto::data::SetMultiExpr::default();
	for item in expr {
		out.items.push(try_from_set_expr(item)?);
	}
	Ok(out)
}

fn try_from_set_expr(
	expr: (Idiom, Operator, Value),
) -> Result<proto::data::SetExpr, anyhow::Error> {
	let (idiom, operator, value) = expr;
	Ok(proto::data::SetExpr {
		idiom: Some(idiom.try_into()?),
		operator: proto::Operator::try_from(operator)? as i32,
		value: Some(value.try_into()?),
	})
}

fn try_from_unset_multi_expr_proto(
	proto: proto::data::UnsetMultiExpr,
) -> Result<Vec<Idiom>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		out.push(item.try_into()?);
	}
	Ok(out)
}

fn try_from_unset_multi_expr(
	expr: Vec<Idiom>,
) -> Result<proto::data::UnsetMultiExpr, anyhow::Error> {
	let mut out = proto::data::UnsetMultiExpr::default();
	for item in expr {
		out.items.push(item.try_into()?);
	}
	Ok(out)
}

fn try_from_values_multi_expr_proto(
	proto: proto::data::ValuesMultiExpr,
) -> Result<Vec<Vec<(Idiom, Value)>>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		out.push(try_from_values_expr_proto(item)?);
	}
	Ok(out)
}

fn try_from_values_expr_proto(
	proto: proto::data::ValuesExpr,
) -> Result<Vec<(Idiom, Value)>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		let idiom = item.idiom.context("idiom is required")?.try_into()?;
		let value = item.value.context("value is required")?.try_into()?;
		out.push((idiom, value));
	}
	Ok(out)
}

fn try_from_values_multi_expr(
	expr: Vec<Vec<(Idiom, Value)>>,
) -> Result<proto::data::ValuesMultiExpr, anyhow::Error> {
	let mut out = proto::data::ValuesMultiExpr::default();
	for item in expr {
		out.items.push(try_from_values_expr(item)?);
	}
	Ok(out)
}

fn try_from_values_expr(
	expr: Vec<(Idiom, Value)>,
) -> Result<proto::data::ValuesExpr, anyhow::Error> {
	let mut out = proto::data::ValuesExpr::default();
	for item in expr {
		let idiom = item.0.try_into()?;
		let value = item.1.try_into()?;
		out.items.push(proto::data::IdiomValuePair {
			idiom: Some(idiom),
			value: Some(value),
		});
	}
	Ok(out)
}

impl TryFrom<proto::Explain> for crate::expr::Explain {
	type Error = anyhow::Error;

	fn try_from(value: proto::Explain) -> Result<Self, Self::Error> {
		Ok(Self(value.explain))
	}
}

impl TryFrom<crate::expr::Explain> for proto::Explain {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::Explain) -> Result<Self, Self::Error> {
		Ok(Self {
			explain: value.0,
		})
	}
}

impl TryFrom<proto::With> for crate::expr::With {
	type Error = anyhow::Error;

	fn try_from(value: proto::With) -> Result<Self, Self::Error> {
		if value.indexes.is_empty() {
			Ok(Self::NoIndex)
		} else {
			Ok(Self::Index(value.indexes.into_iter().map(|s| s.to_string()).collect()))
		}
	}
}

impl TryFrom<crate::expr::With> for proto::With {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::With) -> Result<Self, Self::Error> {
		let indexes = match value {
			crate::expr::With::Index(indexes) => {
				indexes.into_iter().map(|s| s.to_string()).collect()
			}
			crate::expr::With::NoIndex => vec![],
		};
		Ok(Self {
			indexes,
		})
	}
}

impl TryFrom<proto::Start> for crate::expr::Start {
	type Error = anyhow::Error;

	fn try_from(value: proto::Start) -> Result<Self, Self::Error> {
		Ok(Self(Value::Number(Number::Int(value.start as i64))))
	}
}

impl TryFrom<crate::expr::Start> for proto::Start {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::Start) -> Result<Self, Self::Error> {
		let start = match value.0 {
			Value::Number(Number::Int(start)) => start as u64,
			_ => return Err(anyhow::anyhow!("Invalid start value")),
		};
		Ok(Self {
			start,
		})
	}
}

impl TryFrom<proto::Limit> for crate::expr::Limit {
	type Error = anyhow::Error;

	fn try_from(value: proto::Limit) -> Result<Self, Self::Error> {
		Ok(Self(Value::Number(Number::Int(value.limit as i64))))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use rstest::rstest;

	#[rstest]
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
	#[case::duration(Value::Duration(Duration::new(1, 0)))]
	#[case::datetime(Value::Datetime(Datetime(DateTime::<Utc>::from_timestamp(1_000_000_000, 0).unwrap())))]
	#[case::uuid(Value::Uuid(Uuid::new_v4()))]
	#[case::string(Value::Strand(Strand("Hello, World!".to_string())))]
	#[case::bytes(Value::Bytes(crate::expr::Bytes(vec![1, 2, 3, 4, 5])))]
	#[case::thing(Value::Thing(Thing { tb: "test_table".to_string(), id: Id::Number(42) }))] // Example Thing
	#[case::object(Value::Object(Object(BTreeMap::from([("key".to_string(), Value::Strand(Strand("value".to_string())))]))))]
	#[case::array(Value::Array(Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
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
	fn test_flatbuffers_roundtrip(#[case] input: Value) {
		let mut builder = flatbuffers::FlatBufferBuilder::new();
		let input_fb = input.to_fb(&mut builder);
		builder.finish_minimal(input_fb);
		let buf = builder.finished_data();
		let value_fb =
			flatbuffers::root::<proto_fb::Value>(buf).expect("Failed to read FlatBuffer");
		let value = Value::from_fb(value_fb).expect("Failed to convert from FlatBuffer");
		assert_eq!(input, value, "Roundtrip conversion failed for input: {:?}", input);
	}
}
