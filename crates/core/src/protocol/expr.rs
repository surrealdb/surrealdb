use crate::dbs::Variables;
use crate::expr::graph::{GraphSubject, GraphSubjects};
use crate::expr::order::{OrderList, Ordering};
use crate::expr::part::{DestructurePart, Recurse, RecurseInstruction};
use crate::protocol::{FromCapnp, FromFlatbuffers, ToCapnp, ToFlatbuffers};

use crate::expr::{
	self, Array, Cond, Data, Datetime, Dir, Duration, Fetch, Fetchs, Field, Fields, File, Geometry,
	Graph, Group, Groups, Id, IdRange, Ident, Idiom, Limit, Number, Object, Operator, Order, Part,
	Split, Splits, Start, Strand, Table, Thing, Uuid, Value, table,
};
use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use core::panic;
use geo::Point;
use num_traits::AsPrimitive;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::protocol::flatbuffers::surreal_db::protocol::common as common_fb;
use crate::protocol::flatbuffers::surreal_db::protocol::expr::{self as expr_fb, FileArgs};

impl ToFlatbuffers for Value {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Null => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Null,
				value: Some(
					expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {})
						.as_union_value(),
				),
			},
			Self::Bool(b) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Bool,
				value: Some(
					expr_fb::BoolValue::create(
						builder,
						&expr_fb::BoolValueArgs {
							value: *b,
						},
					)
					.as_union_value(),
				),
			},
			Self::Number(n) => match n {
				crate::expr::Number::Int(i) => expr_fb::ValueArgs {
					value_type: expr_fb::ValueType::Int64,
					value: Some(
						expr_fb::Int64Value::create(
							builder,
							&expr_fb::Int64ValueArgs {
								value: *i,
							},
						)
						.as_union_value(),
					),
				},
				crate::expr::Number::Float(f) => expr_fb::ValueArgs {
					value_type: expr_fb::ValueType::Float64,
					value: Some(
						expr_fb::Float64Value::create(
							builder,
							&expr_fb::Float64ValueArgs {
								value: *f,
							},
						)
						.as_union_value(),
					),
				},
				crate::expr::Number::Decimal(d) => expr_fb::ValueArgs {
					value_type: expr_fb::ValueType::Decimal,
					value: Some(d.to_fb(builder).as_union_value()),
				},
			},
			Self::Strand(s) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::String,
				value: Some(s.to_fb(builder).as_union_value()),
			},
			Self::Bytes(b) => {
				let bytes = builder.create_vector(b.as_slice());
				expr_fb::ValueArgs {
					value_type: expr_fb::ValueType::Bytes,
					value: Some(
						common_fb::Bytes::create(
							builder,
							&common_fb::BytesArgs {
								value: Some(bytes),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Thing(thing) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::RecordId,
				value: Some(thing.to_fb(builder).as_union_value()),
			},
			Self::Duration(d) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Duration,
				value: Some(d.to_fb(builder).as_union_value()),
			},
			Self::Datetime(dt) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Timestamp,
				value: Some(dt.to_fb(builder).as_union_value()),
			},
			Self::Uuid(uuid) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Uuid,
				value: Some(uuid.to_fb(builder).as_union_value()),
			},
			Self::Object(obj) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Object,
				value: Some(obj.to_fb(builder).as_union_value()),
			},
			Self::Array(arr) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Array,
				value: Some(arr.to_fb(builder).as_union_value()),
			},
			Self::Geometry(geometry) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::Geometry,
				value: Some(geometry.to_fb(builder).as_union_value()),
			},
			Self::File(file) => expr_fb::ValueArgs {
				value_type: expr_fb::ValueType::File,
				value: Some(file.to_fb(builder).as_union_value()),
			},
			_ => {
				// TODO: DO NOT PANIC, we just need to modify the Value enum which Mees is currently working on.
				panic!("Unsupported value type for Flatbuffers serialization: {:?}", self);
			}
		};

		expr_fb::Value::create(builder, &args)
	}
}

impl FromFlatbuffers for Value {
	type Input<'a> = expr_fb::Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.value_type() {
			expr_fb::ValueType::Null => Ok(Value::Null),
			expr_fb::ValueType::Bool => {
				Ok(Value::Bool(input.value_as_bool().expect("Guaranteed to be a Bool").value()))
			}
			expr_fb::ValueType::Int64 => Ok(Value::Number(Number::Int(
				input.value_as_int_64().expect("Guaranteed to be an Int64").value(),
			))),
			expr_fb::ValueType::Float64 => Ok(Value::Number(Number::Float(
				input.value_as_float_64().expect("Guaranteed to be a Float64").value(),
			))),
			expr_fb::ValueType::Decimal => {
				let decimal_value = input.value_as_decimal().expect("Guaranteed to be a Decimal");
				let decimal = decimal_value
					.value()
					.expect("Decimal value is guaranteed to be present")
					.parse::<Decimal>()
					.map_err(|_| anyhow!("Invalid decimal format"))?;
				Ok(Value::Number(Number::Decimal(decimal)))
			}
			expr_fb::ValueType::String => {
				let string_value = input.value_as_string().expect("Guaranteed to be a String");
				let value = string_value
					.value()
					.expect("String value is guaranteed to be present")
					.to_string();
				Ok(Value::Strand(Strand(value)))
			}
			expr_fb::ValueType::Bytes => {
				let bytes_value = input.value_as_bytes().expect("Guaranteed to be Bytes");
				let value = Vec::<u8>::from_fb(
					bytes_value.value().expect("Bytes value is guaranteed to be present"),
				)?;
				Ok(Value::Bytes(crate::expr::Bytes(value)))
			}
			expr_fb::ValueType::RecordId => {
				let record_id_value =
					input.value_as_record_id().expect("Guaranteed to be a RecordId");
				let thing = Thing::from_fb(record_id_value)?;
				Ok(Value::Thing(thing))
			}
			expr_fb::ValueType::Duration => {
				let duration_value =
					input.value_as_duration().expect("Guaranteed to be a Duration");
				let duration = Duration::from_fb(duration_value)?;
				Ok(Value::Duration(duration))
			}
			expr_fb::ValueType::Timestamp => {
				let timestamp_value =
					input.value_as_timestamp().expect("Guaranteed to be a Timestamp");
				let dt = DateTime::<Utc>::from_fb(timestamp_value)?;
				Ok(Value::Datetime(Datetime(dt)))
			}
			expr_fb::ValueType::Uuid => {
				let uuid_value = input.value_as_uuid().expect("Guaranteed to be a Uuid");
				let uuid = Uuid::from_fb(uuid_value)?;
				Ok(Value::Uuid(uuid))
			}
			expr_fb::ValueType::Object => {
				let object_value = input.value_as_object().expect("Guaranteed to be an Object");
				let object = Object::from_fb(object_value)?;
				Ok(Value::Object(object))
			}
			expr_fb::ValueType::Array => {
				let array_value = input.value_as_array().expect("Guaranteed to be an Array");
				let array = Array::from_fb(array_value)?;
				Ok(Value::Array(array))
			}
			expr_fb::ValueType::Geometry => {
				let geometry_value =
					input.value_as_geometry().expect("Guaranteed to be a Geometry");
				let geometry = Geometry::from_fb(geometry_value)?;
				Ok(Value::Geometry(geometry))
			}
			expr_fb::ValueType::File => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Int64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		expr_fb::Int64Value::create(
			builder,
			&expr_fb::Int64ValueArgs {
				value: *self,
			},
		)
	}
}

impl FromFlatbuffers for i64 {
	type Input<'a> = expr_fb::Int64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for f64 {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Float64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		expr_fb::Float64Value::create(
			builder,
			&expr_fb::Float64ValueArgs {
				value: *self,
			},
		)
	}
}

impl FromFlatbuffers for f64 {
	type Input<'a> = expr_fb::Float64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for String {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::StringValue<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = builder.create_string(self);
		expr_fb::StringValue::create(
			builder,
			&expr_fb::StringValueArgs {
				value: Some(value),
			},
		)
	}
}

impl ToFlatbuffers for Decimal {
	type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Decimal<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = builder.create_string(&self.to_string());
		common_fb::Decimal::create(
			builder,
			&common_fb::DecimalArgs {
				value: Some(value),
			},
		)
	}
}

impl ToFlatbuffers for std::time::Duration {
	type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Duration<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		common_fb::Duration::create(
			builder,
			&common_fb::DurationArgs {
				seconds: self.as_secs(),
				nanos: self.subsec_nanos(),
			},
		)
	}
}

impl FromFlatbuffers for std::time::Duration {
	type Input<'a> = common_fb::Duration<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let seconds = input.seconds();
		let nanos = input.nanos() as u32;
		Ok(std::time::Duration::new(seconds, nanos))
	}
}

impl ToFlatbuffers for Duration {
	type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Duration<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Duration {
	type Input<'a> = common_fb::Duration<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let duration = std::time::Duration::from_fb(input)?;
		Ok(Duration(duration))
	}
}

impl ToFlatbuffers for DateTime<Utc> {
	type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Timestamp<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		common_fb::Timestamp::create(
			builder,
			&common_fb::TimestampArgs {
				seconds: self.timestamp(),
				nanos: self.timestamp_subsec_nanos(),
			},
		)
	}
}

impl FromFlatbuffers for DateTime<Utc> {
	type Input<'a> = common_fb::Timestamp<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let seconds = input.seconds();
		let nanos = input.nanos() as u32;
		DateTime::<Utc>::from_timestamp(seconds, nanos)
			.ok_or_else(|| anyhow::anyhow!("Invalid timestamp format"))
	}
}

impl ToFlatbuffers for Uuid {
	type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Uuid<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let bytes = builder.create_vector(self.as_bytes());
		common_fb::Uuid::create(
			builder,
			&common_fb::UuidArgs {
				bytes: Some(bytes),
			},
		)
	}
}

impl FromFlatbuffers for Uuid {
	type Input<'a> = common_fb::Uuid<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let bytes_vector = input.bytes().ok_or_else(|| anyhow::anyhow!("Missing bytes in Uuid"))?;
		Uuid::from_slice(bytes_vector.bytes()).map_err(|_| anyhow::anyhow!("Invalid UUID format"))
	}
}

impl ToFlatbuffers for Thing {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::RecordId<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let table = builder.create_string(&self.tb);
		let id = self.id.to_fb(builder);
		expr_fb::RecordId::create(
			builder,
			&expr_fb::RecordIdArgs {
				table: Some(table),
				id: Some(id),
			},
		)
	}
}

impl FromFlatbuffers for Thing {
	type Input<'a> = expr_fb::RecordId<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Id<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Id::Number(n) => {
				let id = n.to_fb(builder).as_union_value();
				expr_fb::Id::create(
					builder,
					&expr_fb::IdArgs {
						id_type: expr_fb::IdType::Int64,
						id: Some(id),
					},
				)
			}
			Id::String(s) => {
				let id = s.to_fb(builder).as_union_value();
				expr_fb::Id::create(
					builder,
					&expr_fb::IdArgs {
						id_type: expr_fb::IdType::String,
						id: Some(id),
					},
				)
			}
			Id::Uuid(uuid) => {
				let id = uuid.to_fb(builder).as_union_value();
				expr_fb::Id::create(
					builder,
					&expr_fb::IdArgs {
						id_type: expr_fb::IdType::Uuid,
						id: Some(id),
					},
				)
			}
			Id::Array(arr) => {
				let id = arr.to_fb(builder).as_union_value();
				expr_fb::Id::create(
					builder,
					&expr_fb::IdArgs {
						id_type: expr_fb::IdType::Array,
						id: Some(id),
					},
				)
			}
			_ => panic!("Unsupported Id type for FlatBuffers serialization: {:?}", self),
		}
	}
}

impl FromFlatbuffers for Id {
	type Input<'a> = expr_fb::Id<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.id_type() {
			expr_fb::IdType::Int64 => {
				let id_value =
					input.id_as_int_64().ok_or_else(|| anyhow::anyhow!("Expected Int64 Id"))?;
				Ok(Id::Number(id_value.value()))
			}
			expr_fb::IdType::String => {
				let id_value =
					input.id_as_string().ok_or_else(|| anyhow::anyhow!("Expected String Id"))?;
				Ok(Id::String(
					id_value
						.value()
						.ok_or_else(|| anyhow::anyhow!("Missing String value"))?
						.to_string(),
				))
			}
			expr_fb::IdType::Uuid => {
				let id_value =
					input.id_as_uuid().ok_or_else(|| anyhow::anyhow!("Expected Uuid Id"))?;
				let uuid = Uuid::from_fb(id_value)?;
				Ok(Id::Uuid(uuid))
			}
			expr_fb::IdType::Array => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::File<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let bucket = builder.create_string(&self.bucket);
		let key = builder.create_string(&self.key);
		expr_fb::File::create(
			builder,
			&expr_fb::FileArgs {
				bucket: Some(bucket),
				key: Some(key),
			},
		)
	}
}

impl FromFlatbuffers for File {
	type Input<'a> = expr_fb::File<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Object<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut entries = Vec::with_capacity(self.0.len());
		for (key, value) in &self.0 {
			let key_fb = builder.create_string(key);
			let value_fb = value.to_fb(builder);

			let object_item = expr_fb::KeyValue::create(
				builder,
				&&expr_fb::KeyValueArgs {
					key: Some(key_fb),
					value: Some(value_fb),
				},
			);

			entries.push(object_item);
		}
		let entries_vector = builder.create_vector(&entries);
		expr_fb::Object::create(
			builder,
			&expr_fb::ObjectArgs {
				items: Some(entries_vector),
			},
		)
	}
}

impl FromFlatbuffers for Object {
	type Input<'a> = expr_fb::Object<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Array<'bldr>>;

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
		expr_fb::Array::create(
			builder,
			&expr_fb::ArrayArgs {
				values: Some(values_vector),
			},
		)
	}
}

impl FromFlatbuffers for Array {
	type Input<'a> = expr_fb::Array<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Geometry<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Geometry::Point(point) => {
				let geometry = point.to_fb(builder);
				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::Point,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::Line(line_string) => {
				let geometry = line_string.to_fb(builder);
				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::LineString,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::Polygon(polygon) => {
				let geometry = polygon.to_fb(builder);
				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::Polygon,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::MultiPoint(multi_point) => {
				let geometry = multi_point.to_fb(builder);
				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::MultiPoint,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::MultiLine(multi_line_string) => {
				let geometry = multi_line_string.to_fb(builder);
				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::MultiLineString,
						geometry: Some(geometry.as_union_value()),
					},
				)
			}
			Geometry::MultiPolygon(multi_polygon) => {
				let geometry = multi_polygon.to_fb(builder);
				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::MultiPolygon,
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

				let collection = expr_fb::GeometryCollection::create(
					builder,
					&expr_fb::GeometryCollectionArgs {
						geometries: Some(geometries_vector),
					},
				);

				expr_fb::Geometry::create(
					builder,
					&expr_fb::GeometryArgs {
						geometry_type: expr_fb::GeometryType::Collection,
						geometry: Some(collection.as_union_value()),
					},
				)
			}
		}
	}
}

impl FromFlatbuffers for Geometry {
	type Input<'a> = expr_fb::Geometry<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.geometry_type() {
			expr_fb::GeometryType::Point => {
				let point = input
					.geometry_as_point()
					.ok_or_else(|| anyhow::anyhow!("Expected Point geometry"))?;
				Ok(Geometry::Point(geo::Point::from_fb(point)?))
			}
			expr_fb::GeometryType::LineString => {
				let line_string = input
					.geometry_as_line_string()
					.ok_or_else(|| anyhow::anyhow!("Expected LineString geometry"))?;
				Ok(Geometry::Line(geo::LineString::from_fb(line_string)?))
			}
			expr_fb::GeometryType::Polygon => {
				let polygon = input
					.geometry_as_polygon()
					.ok_or_else(|| anyhow::anyhow!("Expected Polygon geometry"))?;
				Ok(Geometry::Polygon(geo::Polygon::from_fb(polygon)?))
			}
			expr_fb::GeometryType::MultiPoint => {
				let multi_point = input
					.geometry_as_multi_point()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiPoint geometry"))?;
				Ok(Geometry::MultiPoint(geo::MultiPoint::from_fb(multi_point)?))
			}
			expr_fb::GeometryType::MultiLineString => {
				let multi_line_string = input
					.geometry_as_multi_line_string()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiLineString geometry"))?;
				Ok(Geometry::MultiLine(geo::MultiLineString::from_fb(multi_line_string)?))
			}
			expr_fb::GeometryType::MultiPolygon => {
				let multi_polygon = input
					.geometry_as_multi_polygon()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiPolygon geometry"))?;
				Ok(Geometry::MultiPolygon(geo::MultiPolygon::from_fb(multi_polygon)?))
			}
			expr_fb::GeometryType::Collection => {
				let collection = input
					.geometry_as_collection()
					.ok_or_else(|| anyhow::anyhow!("Expected GeometryCollection"))?;
				let geometries_reader = collection.geometries().context("Geometries is not set")?;
				let mut geometries = Vec::with_capacity(geometries_reader.len() as usize);
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Point<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		expr_fb::Point::create(
			builder,
			&expr_fb::PointArgs {
				x: self.x(),
				y: self.y(),
			},
		)
	}
}

impl FromFlatbuffers for geo::Point {
	type Input<'a> = expr_fb::Point<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(geo::Point::new(input.x(), input.y()))
	}
}

impl ToFlatbuffers for geo::Coord {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Point<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		expr_fb::Point::create(
			builder,
			&expr_fb::PointArgs {
				x: self.x,
				y: self.y,
			},
		)
	}
}

impl FromFlatbuffers for geo::Coord {
	type Input<'a> = expr_fb::Point<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(geo::Coord {
			x: input.x(),
			y: input.y(),
		})
	}
}

impl ToFlatbuffers for geo::LineString {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::LineString<'bldr>>;

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
		expr_fb::LineString::create(
			builder,
			&expr_fb::LineStringArgs {
				points: Some(points_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::LineString {
	type Input<'a> = expr_fb::LineString<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Polygon<'bldr>>;

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
		expr_fb::Polygon::create(
			builder,
			&expr_fb::PolygonArgs {
				exterior: Some(exterior),
				interiors: Some(interiors_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::Polygon {
	type Input<'a> = expr_fb::Polygon<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::MultiPoint<'bldr>>;

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
		expr_fb::MultiPoint::create(
			builder,
			&expr_fb::MultiPointArgs {
				points: Some(points_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::MultiPoint {
	type Input<'a> = expr_fb::MultiPoint<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::MultiLineString<'bldr>>;

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
		expr_fb::MultiLineString::create(
			builder,
			&expr_fb::MultiLineStringArgs {
				lines: Some(lines_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::MultiLineString {
	type Input<'a> = expr_fb::MultiLineString<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::MultiPolygon<'bldr>>;

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
		expr_fb::MultiPolygon::create(
			builder,
			&expr_fb::MultiPolygonArgs {
				polygons: Some(polygons_vector),
			},
		)
	}
}

impl FromFlatbuffers for geo::MultiPolygon {
	type Input<'a> = expr_fb::MultiPolygon<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Idiom<'bldr>>;

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
		expr_fb::Idiom::create(
			builder,
			&expr_fb::IdiomArgs {
				parts: Some(parts_vector),
			},
		)
	}
}

impl FromFlatbuffers for Idiom {
	type Input<'a> = expr_fb::Idiom<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Part<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::All => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::All,
					part: Some(null.as_union_value()),
				}
			}
			Self::Flatten => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Flatten,
					part: Some(null.as_union_value()),
				}
			}
			Self::Last => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Last,
					part: Some(null.as_union_value()),
				}
			}
			Self::First => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::First,
					part: Some(null.as_union_value()),
				}
			}
			Self::Field(ident) => {
				let ident = ident.to_fb(builder);
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Field,
					part: Some(ident.as_union_value()),
				}
			}
			Self::Index(index) => {
				let index: i64 = index.as_int();
				let index_value = index.to_fb(builder);
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Index,
					part: Some(index_value.as_union_value()),
				}
			}
			Self::Where(value) => {
				let value_fb = value.to_fb(builder).as_union_value();
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Where,
					part: Some(value_fb),
				}
			}
			Self::Graph(graph) => {
				let graph_fb = graph.to_fb(builder).as_union_value();
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Graph,
					part: Some(graph_fb),
				}
			}
			Self::Value(value) => {
				let value_fb = value.to_fb(builder).as_union_value();
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Value,
					part: Some(value_fb),
				}
			}
			Self::Start(value) => {
				let value_fb = value.to_fb(builder).as_union_value();
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Start,
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

				let method = expr_fb::MethodPart::create(
					builder,
					&expr_fb::MethodPartArgs {
						name: Some(name),
						args: Some(args),
					},
				);

				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Method,
					part: Some(method.as_union_value()),
				}
			}
			Self::Destructure(parts) => {
				let mut parts_vec = Vec::with_capacity(parts.len());
				for part in parts {
					parts_vec.push(part.to_fb(builder));
				}
				let parts = builder.create_vector(&parts_vec);

				let part = expr_fb::DestructureParts::create(
					builder,
					&expr_fb::DestructurePartsArgs {
						parts: Some(parts),
					},
				);

				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Destructure,
					part: Some(part.as_union_value()),
				}
			}
			Self::Optional => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Optional,
					part: Some(null.as_union_value()),
				}
			}
			Self::Recurse(recurse, idiom, instruction) => {
				let spec = recurse.to_fb(builder);
				let idiom = idiom.as_ref().map(|i| i.to_fb(builder));
				let recurse_operation = instruction.as_ref().map(|op| op.to_fb(builder));

				let recurse_fb = expr_fb::RecursePart::create(
					builder,
					&expr_fb::RecursePartArgs {
						spec: Some(spec),
						idiom,
						recurse_operation,
					},
				);

				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Recurse,
					part: Some(recurse_fb.as_union_value()),
				}
			}
			Self::Doc => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::Doc,
					part: Some(null.as_union_value()),
				}
			}
			Self::RepeatRecurse => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::PartArgs {
					part_type: expr_fb::PartType::RepeatRecurse,
					part: Some(null.as_union_value()),
				}
			}
		};

		expr_fb::Part::create(builder, &args)
	}
}

impl FromFlatbuffers for Part {
	type Input<'a> = expr_fb::Part<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.part_type() {
			expr_fb::PartType::All => Ok(Self::All),
			expr_fb::PartType::Flatten => Ok(Self::Flatten),
			expr_fb::PartType::Last => Ok(Self::Last),
			expr_fb::PartType::First => Ok(Self::First),
			expr_fb::PartType::Field => {
				let ident =
					input.part_as_field().ok_or_else(|| anyhow::anyhow!("Expected Field part"))?;
				let ident =
					ident.value().ok_or_else(|| anyhow::anyhow!("Missing value in Field part"))?;
				Ok(Self::Field(Ident(ident.to_string())))
			}
			expr_fb::PartType::Index => {
				let index =
					input.part_as_index().ok_or_else(|| anyhow::anyhow!("Expected Index part"))?;
				let index = index.value();
				Ok(Self::Index(Number::Int(index)))
			}
			expr_fb::PartType::Where => {
				let value =
					input.part_as_where().ok_or_else(|| anyhow::anyhow!("Expected Where part"))?;
				Ok(Self::Where(Value::from_fb(value)?))
			}
			expr_fb::PartType::Graph => {
				let graph =
					input.part_as_graph().ok_or_else(|| anyhow::anyhow!("Expected Graph part"))?;
				Ok(Self::Graph(Graph::from_fb(graph)?))
			}
			expr_fb::PartType::Value => {
				let value =
					input.part_as_value().ok_or_else(|| anyhow::anyhow!("Expected Value part"))?;
				Ok(Self::Value(Value::from_fb(value)?))
			}
			expr_fb::PartType::Start => {
				let value =
					input.part_as_start().ok_or_else(|| anyhow::anyhow!("Expected Start part"))?;
				Ok(Self::Start(Value::from_fb(value)?))
			}
			expr_fb::PartType::Method => {
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
			expr_fb::PartType::Destructure => {
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
			expr_fb::PartType::Optional => Ok(Self::Optional),
			expr_fb::PartType::Recurse => {
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
			expr_fb::PartType::Doc => Ok(Self::Doc),
			expr_fb::PartType::RepeatRecurse => Ok(Self::RepeatRecurse),
			_ => Err(anyhow::anyhow!(
				"Unsupported Part type for FlatBuffers deserialization: {:?}",
				input.part_type()
			)),
		}
	}
}

impl ToFlatbuffers for Ident {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Ident<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = builder.create_string(&self.0);
		expr_fb::Ident::create(
			builder,
			&expr_fb::IdentArgs {
				value: Some(value),
			},
		)
	}
}

impl FromFlatbuffers for Ident {
	type Input<'a> = expr_fb::Ident<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let value = input.value().ok_or_else(|| anyhow::anyhow!("Missing value in Ident"))?;
		Ok(Ident(value.to_string()))
	}
}

impl ToFlatbuffers for Recurse {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::RecurseSpec<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Fixed(fixed) => {
				let fixed_value = expr_fb::FixedSpec::create(
					builder,
					&expr_fb::FixedSpecArgs {
						value: *fixed,
					},
				);

				expr_fb::RecurseSpecArgs {
					spec_type: expr_fb::RecurseSpecType::Fixed,
					spec: Some(fixed_value.as_union_value()),
				}
			}
			Self::Range(start, end) => {
				let range_value = expr_fb::RangeSpec::create(
					builder,
					&expr_fb::RangeSpecArgs {
						start: start.clone(),
						end: end.clone(),
					},
				);

				expr_fb::RecurseSpecArgs {
					spec_type: expr_fb::RecurseSpecType::Range,
					spec: Some(range_value.as_union_value()),
				}
			}
		};

		expr_fb::RecurseSpec::create(builder, &args)
	}
}

impl FromFlatbuffers for Recurse {
	type Input<'a> = expr_fb::RecurseSpec<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.spec_type() {
			expr_fb::RecurseSpecType::Fixed => {
				let fixed =
					input.spec_as_fixed().ok_or_else(|| anyhow::anyhow!("Expected Fixed spec"))?;
				Ok(Self::Fixed(fixed.value()))
			}
			expr_fb::RecurseSpecType::Range => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::RecurseOperation<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Path {
				inclusive,
			} => {
				let operation = expr_fb::RecursePath::create(
					builder,
					&expr_fb::RecursePathArgs {
						inclusive: *inclusive,
					},
				);

				expr_fb::RecurseOperationArgs {
					operation_type: expr_fb::RecurseOperationType::Path,
					operation: Some(operation.as_union_value()),
				}
			}
			Self::Collect {
				inclusive,
			} => {
				let operation = expr_fb::RecurseCollect::create(
					builder,
					&expr_fb::RecurseCollectArgs {
						inclusive: *inclusive,
					},
				);

				expr_fb::RecurseOperationArgs {
					operation_type: expr_fb::RecurseOperationType::Collect,
					operation: Some(operation.as_union_value()),
				}
			}
			Self::Shortest {
				expects,
				inclusive,
			} => {
				let expects_value = expects.to_fb(builder);
				let operation = expr_fb::RecurseShortest::create(
					builder,
					&expr_fb::RecurseShortestArgs {
						expects: Some(expects_value),
						inclusive: *inclusive,
					},
				);

				expr_fb::RecurseOperationArgs {
					operation_type: expr_fb::RecurseOperationType::Shortest,
					operation: Some(operation.as_union_value()),
				}
			}
		};

		expr_fb::RecurseOperation::create(builder, &args)
	}
}

impl FromFlatbuffers for RecurseInstruction {
	type Input<'a> = expr_fb::RecurseOperation<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.operation_type() {
			expr_fb::RecurseOperationType::Path => {
				let path = input
					.operation_as_path()
					.ok_or_else(|| anyhow::anyhow!("Expected Path operation"))?;
				Ok(Self::Path {
					inclusive: path.inclusive(),
				})
			}
			expr_fb::RecurseOperationType::Collect => {
				let collect = input
					.operation_as_collect()
					.ok_or_else(|| anyhow::anyhow!("Expected Collect operation"))?;
				Ok(Self::Collect {
					inclusive: collect.inclusive(),
				})
			}
			expr_fb::RecurseOperationType::Shortest => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::DestructurePart<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::All(ident) => {
				let ident = ident.to_fb(builder);
				expr_fb::DestructurePartArgs {
					part_type: expr_fb::DestructurePartType::All,
					part: Some(ident.as_union_value()),
				}
			}
			Self::Field(ident) => {
				let ident = ident.to_fb(builder);
				expr_fb::DestructurePartArgs {
					part_type: expr_fb::DestructurePartType::Field,
					part: Some(ident.as_union_value()),
				}
			}
			Self::Aliased(ident, idiom) => {
				let value = builder.create_string(&ident.0);
				let alias = idiom.to_fb(builder);
				let alias = expr_fb::Alias::create(
					builder,
					&expr_fb::AliasArgs {
						value: Some(value),
						alias: Some(alias),
					},
				);

				expr_fb::DestructurePartArgs {
					part_type: expr_fb::DestructurePartType::Aliased,
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
				let destructure_ident_parts = expr_fb::DestructureIdentParts::create(
					builder,
					&expr_fb::DestructureIdentPartsArgs {
						name: Some(name),
						parts: Some(parts_vector),
					},
				);
				expr_fb::DestructurePartArgs {
					part_type: expr_fb::DestructurePartType::Destructure,
					part: Some(destructure_ident_parts.as_union_value()),
				}
			}
		};

		expr_fb::DestructurePart::create(builder, &args)
	}
}

impl FromFlatbuffers for DestructurePart {
	type Input<'a> = expr_fb::DestructurePart<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.part_type() {
			expr_fb::DestructurePartType::All => {
				let ident =
					input.part_as_all().ok_or_else(|| anyhow::anyhow!("Expected All part"))?;
				Ok(Self::All(Ident::from_fb(ident)?))
			}
			expr_fb::DestructurePartType::Field => {
				let ident =
					input.part_as_field().ok_or_else(|| anyhow::anyhow!("Expected Field part"))?;
				Ok(Self::Field(Ident::from_fb(ident)?))
			}
			expr_fb::DestructurePartType::Aliased => {
				let alias = input
					.part_as_aliased()
					.ok_or_else(|| anyhow::anyhow!("Expected Aliased part"))?;
				let value = alias.value().context("Missing value in Aliased part")?.to_string();
				let idiom =
					Idiom::from_fb(alias.alias().context("Missing alias in Aliased part")?)?;
				Ok(Self::Aliased(Ident(value), idiom))
			}
			expr_fb::DestructurePartType::Destructure => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Graph<'bldr>>;

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

		expr_fb::Graph::create(
			builder,
			&expr_fb::GraphArgs {
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
	type Input<'a> = expr_fb::Graph<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Splits<'bldr>>;

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
		expr_fb::Splits::create(
			builder,
			&expr_fb::SplitsArgs {
				splits: Some(splits_vector),
			},
		)
	}
}

impl FromFlatbuffers for Splits {
	type Input<'a> = expr_fb::Splits<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Idiom<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Split {
	type Input<'a> = expr_fb::Idiom<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let idiom = Idiom::from_fb(input)?;
		Ok(Self(idiom))
	}
}

impl ToFlatbuffers for Groups {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Groups<'bldr>>;

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
		expr_fb::Groups::create(
			builder,
			&expr_fb::GroupsArgs {
				groups: Some(groups_vector),
			},
		)
	}
}

impl FromFlatbuffers for Groups {
	type Input<'a> = expr_fb::Groups<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Idiom<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Group {
	type Input<'a> = expr_fb::Idiom<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let idiom = Idiom::from_fb(input)?;
		Ok(Self(idiom))
	}
}

impl ToFlatbuffers for Ordering {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::OrderingSpec<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Random => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::OrderingSpecArgs {
					ordering_type: expr_fb::OrderingType::Random,
					ordering: Some(null.as_union_value()),
				}
			}
			Self::Order(order_list) => {
				let order_list = order_list.to_fb(builder);
				expr_fb::OrderingSpecArgs {
					ordering_type: expr_fb::OrderingType::Ordered,
					ordering: Some(order_list.as_union_value()),
				}
			}
		};

		expr_fb::OrderingSpec::create(builder, &args)
	}
}

impl FromFlatbuffers for Ordering {
	type Input<'a> = expr_fb::OrderingSpec<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.ordering_type() {
			expr_fb::OrderingType::Random => Ok(Self::Random),
			expr_fb::OrderingType::Ordered => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::OrderList<'bldr>>;

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
		expr_fb::OrderList::create(
			builder,
			&expr_fb::OrderListArgs {
				orders: Some(orders_vector),
			},
		)
	}
}

impl FromFlatbuffers for OrderList {
	type Input<'a> = expr_fb::OrderList<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Order<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let value = self.value.to_fb(builder);

		expr_fb::Order::create(
			builder,
			&expr_fb::OrderArgs {
				value: Some(value),
				collate: self.collate,
				numeric: self.numeric,
				ascending: self.direction,
			},
		)
	}
}

impl FromFlatbuffers for Order {
	type Input<'a> = expr_fb::Order<'a>;

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
	type Output<'bldr> = expr_fb::GraphDirection;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Dir::In => expr_fb::GraphDirection::In,
			Dir::Out => expr_fb::GraphDirection::Out,
			Dir::Both => expr_fb::GraphDirection::Both,
		}
	}
}

impl FromFlatbuffers for Dir {
	type Input<'a> = expr_fb::GraphDirection;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input {
			expr_fb::GraphDirection::In => Ok(Dir::In),
			expr_fb::GraphDirection::Out => Ok(Dir::Out),
			expr_fb::GraphDirection::Both => Ok(Dir::Both),
			_ => Err(anyhow::anyhow!(
				"Unsupported GraphDirection type for FlatBuffers deserialization: {:?}",
				input
			)),
		}
	}
}

impl ToFlatbuffers for GraphSubjects {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::GraphSubjects<'bldr>>;

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
		expr_fb::GraphSubjects::create(
			builder,
			&expr_fb::GraphSubjectsArgs {
				subjects: Some(subjects_vector),
			},
		)
	}
}

impl FromFlatbuffers for GraphSubjects {
	type Input<'a> = expr_fb::GraphSubjects<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::GraphSubject<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Self::Table(table) => {
				let table = builder.create_string(&table.0);
				let table_fb = expr_fb::Table::create(
					builder,
					&expr_fb::TableArgs {
						name: Some(table),
					},
				);
				expr_fb::GraphSubjectArgs {
					subject_type: expr_fb::GraphSubjectType::Table,
					subject: Some(table_fb.as_union_value()),
				}
			}
			Self::Range(table, id_range) => {
				let table = builder.create_string(&table.0);
				let start = id_range.beg.to_fb(builder);
				let end = id_range.end.to_fb(builder);
				let range_fb = expr_fb::TableIdRange::create(
					builder,
					&expr_fb::TableIdRangeArgs {
						table: Some(table),
						start: Some(start),
						end: Some(end),
					},
				);

				expr_fb::GraphSubjectArgs {
					subject_type: expr_fb::GraphSubjectType::Range,
					subject: Some(range_fb.as_union_value()),
				}
			}
		};

		expr_fb::GraphSubject::create(builder, &args)
	}
}

impl FromFlatbuffers for GraphSubject {
	type Input<'a> = expr_fb::GraphSubject<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.subject_type() {
			expr_fb::GraphSubjectType::Table => {
				let table = input.subject_as_table().context("Expected Table subject")?;
				let name = table.name().context("Missing name in Table subject")?.to_string();
				Ok(GraphSubject::Table(Table(name)))
			}
			expr_fb::GraphSubjectType::Range => {
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::IdBound<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Bound::Included(id) => {
				let id_value = id.to_fb(builder);
				expr_fb::IdBoundArgs {
					id: Some(id_value),
					inclusive: true,
				}
			}
			Bound::Excluded(id) => {
				let id_value = id.to_fb(builder);
				expr_fb::IdBoundArgs {
					id: Some(id_value),
					inclusive: false,
				}
			}
			Bound::Unbounded => expr_fb::IdBoundArgs {
				id: None,
				inclusive: false,
			},
		};

		expr_fb::IdBound::create(builder, &args)
	}
}

impl FromFlatbuffers for Bound<Id> {
	type Input<'a> = expr_fb::IdBound<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Field<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let args = match self {
			Field::All => {
				let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
				expr_fb::FieldArgs {
					field_type: expr_fb::FieldType::All,
					field: Some(null.as_union_value()),
				}
			}
			Field::Single {
				expr,
				alias,
			} => {
				let expr = expr.to_fb(builder);
				let alias = match alias {
					Some(a) => Some(a.to_fb(builder)),
					None => None,
				};
				let single_field = expr_fb::SingleField::create(
					builder,
					&expr_fb::SingleFieldArgs {
						expr: Some(expr),
						alias,
					},
				);

				expr_fb::FieldArgs {
					field_type: expr_fb::FieldType::Single,
					field: Some(single_field.as_union_value()),
				}
			}
		};

		expr_fb::Field::create(builder, &args)
	}
}

impl FromFlatbuffers for Field {
	type Input<'a> = expr_fb::Field<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.field_type() {
			expr_fb::FieldType::All => Ok(Field::All),
			expr_fb::FieldType::Single => {
				let single_field = input.field_as_single().context("Expected SingleField")?;
				let expr =
					Value::from_fb(single_field.expr().context("Missing expr in SingleField")?)?;
				let alias = single_field.alias().map(|a| Idiom::from_fb(a)).transpose()?;
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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Fields<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut fields = Vec::with_capacity(self.0.len());
		for field in &self.0 {
			let args = match field {
				Field::All => {
					let null = expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {});
					expr_fb::FieldArgs {
						field_type: expr_fb::FieldType::All,
						field: Some(null.as_union_value()),
					}
				}
				Field::Single {
					expr,
					alias,
				} => {
					let expr = expr.to_fb(builder);
					let alias = alias.as_ref().map(|a| a.to_fb(builder));
					let single_field = expr_fb::SingleField::create(
						builder,
						&expr_fb::SingleFieldArgs {
							expr: Some(expr),
							alias,
						},
					);
					expr_fb::FieldArgs {
						field_type: expr_fb::FieldType::Single,
						field: Some(single_field.as_union_value()),
					}
				}
			};

			let field_item = expr_fb::Field::create(builder, &args);

			fields.push(field_item);
		}
		let fields_vector = builder.create_vector(&fields);
		expr_fb::Fields::create(
			builder,
			&expr_fb::FieldsArgs {
				single: self.1,
				fields: Some(fields_vector),
			},
		)
	}
}

impl FromFlatbuffers for Fields {
	type Input<'a> = expr_fb::Fields<'a>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Fetch {
	type Input<'a> = expr_fb::Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let value = Value::from_fb(input)?;
		Ok(Fetch(value))
	}
}

impl ToFlatbuffers for Fetchs {
	type Output<'bldr> = flatbuffers::WIPOffset<
		::flatbuffers::Vector<'bldr, ::flatbuffers::ForwardsUOffset<expr_fb::Value<'bldr>>>,
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
	type Input<'a> = flatbuffers::Vector<'a, ::flatbuffers::ForwardsUOffset<expr_fb::Value<'a>>>;

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
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Variables<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let mut vars = Vec::with_capacity(self.len());
		for (key, value) in self.iter() {
			let key_str = builder.create_string(key);
			let value_fb = value.to_fb(builder);
			let var = expr_fb::Variable::create(
				builder,
				&expr_fb::VariableArgs {
					key: Some(key_str),
					value: Some(value_fb),
				},
			);
			vars.push(var);
		}
		let vars_vector = builder.create_vector(&vars);
		expr_fb::Variables::create(
			builder,
			&expr_fb::VariablesArgs {
				items: Some(vars_vector),
			},
		)
	}
}

impl FromFlatbuffers for Variables {
	type Input<'a> = expr_fb::Variables<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let items_reader = input.items().context("Variables is not set")?;
		let mut vars = BTreeMap::new();
		for item in items_reader {
			let key = item.key().context("Missing key in Variable")?.to_string();
			let value = Value::from_fb(item.value().context("Missing value in Variable")?)?;
			vars.insert(key, value);
		}
		Ok(vars)
	}
}

impl ToFlatbuffers for Operator {
	type Output<'bldr> = expr_fb::Operator;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Operator::Neg => expr_fb::Operator::Neg,
			Operator::Not => expr_fb::Operator::Not,
			Operator::Or => expr_fb::Operator::Or,
			Operator::And => expr_fb::Operator::And,
			Operator::Tco => expr_fb::Operator::Tco,
			Operator::Nco => expr_fb::Operator::Nco,
			Operator::Add => expr_fb::Operator::Add,
			Operator::Sub => expr_fb::Operator::Sub,
			Operator::Mul => expr_fb::Operator::Mul,
			Operator::Div => expr_fb::Operator::Div,
			Operator::Rem => expr_fb::Operator::Rem,
			Operator::Pow => expr_fb::Operator::Pow,
			Operator::Inc => expr_fb::Operator::Inc,
			Operator::Dec => expr_fb::Operator::Dec,
			Operator::Ext => expr_fb::Operator::Ext,
			Operator::Equal => expr_fb::Operator::Equal,
			Operator::Exact => expr_fb::Operator::Exact,
			Operator::NotEqual => expr_fb::Operator::NotEqual,
			Operator::AllEqual => expr_fb::Operator::AllEqual,
			Operator::AnyEqual => expr_fb::Operator::AnyEqual,
			Operator::Like => expr_fb::Operator::Like,
			Operator::NotLike => expr_fb::Operator::NotLike,
			Operator::AllLike => expr_fb::Operator::AllLike,
			Operator::AnyLike => expr_fb::Operator::AnyLike,
			Operator::LessThan => expr_fb::Operator::LessThan,
			Operator::LessThanOrEqual => expr_fb::Operator::LessThanOrEqual,
			Operator::MoreThan => expr_fb::Operator::GreaterThan,
			Operator::MoreThanOrEqual => expr_fb::Operator::GreaterThanOrEqual,
			Operator::Contain => expr_fb::Operator::Contain,
			Operator::NotContain => expr_fb::Operator::NotContain,
			Operator::ContainAll => expr_fb::Operator::ContainAll,
			Operator::ContainAny => expr_fb::Operator::ContainAny,
			Operator::ContainNone => expr_fb::Operator::ContainNone,
			Operator::Inside => expr_fb::Operator::Inside,
			Operator::NotInside => expr_fb::Operator::NotInside,
			Operator::AllInside => expr_fb::Operator::AllInside,
			Operator::AnyInside => expr_fb::Operator::AnyInside,
			Operator::NoneInside => expr_fb::Operator::NoneInside,
			Operator::Outside => expr_fb::Operator::Outside,
			Operator::Intersects => expr_fb::Operator::Intersects,
			Operator::AnyInside => expr_fb::Operator::AnyInside,
			Operator::NoneInside => expr_fb::Operator::NoneInside,
			Operator::Knn(_, _) => panic!("KNN operator not supported"),
			Operator::Ann(_, _) => panic!("ANN operator not supported"),
			Operator::Matches(_) => panic!("Matches not supported"),
		}
	}
}

impl FromFlatbuffers for Operator {
	type Input<'a> = expr_fb::Operator;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input {
			expr_fb::Operator::Neg => Ok(Operator::Neg),
			expr_fb::Operator::Not => Ok(Operator::Not),
			expr_fb::Operator::Or => Ok(Operator::Or),
			expr_fb::Operator::And => Ok(Operator::And),
			expr_fb::Operator::Tco => Ok(Operator::Tco),
			expr_fb::Operator::Nco => Ok(Operator::Nco),
			expr_fb::Operator::Add => Ok(Operator::Add),
			expr_fb::Operator::Sub => Ok(Operator::Sub),
			expr_fb::Operator::Mul => Ok(Operator::Mul),
			expr_fb::Operator::Div => Ok(Operator::Div),
			expr_fb::Operator::Rem => Ok(Operator::Rem),
			expr_fb::Operator::Pow => Ok(Operator::Pow),
			expr_fb::Operator::Inc => Ok(Operator::Inc),
			expr_fb::Operator::Dec => Ok(Operator::Dec),
			expr_fb::Operator::Ext => Ok(Operator::Ext),
			expr_fb::Operator::Equal => Ok(Operator::Equal),
			expr_fb::Operator::Exact => Ok(Operator::Exact),
			expr_fb::Operator::NotEqual => Ok(Operator::NotEqual),
			expr_fb::Operator::AllEqual => Ok(Operator::AllEqual),
			expr_fb::Operator::AnyEqual => Ok(Operator::AnyEqual),
			expr_fb::Operator::Like => Ok(Operator::Like),
			expr_fb::Operator::NotLike => Ok(Operator::NotLike),
			expr_fb::Operator::AllLike => Ok(Operator::AllLike),
			expr_fb::Operator::AnyLike => Ok(Operator::AnyLike),
			expr_fb::Operator::LessThan => Ok(Operator::LessThan),
			expr_fb::Operator::LessThanOrEqual => Ok(Operator::LessThanOrEqual),
			expr_fb::Operator::GreaterThan => Ok(Operator::MoreThan),
			expr_fb::Operator::GreaterThanOrEqual => Ok(Operator::MoreThanOrEqual),
			expr_fb::Operator::Contain => Ok(Operator::Contain),
			expr_fb::Operator::NotContain => Ok(Operator::NotContain),
			expr_fb::Operator::ContainAll => Ok(Operator::ContainAll),
			expr_fb::Operator::ContainAny => Ok(Operator::ContainAny),
			expr_fb::Operator::ContainNone => Ok(Operator::ContainNone),
			expr_fb::Operator::Inside => Ok(Operator::Inside),
			expr_fb::Operator::NotInside => Ok(Operator::NotInside),
			expr_fb::Operator::AllInside => Ok(Operator::AllInside),
			expr_fb::Operator::AnyInside => Ok(Operator::AnyInside),
			expr_fb::Operator::NoneInside => Ok(Operator::NoneInside),
			expr_fb::Operator::Outside => Ok(Operator::Outside),
			expr_fb::Operator::Intersects => Ok(Operator::Intersects),
			expr_fb::Operator::AnyInside => Ok(Operator::AnyInside),
			expr_fb::Operator::NoneInside => Ok(Operator::NoneInside),
			_ => Err(anyhow::anyhow!("Invalid operator: {:?}", input)),
		}
	}
}

impl ToFlatbuffers for Data {
	type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Data<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let (contents_type, contents) = match self {
			Data::EmptyExpression => (
				expr_fb::DataContents::Empty,
				expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {}).as_union_value(),
			),
			Data::SetExpression(set) => {
				let mut items = Vec::with_capacity(set.len());
				for (idiom, operator, value) in set {
					let idiom_fb = idiom.to_fb(builder);
					let operator_fb = operator.to_fb(builder);
					let value_fb = value.to_fb(builder);
					items.push(expr_fb::SetExpr::create(
						builder,
						&expr_fb::SetExprArgs {
							idiom: Some(idiom_fb),
							operator: operator_fb,
							value: Some(value_fb),
						},
					));
				}
				let set_exprs = builder.create_vector(&items);
				(
					expr_fb::DataContents::Set,
					expr_fb::SetMultiExpr::create(
						builder,
						&expr_fb::SetMultiExprArgs {
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
					expr_fb::DataContents::Unset,
					expr_fb::UnsetMultiExpr::create(
						builder,
						&expr_fb::UnsetMultiExprArgs {
							items: Some(unset_exprs),
						},
					)
					.as_union_value(),
				)
			}
			Data::PatchExpression(patch) => {
				let patch_fb = patch.to_fb(builder);
				(expr_fb::DataContents::Patch, patch_fb.as_union_value())
			}
			Data::MergeExpression(merge) => {
				let merge_fb = merge.to_fb(builder);
				(expr_fb::DataContents::Merge, merge_fb.as_union_value())
			}
			Data::ReplaceExpression(replace) => {
				let replace_fb = replace.to_fb(builder);
				(expr_fb::DataContents::Replace, replace_fb.as_union_value())
			}
			Data::ContentExpression(content) => {
				let content_fb = content.to_fb(builder);
				(expr_fb::DataContents::Content, content_fb.as_union_value())
			}
			Data::SingleExpression(single) => {
				let single_fb = single.to_fb(builder);
				(expr_fb::DataContents::Value, single_fb.as_union_value())
			}
			Data::ValuesExpression(values) => {
				todo!("STU")
				// let items = Vec::with_capacity(values.len());
				// for value in values {
				// 	let idiom_fb = idiom.to_fb
				// 	items.push(value_fb.as_union_value());
				// }
				// let values_fb = builder.create_vector(&items);
				// (
				// 	expr_fb::DataContents::Values,
				// 	values_fb.as_union_value(),
				// )
			}
			Data::UpdateExpression(update) => {
				let mut items = Vec::with_capacity(update.len());
				for (idiom, operator, value) in update {
					let idiom_fb = idiom.to_fb(builder);
					let operator_fb = operator.to_fb(builder);
					let value_fb = value.to_fb(builder);
					items.push(expr_fb::SetExpr::create(
						builder,
						&expr_fb::SetExprArgs {
							idiom: Some(idiom_fb),
							operator: operator_fb,
							value: Some(value_fb),
						},
					));
				}
				let update_exprs = builder.create_vector(&items);
				(
					expr_fb::DataContents::Update,
					expr_fb::SetMultiExpr::create(
						builder,
						&expr_fb::SetMultiExprArgs {
							items: Some(update_exprs),
						},
					)
					.as_union_value(),
				)
			}
		};

		expr_fb::Data::create(
			builder,
			&expr_fb::DataArgs {
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
	type Input<'a> = expr_fb::Data<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.contents_type() {
			expr_fb::DataContents::Empty => Ok(Data::EmptyExpression),
			expr_fb::DataContents::Set => {
				let params = input.contents_as_set().context("Missing set")?;
				Ok(Data::SetExpression(SetMultiExpr::from_fb(params)?))
			}
			expr_fb::DataContents::Unset => {
				let params = input.contents_as_unset().context("Missing unset")?;
				Ok(Data::UnsetExpression(UnsetMultiExpr::from_fb(params)?))
			}
			expr_fb::DataContents::Patch => {
				let params = input.contents_as_patch().context("Missing patch")?;
				Ok(Data::PatchExpression(Value::from_fb(params)?))
			}
			expr_fb::DataContents::Merge => {
				let params = input.contents_as_merge().context("Missing merge")?;
				Ok(Data::MergeExpression(Value::from_fb(params)?))
			}
			expr_fb::DataContents::Replace => {
				let params = input.contents_as_replace().context("Missing replace")?;
				Ok(Data::ReplaceExpression(Value::from_fb(params)?))
			}
			expr_fb::DataContents::Content => {
				let params = input.contents_as_content().context("Missing content")?;
				Ok(Data::ContentExpression(Value::from_fb(params)?))
			}
			expr_fb::DataContents::Value => {
				let params = input.contents_as_value().context("Missing value")?;
				Ok(Data::SingleExpression(Value::from_fb(params)?))
			}
			expr_fb::DataContents::Values => {
				let params = input.contents_as_values().context("Missing values")?;
				Ok(Data::ValuesExpression(ValuesExpr::from_fb(params)?))
			}
			expr_fb::DataContents::Update => {
				let params = input.contents_as_update().context("Missing update")?;
				Ok(Data::UpdateExpression(SetMultiExpr::from_fb(params)?))
			}
			unexpected => {
				return Err(anyhow::anyhow!("Unexpected data contents: {:?}", unexpected));
			}
		}
	}
}

impl FromFlatbuffers for SetMultiExpr {
	type Input<'a> = expr_fb::SetMultiExpr<'a>;

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
	type Output<'a> = flatbuffers::WIPOffset<expr_fb::SetExpr<'a>>;

	#[inline]
	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let idiom = self.0.to_fb(fbb);
		let operator = self.1.to_fb(fbb);
		let value = self.2.to_fb(fbb);
		expr_fb::SetExpr::create(
			fbb,
			&expr_fb::SetExprArgs {
				idiom: Some(idiom),
				operator,
				value: Some(value),
			},
		)
	}
}

impl FromFlatbuffers for SetExpr {
	type Input<'a> = expr_fb::SetExpr<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let idiom = Idiom::from_fb(input.idiom().context("Missing idiom")?)?;
		let operator = Operator::from_fb(input.operator())?;
		let value = Value::from_fb(input.value().context("Missing value")?)?;
		Ok((idiom, operator, value))
	}
}

impl ToFlatbuffers for SetMultiExpr {
	type Output<'a> = flatbuffers::WIPOffset<expr_fb::SetMultiExpr<'a>>;

	#[inline]
	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let items = self.iter().map(|v| v.to_fb(fbb)).collect::<Vec<_>>();
		let items = fbb.create_vector(&items);
		expr_fb::SetMultiExpr::create(
			fbb,
			&expr_fb::SetMultiExprArgs {
				items: Some(items),
			},
		)
	}
}

impl FromFlatbuffers for UnsetMultiExpr {
	type Input<'a> = expr_fb::UnsetMultiExpr<'a>;

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
	type Input<'a> = expr_fb::ValuesMultiExpr<'a>;

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
		let value_fb = flatbuffers::root::<expr_fb::Value>(buf).expect("Failed to read FlatBuffer");
		let value = Value::from_fb(value_fb).expect("Failed to convert from FlatBuffer");
		assert_eq!(input, value, "Roundtrip conversion failed for input: {:?}", input);
	}
}
