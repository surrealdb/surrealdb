// use std::collections::BTreeMap;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::FromStr;

use surrealdb_protocol::proto::prost_types::{
	Duration as DurationProto, Timestamp as TimestampProto,
};

use crate::val::{Number, Value};
use anyhow::Context;
use anyhow::Result;
use surrealdb_protocol::TryFromValue;
use surrealdb_protocol::proto::v1::value::Value as ValueInner;
use surrealdb_protocol::proto::v1::{
	Array as ArrayProto, File as FileProto, Geometry as GeometryProto, Id as IdProto,
	NullValue as NullValueProto, Object as ObjectProto, RecordId as RecordIdProto,
	Uuid as UuidProto, Value as ValueProto, geometry as geometry_proto, id as id_proto,
	value as value_proto,
};

impl TryFrom<ValueProto> for Value {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.value else {
			return Ok(Value::None);
		};

		let value = match inner {
			ValueInner::Null(_) => Value::Null,
			ValueInner::Bool(v) => Value::Bool(v),
			ValueInner::Int64(v) => Value::Number(v.into()),
			ValueInner::Uint64(v) => Value::Number(v.into()),
			ValueInner::Float64(v) => Value::Number(v.into()),
			ValueInner::Decimal(v) => Value::Number(Number::Decimal(v.try_into()?)),
			ValueInner::String(v) => Value::Strand(v.into()),
			ValueInner::Duration(v) => Value::Duration(v.into()),
			ValueInner::Datetime(v) => Value::Datetime(v.try_into()?),
			ValueInner::Uuid(v) => Value::Uuid(v.try_into()?),
			ValueInner::Array(v) => Value::Array(v.try_into()?),
			ValueInner::Object(v) => Value::Object(v.try_into()?),
			ValueInner::Geometry(v) => Value::Geometry(v.try_into()?),
			ValueInner::Bytes(v) => Value::Bytes(v.into()),
			ValueInner::RecordId(v) => Value::Thing(v.try_into()?),
			ValueInner::File(v) => Value::File(v.into()),
		};

		Ok(value)
	}
}

impl TryFrom<Value> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		use value_proto::Value as ValueInner;
		let inner = match value {
			// These value types are simple values which
			// can be used in query responses sent to
			// the client.
			Value::None => {
				return Ok(Self {
					value: None,
				});
			}
			Value::Null => ValueInner::Null(NullValueProto {}),
			Value::Bool(boolean) => ValueInner::Bool(boolean),
			Value::Number(number) => match number {
				Number::Int(int) => ValueInner::Int64(int),
				Number::Float(float) => ValueInner::Float64(float),
				Number::Decimal(decimal) => ValueInner::Decimal(decimal.try_into()?),
			},
			Value::Strand(strand) => ValueInner::String(strand.into()),
			Value::Duration(duration) => ValueInner::Duration(DurationProto {
				seconds: duration.0.as_secs() as i64,
				nanos: duration.0.subsec_nanos() as i32,
			}),
			Value::Datetime(datetime) => ValueInner::Datetime(TimestampProto {
				seconds: datetime.0.timestamp(),
				nanos: datetime.0.timestamp_subsec_nanos() as i32,
			}),
			Value::Uuid(uuid) => ValueInner::Uuid(uuid.try_into()?),
			Value::Array(array) => ValueInner::Array(array.try_into()?),
			Value::Object(object) => ValueInner::Object(object.try_into()?),
			Value::Geometry(geometry) => ValueInner::Geometry(geometry.try_into()?),
			Value::Bytes(bytes) => ValueInner::Bytes(bytes.0.into()),
			Value::Thing(thing) => ValueInner::RecordId(RecordIdProto {
				id: Some(thing.key.try_into()?),
				table: thing.table,
			}),
			Value::File(file) => ValueInner::File(FileProto {
				bucket: file.bucket,
				key: file.key,
			}),
			Value::Table(_) | Value::Closure(_) | Value::Regex(_) => {
				return Err(anyhow::anyhow!("Value is not network compatible: {:?}", value));
			}
		};

		Ok(Self {
			value: Some(inner),
		})
	}
}

impl From<crate::val::Duration> for DurationProto {
	fn from(duration: crate::val::Duration) -> Self {
		DurationProto {
			seconds: duration.0.as_secs() as i64,
			nanos: duration.0.subsec_nanos() as i32,
		}
	}
}

impl From<DurationProto> for crate::val::Duration {
	fn from(proto: DurationProto) -> Self {
		crate::val::Duration(std::time::Duration::from_nanos(
			proto.seconds as u64 * 1_000_000_000 + proto.nanos as u64,
		))
	}
}

impl From<crate::val::Datetime> for TimestampProto {
	fn from(datetime: crate::val::Datetime) -> Self {
		TimestampProto {
			seconds: datetime.0.timestamp(),
			nanos: datetime.0.timestamp_subsec_nanos() as i32,
		}
	}
}

impl TryFrom<TimestampProto> for crate::val::Datetime {
	type Error = anyhow::Error;

	fn try_from(proto: TimestampProto) -> Result<Self, Self::Error> {
		Ok(crate::val::Datetime(
			chrono::DateTime::from_timestamp(proto.seconds, proto.nanos as u32)
				.context("Invalid timestamp")?,
		))
	}
}

impl TryFrom<UuidProto> for crate::val::Uuid {
	type Error = uuid::Error;

	fn try_from(proto: UuidProto) -> Result<Self, Self::Error> {
		Ok(crate::val::Uuid(uuid::Uuid::from_str(&proto.value)?))
	}
}

impl TryFrom<crate::val::Uuid> for UuidProto {
	type Error = Infallible;

	fn try_from(uuid: crate::val::Uuid) -> Result<Self, Self::Error> {
		Ok(UuidProto {
			value: uuid.0.to_string(),
		})
	}
}

impl TryFrom<crate::val::Array> for ArrayProto {
	type Error = anyhow::Error;

	fn try_from(array: crate::val::Array) -> Result<Self, Self::Error> {
		Ok(ArrayProto {
			values: array.0.into_iter().map(ValueProto::try_from).collect::<Result<Vec<_>, _>>()?,
		})
	}
}

impl TryFrom<ArrayProto> for crate::val::Array {
	type Error = anyhow::Error;

	fn try_from(proto: ArrayProto) -> Result<Self, Self::Error> {
		Ok(crate::val::Array(
			proto.values.into_iter().map(Value::try_from).collect::<Result<Vec<_>, _>>()?,
		))
	}
}

impl TryFrom<ObjectProto> for crate::val::Object {
	type Error = anyhow::Error;

	fn try_from(proto: ObjectProto) -> Result<Self, Self::Error> {
		let mut object = BTreeMap::new();
		for (key, value) in proto.items {
			object.insert(key, Value::try_from(value)?);
		}
		Ok(crate::val::Object(object))
	}
}

impl TryFrom<crate::val::Object> for ObjectProto {
	type Error = anyhow::Error;

	fn try_from(object: crate::val::Object) -> Result<Self, Self::Error> {
		let mut items = BTreeMap::new();
		for (key, value) in object.0 {
			items.insert(key, ValueProto::try_from(value)?);
		}
		Ok(ObjectProto {
			items,
		})
	}
}

impl TryFrom<GeometryProto> for crate::val::Geometry {
	type Error = anyhow::Error;

	fn try_from(proto: GeometryProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.geometry else {
			return Err(anyhow::anyhow!("Invalid Geometry: missing value"));
		};

		let geometry = match inner {
			geometry_proto::Geometry::Point(v) => crate::val::Geometry::Point(v.into()),
			geometry_proto::Geometry::Line(v) => crate::val::Geometry::Line(v.into()),
			geometry_proto::Geometry::Polygon(v) => crate::val::Geometry::Polygon(v.try_into()?),
			geometry_proto::Geometry::MultiPoint(v) => crate::val::Geometry::MultiPoint(v.into()),
			geometry_proto::Geometry::MultiLine(v) => crate::val::Geometry::MultiLine(v.into()),
			geometry_proto::Geometry::MultiPolygon(v) => {
				crate::val::Geometry::MultiPolygon(v.try_into()?)
			}
			geometry_proto::Geometry::Collection(v) => {
				crate::val::Geometry::Collection(v.try_into()?)
			}
		};

		Ok(geometry)
	}
}

impl TryFrom<crate::val::Geometry> for GeometryProto {
	type Error = anyhow::Error;

	fn try_from(geometry: crate::val::Geometry) -> Result<Self, Self::Error> {
		let inner = match geometry {
			crate::val::Geometry::Point(v) => geometry_proto::Geometry::Point(v.into()),
			crate::val::Geometry::Line(v) => geometry_proto::Geometry::Line(v.into()),
			crate::val::Geometry::Polygon(v) => geometry_proto::Geometry::Polygon(v.into()),
			crate::val::Geometry::MultiPoint(v) => geometry_proto::Geometry::MultiPoint(v.into()),
			crate::val::Geometry::MultiLine(v) => geometry_proto::Geometry::MultiLine(v.into()),
			crate::val::Geometry::MultiPolygon(v) => {
				geometry_proto::Geometry::MultiPolygon(v.into())
			}
			crate::val::Geometry::Collection(v) => {
				geometry_proto::Geometry::Collection(v.try_into()?)
			}
		};

		Ok(Self {
			geometry: Some(inner),
		})
	}
}

impl TryFrom<RecordIdProto> for crate::val::RecordId {
	type Error = anyhow::Error;

	fn try_from(proto: RecordIdProto) -> Result<Self, Self::Error> {
		let Some(id) = proto.id else {
			return Err(anyhow::anyhow!("Invalid RecordId: missing id"));
		};
		Ok(Self {
			table: proto.table,
			key: id.try_into()?,
		})
	}
}

impl TryFrom<crate::val::RecordId> for RecordIdProto {
	type Error = anyhow::Error;

	fn try_from(recordid: crate::val::RecordId) -> Result<Self, Self::Error> {
		Ok(Self {
			table: recordid.table,
			id: Some(recordid.key.try_into()?),
		})
	}
}

impl From<FileProto> for crate::val::File {
	fn from(proto: FileProto) -> Self {
		Self {
			bucket: proto.bucket,
			key: proto.key,
		}
	}
}

impl From<crate::val::File> for FileProto {
	fn from(file: crate::val::File) -> Self {
		Self {
			bucket: file.bucket,
			key: file.key,
		}
	}
}

impl TryFrom<IdProto> for crate::val::RecordIdKey {
	type Error = anyhow::Error;

	fn try_from(proto: IdProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.id else {
			return Err(anyhow::anyhow!("Invalid Id: missing value"));
		};

		Ok(match inner {
			id_proto::Id::Int64(v) => crate::val::RecordIdKey::Number(v),
			id_proto::Id::String(v) => crate::val::RecordIdKey::String(v),
			id_proto::Id::Uuid(v) => crate::val::RecordIdKey::Uuid(v.try_into()?),
			id_proto::Id::Array(v) => crate::val::RecordIdKey::Array(v.try_into()?),
		})
	}
}

impl TryFrom<crate::val::RecordIdKey> for IdProto {
	type Error = anyhow::Error;

	fn try_from(id: crate::val::RecordIdKey) -> Result<Self, Self::Error> {
		let inner = match id {
			crate::val::RecordIdKey::Number(v) => id_proto::Id::Int64(v),
			crate::val::RecordIdKey::String(v) => id_proto::Id::String(v),
			crate::val::RecordIdKey::Uuid(v) => id_proto::Id::Uuid(v.0.into()),
			crate::val::RecordIdKey::Array(v) => id_proto::Id::Array(v.try_into()?),
			crate::val::RecordIdKey::Object(v) => {
				return Err(anyhow::anyhow!(
					"Id::Object is not supported in proto conversion: {v:?}"
				));
			}
			crate::val::RecordIdKey::Range(v) => {
				return Err(anyhow::anyhow!(
					"Id::Range is not supported in proto conversion: {v:?}"
				));
			}
		};

		Ok(Self {
			id: Some(inner),
		})
	}
}

/*
impl TryFrom<ValueProto> for crate::val::Cond {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let value = Value::try_from(proto)?;
		Ok(Self(value))
	}
}

impl TryFrom<crate::val::Cond> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(cond: crate::val::Cond) -> Result<Self, Self::Error> {
		let value = ValueProto::try_from(cond.0)?;
		Ok(value)
	}
}

impl TryFrom<ValueProto> for Version {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let value = Value::try_from(proto)?;
		Ok(Version(value))
	}
}

impl TryFrom<Version> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(version: Version) -> Result<Self, Self::Error> {
		let value = ValueProto::try_from(version.0)?;
		Ok(value)
	}
}
*/

impl TryFromValue for Value {
	#[inline]
	fn try_from_value(value: ValueProto) -> Result<Self> {
		Value::try_from(value)
	}
}

impl PartialEq<Value> for ValueProto {
	fn eq(&self, other: &Value) -> bool {
		match Value::try_from(self.clone()) {
			Ok(value) => &value == other,
			Err(_) => false,
		}
	}
}

impl PartialEq<ValueProto> for Value {
	fn eq(&self, other: &ValueProto) -> bool {
		match ValueProto::try_from(self.clone()) {
			Ok(value) => &value == other,
			Err(_) => false,
		}
	}
}
