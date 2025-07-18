// use std::collections::BTreeMap;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::FromStr;

use surrealdb_protocol::proto::prost_types::{
	Duration as DurationProto, Timestamp as TimestampProto,
};

use crate::expr::{Number, Value};
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

impl TryFrom<ValueProto> for crate::expr::Value {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.value else {
			return Ok(crate::expr::Value::None);
		};

		let value = match inner {
			ValueInner::Null(_) => crate::expr::Value::Null,
			ValueInner::Bool(v) => crate::expr::Value::Bool(v),
			ValueInner::Int64(v) => crate::expr::Value::Number(v.into()),
			ValueInner::Uint64(v) => crate::expr::Value::Number(v.into()),
			ValueInner::Float64(v) => crate::expr::Value::Number(v.into()),
			ValueInner::Decimal(v) => {
				crate::expr::Value::Number(crate::expr::Number::Decimal(v.try_into()?))
			}
			ValueInner::String(v) => crate::expr::Value::Strand(v.into()),
			ValueInner::Duration(v) => crate::expr::Value::Duration(v.into()),
			ValueInner::Datetime(v) => crate::expr::Value::Datetime(v.try_into()?),
			ValueInner::Uuid(v) => crate::expr::Value::Uuid(v.try_into()?),
			ValueInner::Array(v) => crate::expr::Value::Array(v.try_into()?),
			ValueInner::Object(v) => crate::expr::Value::Object(v.try_into()?),
			ValueInner::Geometry(v) => crate::expr::Value::Geometry(v.try_into()?),
			ValueInner::Bytes(v) => crate::expr::Value::Bytes(v.into()),
			ValueInner::RecordId(v) => crate::expr::Value::Thing(v.try_into()?),
			ValueInner::File(v) => crate::expr::Value::File(v.into()),
		};

		Ok(value)
	}
}

impl TryFrom<crate::expr::Value> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::Value) -> Result<Self, Self::Error> {
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
				id: Some(thing.id.try_into()?),
				table: thing.tb,
			}),
			Value::File(file) => ValueInner::File(FileProto {
				bucket: file.bucket,
				key: file.key,
			}),
			Value::Idiom(_)
			| Value::Param(_)
			| Value::Function(_)
			| Value::Table(_)
			| Value::Mock(_)
			| Value::Regex(_)
			| Value::Cast(_)
			| Value::Block(_)
			| Value::Range(_)
			| Value::Edges(_)
			| Value::Future(_)
			| Value::Constant(_)
			| Value::Subquery(_)
			| Value::Expression(_)
			| Value::Query(_)
			| Value::Model(_)
			| Value::Closure(_)
			| Value::Refs(_) => {
				return Err(anyhow::anyhow!("Value is not network compatible: {:?}", value));
			}
		};

		Ok(Self {
			value: Some(inner),
		})
	}
}

impl From<crate::expr::Duration> for DurationProto {
	fn from(duration: crate::expr::Duration) -> Self {
		DurationProto {
			seconds: duration.0.as_secs() as i64,
			nanos: duration.0.subsec_nanos() as i32,
		}
	}
}

impl From<DurationProto> for crate::expr::Duration {
	fn from(proto: DurationProto) -> Self {
		crate::expr::Duration(std::time::Duration::from_nanos(
			proto.seconds as u64 * 1_000_000_000 + proto.nanos as u64,
		))
	}
}

impl From<crate::expr::Datetime> for TimestampProto {
	fn from(datetime: crate::expr::Datetime) -> Self {
		TimestampProto {
			seconds: datetime.0.timestamp(),
			nanos: datetime.0.timestamp_subsec_nanos() as i32,
		}
	}
}

impl TryFrom<TimestampProto> for crate::expr::Datetime {
	type Error = anyhow::Error;

	fn try_from(proto: TimestampProto) -> Result<Self, Self::Error> {
		Ok(crate::expr::Datetime(
			chrono::DateTime::from_timestamp(proto.seconds, proto.nanos as u32)
				.context("Invalid timestamp")?,
		))
	}
}

impl From<crate::expr::Bytes> for bytes::Bytes {
	fn from(bytes: crate::expr::Bytes) -> Self {
		bytes.0.into()
	}
}

impl From<bytes::Bytes> for crate::expr::Bytes {
	fn from(bytes: bytes::Bytes) -> Self {
		crate::expr::Bytes(bytes.into())
	}
}

impl TryFrom<UuidProto> for crate::expr::Uuid {
	type Error = uuid::Error;

	fn try_from(proto: UuidProto) -> Result<Self, Self::Error> {
		Ok(crate::expr::Uuid(uuid::Uuid::from_str(&proto.value)?))
	}
}

impl TryFrom<crate::expr::Uuid> for UuidProto {
	type Error = Infallible;

	fn try_from(uuid: crate::expr::Uuid) -> Result<Self, Self::Error> {
		Ok(UuidProto {
			value: uuid.0.to_string(),
		})
	}
}

impl TryFrom<crate::expr::Array> for ArrayProto {
	type Error = anyhow::Error;

	fn try_from(array: crate::expr::Array) -> Result<Self, Self::Error> {
		Ok(ArrayProto {
			values: array.0.into_iter().map(ValueProto::try_from).collect::<Result<Vec<_>, _>>()?,
		})
	}
}

impl TryFrom<ArrayProto> for crate::expr::Array {
	type Error = anyhow::Error;

	fn try_from(proto: ArrayProto) -> Result<Self, Self::Error> {
		Ok(crate::expr::Array(
			proto
				.values
				.into_iter()
				.map(crate::expr::Value::try_from)
				.collect::<Result<Vec<_>, _>>()?,
		))
	}
}

impl TryFrom<ObjectProto> for crate::expr::Object {
	type Error = anyhow::Error;

	fn try_from(proto: ObjectProto) -> Result<Self, Self::Error> {
		let mut object = BTreeMap::new();
		for (key, value) in proto.items {
			object.insert(key, crate::expr::Value::try_from(value)?);
		}
		Ok(crate::expr::Object(object))
	}
}

impl TryFrom<crate::expr::Object> for ObjectProto {
	type Error = anyhow::Error;

	fn try_from(object: crate::expr::Object) -> Result<Self, Self::Error> {
		let mut items = BTreeMap::new();
		for (key, value) in object.0 {
			items.insert(key, ValueProto::try_from(value)?);
		}
		Ok(ObjectProto {
			items,
		})
	}
}

impl TryFrom<GeometryProto> for crate::expr::Geometry {
	type Error = anyhow::Error;

	fn try_from(proto: GeometryProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.geometry else {
			return Err(anyhow::anyhow!("Invalid Geometry: missing value"));
		};

		let geometry = match inner {
			geometry_proto::Geometry::Point(v) => crate::expr::Geometry::Point(v.into()),
			geometry_proto::Geometry::Line(v) => crate::expr::Geometry::Line(v.into()),
			geometry_proto::Geometry::Polygon(v) => crate::expr::Geometry::Polygon(v.try_into()?),
			geometry_proto::Geometry::MultiPoint(v) => crate::expr::Geometry::MultiPoint(v.into()),
			geometry_proto::Geometry::MultiLine(v) => crate::expr::Geometry::MultiLine(v.into()),
			geometry_proto::Geometry::MultiPolygon(v) => {
				crate::expr::Geometry::MultiPolygon(v.try_into()?)
			}
			geometry_proto::Geometry::Collection(v) => {
				crate::expr::Geometry::Collection(v.try_into()?)
			}
		};

		Ok(geometry)
	}
}

impl TryFrom<crate::expr::Geometry> for GeometryProto {
	type Error = anyhow::Error;

	fn try_from(geometry: crate::expr::Geometry) -> Result<Self, Self::Error> {
		let inner = match geometry {
			crate::expr::Geometry::Point(v) => geometry_proto::Geometry::Point(v.into()),
			crate::expr::Geometry::Line(v) => geometry_proto::Geometry::Line(v.into()),
			crate::expr::Geometry::Polygon(v) => geometry_proto::Geometry::Polygon(v.into()),
			crate::expr::Geometry::MultiPoint(v) => geometry_proto::Geometry::MultiPoint(v.into()),
			crate::expr::Geometry::MultiLine(v) => geometry_proto::Geometry::MultiLine(v.into()),
			crate::expr::Geometry::MultiPolygon(v) => {
				geometry_proto::Geometry::MultiPolygon(v.into())
			}
			crate::expr::Geometry::Collection(v) => {
				geometry_proto::Geometry::Collection(v.try_into()?)
			}
		};

		Ok(Self {
			geometry: Some(inner),
		})
	}
}

impl TryFrom<RecordIdProto> for crate::expr::Thing {
	type Error = anyhow::Error;

	fn try_from(proto: RecordIdProto) -> Result<Self, Self::Error> {
		let Some(id) = proto.id else {
			return Err(anyhow::anyhow!("Invalid RecordId: missing id"));
		};
		Ok(Self {
			tb: proto.table,
			id: id.try_into()?,
		})
	}
}

impl TryFrom<crate::expr::Thing> for RecordIdProto {
	type Error = anyhow::Error;

	fn try_from(thing: crate::expr::Thing) -> Result<Self, Self::Error> {
		Ok(Self {
			table: thing.tb,
			id: Some(thing.id.try_into()?),
		})
	}
}

impl From<FileProto> for crate::expr::File {
	fn from(proto: FileProto) -> Self {
		Self {
			bucket: proto.bucket,
			key: proto.key,
		}
	}
}

impl From<crate::expr::File> for FileProto {
	fn from(file: crate::expr::File) -> Self {
		Self {
			bucket: file.bucket,
			key: file.key,
		}
	}
}

impl TryFrom<IdProto> for crate::expr::Id {
	type Error = anyhow::Error;

	fn try_from(proto: IdProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.id else {
			return Err(anyhow::anyhow!("Invalid Id: missing value"));
		};

		Ok(match inner {
			id_proto::Id::Int64(v) => crate::expr::Id::Number(v),
			id_proto::Id::String(v) => crate::expr::Id::String(v),
			id_proto::Id::Uuid(v) => crate::expr::Id::Uuid(v.try_into()?),
			id_proto::Id::Array(v) => crate::expr::Id::Array(v.try_into()?),
		})
	}
}

impl TryFrom<crate::expr::Id> for IdProto {
	type Error = anyhow::Error;

	fn try_from(id: crate::expr::Id) -> Result<Self, Self::Error> {
		let inner = match id {
			crate::expr::Id::Number(v) => id_proto::Id::Int64(v),
			crate::expr::Id::String(v) => id_proto::Id::String(v),
			crate::expr::Id::Uuid(v) => id_proto::Id::Uuid(v.0.into()),
			crate::expr::Id::Array(v) => id_proto::Id::Array(v.try_into()?),
			crate::expr::Id::Generate(v) => {
				return Err(anyhow::anyhow!(
					"Id::Generate is not supported in proto conversion: {v:?}"
				));
			}
			crate::expr::Id::Object(v) => {
				return Err(anyhow::anyhow!(
					"Id::Object is not supported in proto conversion: {v:?}"
				));
			}
			crate::expr::Id::Range(v) => {
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

impl TryFrom<ValueProto> for crate::expr::Cond {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let value = crate::expr::Value::try_from(proto)?;
		Ok(Self(value))
	}
}

impl TryFrom<crate::expr::Cond> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(cond: crate::expr::Cond) -> Result<Self, Self::Error> {
		let value = ValueProto::try_from(cond.0)?;
		Ok(value)
	}
}

impl TryFrom<ValueProto> for crate::expr::Version {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let value = crate::expr::Value::try_from(proto)?;
		Ok(crate::expr::Version(value))
	}
}

impl TryFrom<crate::expr::Version> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(version: crate::expr::Version) -> Result<Self, Self::Error> {
		let value = ValueProto::try_from(version.0)?;
		Ok(value)
	}
}

impl TryFromValue for crate::expr::Value {
	#[inline]
	fn try_from_value(value: ValueProto) -> Result<Self> {
		crate::expr::Value::try_from(value)
	}
}

impl PartialEq<crate::expr::Value> for ValueProto {
	fn eq(&self, other: &crate::expr::Value) -> bool {
		match crate::expr::Value::try_from(self.clone()) {
			Ok(value) => &value == other,
			Err(_) => false,
		}
	}
}

impl PartialEq<ValueProto> for crate::expr::Value {
	fn eq(&self, other: &ValueProto) -> bool {
		match ValueProto::try_from(self.clone()) {
			Ok(value) => &value == other,
			Err(_) => false,
		}
	}
}

impl PartialEq<crate::sql::SqlValue> for ValueProto {
	fn eq(&self, other: &crate::sql::SqlValue) -> bool {
		let expr_value = crate::expr::Value::try_from(self.clone()).unwrap();
		crate::sql::SqlValue::from(expr_value) == *other
	}
}

impl PartialEq<ValueProto> for crate::sql::SqlValue {
	fn eq(&self, other: &ValueProto) -> bool {
		let expr_value = crate::expr::Value::try_from(other.clone()).unwrap();
		*self == crate::sql::SqlValue::from(expr_value)
	}
}
