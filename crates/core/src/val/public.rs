use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Bound;
use std::str::FromStr;

use anyhow::bail;
use surrealdb_types::{
	Array as PublicArray, Bytes as PublicBytes, Datetime as PublicDatetime,
	Duration as PublicDuration, File as PublicFile, Geometry as PublicGeometry,
	Number as PublicNumber, Object as PublicObject, Range as PublicRange,
	RecordId as PublicRecordId, RecordIdKey as PublicRecordIdKey,
	RecordIdKeyRange as PublicRecordIdKeyRange, Regex as PublicRegex, Uuid as PublicUuid,
	Value as PublicValue,
};

use super::{
	Array as InternalArray, Bytes as InternalBytes, Datetime as InternalDatetime,
	Duration as InternalDuration, File as InternalFile, Geometry as InternalGeometry,
	Number as InternalNumber, Object as InternalObject, Range as InternalRange,
	RecordId as InternalRecordId, RecordIdKey as InternalRecordIdKey,
	RecordIdKeyRange as InternalRecordIdKeyRange, Regex as InternalRegex, Uuid as InternalUuid,
	Value as InternalValue,
};
use crate::val::Strand;

impl TryFrom<PublicValue> for InternalValue {
	type Error = anyhow::Error;
	fn try_from(v: PublicValue) -> anyhow::Result<Self> {
		match v {
			PublicValue::None => Ok(InternalValue::None),
			PublicValue::Null => Ok(InternalValue::Null),
			PublicValue::Bool(b) => Ok(InternalValue::Bool(b)),
			PublicValue::Number(n) => Ok(InternalValue::Number(n.into())),
			// TODO: Null byte validity.
			PublicValue::String(s) => Ok(InternalValue::Strand(Strand::new(s).unwrap())),
			PublicValue::Duration(d) => Ok(InternalValue::Duration(d.into())),
			PublicValue::Datetime(d) => Ok(InternalValue::Datetime(d.into())),
			PublicValue::Uuid(u) => Ok(InternalValue::Uuid(u.into())),
			PublicValue::Array(a) => Ok(InternalValue::Array(a.try_into()?)),
			PublicValue::Object(o) => Ok(InternalValue::Object(o.try_into()?)),
			PublicValue::Geometry(g) => Ok(InternalValue::Geometry(g.into())),
			PublicValue::Bytes(b) => Ok(InternalValue::Bytes(b.into())),
			PublicValue::RecordId(r) => Ok(InternalValue::RecordId(r.try_into()?)),
			PublicValue::File(f) => Ok(InternalValue::File(f.into())),
			PublicValue::Range(r) => Ok(InternalValue::Range(Box::new((*r).try_into()?))),
			PublicValue::Regex(r) => Ok(InternalValue::Regex(r.into())),
		}
	}
}

impl TryFrom<InternalValue> for PublicValue {
	type Error = anyhow::Error;
	fn try_from(v: InternalValue) -> anyhow::Result<Self> {
		Ok(match v {
			InternalValue::None => PublicValue::None,
			InternalValue::Null => PublicValue::Null,
			InternalValue::Bool(b) => PublicValue::Bool(b),
			InternalValue::Number(n) => PublicValue::Number(n.into()),
			InternalValue::Strand(s) => PublicValue::String(s.to_string()),
			InternalValue::Duration(d) => PublicValue::Duration(d.into()),
			InternalValue::Datetime(d) => PublicValue::Datetime(d.into()),
			InternalValue::Uuid(u) => PublicValue::Uuid(u.into()),
			InternalValue::Array(a) => PublicValue::Array(a.try_into()?),
			InternalValue::Object(o) => PublicValue::Object(o.try_into()?),
			InternalValue::Geometry(g) => PublicValue::Geometry(g.try_into()?),
			InternalValue::Bytes(b) => PublicValue::Bytes(b.into()),
			InternalValue::RecordId(r) => PublicValue::RecordId(r.try_into()?),
			InternalValue::File(f) => PublicValue::File(f.into()),
			InternalValue::Range(r) => PublicValue::Range(Box::new((*r).try_into()?)),
			InternalValue::Regex(r) => PublicValue::Regex(r.into()),
			_ => bail!("Could not convert internal value of type {:?} to public value", v),
		})
	}
}

impl From<PublicNumber> for InternalNumber {
	fn from(n: PublicNumber) -> Self {
		match n {
			PublicNumber::Int(i) => InternalNumber::Int(i),
			PublicNumber::Float(f) => InternalNumber::Float(f),
			PublicNumber::Decimal(d) => InternalNumber::Decimal(d),
		}
	}
}

impl From<InternalNumber> for PublicNumber {
	fn from(n: InternalNumber) -> Self {
		match n {
			InternalNumber::Int(i) => PublicNumber::Int(i),
			InternalNumber::Float(f) => PublicNumber::Float(f),
			InternalNumber::Decimal(d) => PublicNumber::Decimal(d),
		}
	}
}

impl From<PublicDatetime> for InternalDatetime {
	fn from(d: PublicDatetime) -> Self {
		InternalDatetime(d.0)
	}
}

impl From<InternalDatetime> for PublicDatetime {
	fn from(d: InternalDatetime) -> Self {
		PublicDatetime(d.0)
	}
}

impl From<PublicUuid> for InternalUuid {
	fn from(u: PublicUuid) -> Self {
		InternalUuid(u.0)
	}
}

impl From<InternalUuid> for PublicUuid {
	fn from(u: InternalUuid) -> Self {
		PublicUuid(u.0)
	}
}

impl From<PublicDuration> for InternalDuration {
	fn from(d: PublicDuration) -> Self {
		InternalDuration(d.0)
	}
}

impl From<InternalDuration> for PublicDuration {
	fn from(d: InternalDuration) -> Self {
		PublicDuration(d.0)
	}
}

impl From<PublicBytes> for InternalBytes {
	fn from(b: PublicBytes) -> Self {
		InternalBytes(b.0)
	}
}

impl From<InternalBytes> for PublicBytes {
	fn from(b: InternalBytes) -> Self {
		PublicBytes(b.0)
	}
}

impl From<PublicGeometry> for InternalGeometry {
	fn from(g: PublicGeometry) -> Self {
		match g {
			PublicGeometry::Point(p) => InternalGeometry::Point(p),
			PublicGeometry::Line(l) => InternalGeometry::Line(l),
			PublicGeometry::Polygon(p) => InternalGeometry::Polygon(p),
			PublicGeometry::MultiPoint(mp) => InternalGeometry::MultiPoint(mp),
			PublicGeometry::MultiLine(ml) => InternalGeometry::MultiLine(ml),
			PublicGeometry::MultiPolygon(mp) => InternalGeometry::MultiPolygon(mp),
			PublicGeometry::Collection(c) => {
				InternalGeometry::Collection(c.into_iter().map(|g| g.into()).collect())
			}
		}
	}
}

impl From<InternalGeometry> for PublicGeometry {
	fn from(g: InternalGeometry) -> Self {
		match g {
			InternalGeometry::Point(p) => PublicGeometry::Point(p),
			InternalGeometry::Line(l) => PublicGeometry::Line(l),
			InternalGeometry::Polygon(p) => PublicGeometry::Polygon(p),
			InternalGeometry::MultiPoint(mp) => PublicGeometry::MultiPoint(mp),
			InternalGeometry::MultiLine(ml) => PublicGeometry::MultiLine(ml),
			InternalGeometry::MultiPolygon(mp) => PublicGeometry::MultiPolygon(mp),
			InternalGeometry::Collection(c) => {
				PublicGeometry::Collection(c.into_iter().map(|g| g.into()).collect())
			}
		}
	}
}

impl From<PublicFile> for InternalFile {
	fn from(f: PublicFile) -> Self {
		InternalFile {
			bucket: f.bucket,
			key: f.key,
		}
	}
}

impl From<InternalFile> for PublicFile {
	fn from(f: InternalFile) -> Self {
		PublicFile {
			bucket: f.bucket,
			key: f.key,
		}
	}
}

impl TryFrom<PublicArray> for InternalArray {
	type Error = anyhow::Error;
	fn try_from(a: PublicArray) -> anyhow::Result<Self> {
		Ok(InternalArray(
			a.0.into_iter().map(|v| v.try_into()).collect::<anyhow::Result<Vec<_>>>()?,
		))
	}
}

impl TryFrom<InternalArray> for PublicArray {
	type Error = anyhow::Error;
	fn try_from(a: InternalArray) -> anyhow::Result<Self> {
		Ok(PublicArray(a.0.into_iter().map(|v| v.try_into()).collect::<anyhow::Result<Vec<_>>>()?))
	}
}

impl TryFrom<PublicObject> for InternalObject {
	type Error = anyhow::Error;
	fn try_from(o: PublicObject) -> anyhow::Result<Self> {
		Ok(InternalObject(
			o.0.into_iter()
				.map(|(k, v)| v.try_into().map(|v: InternalValue| (k, v)))
				.collect::<anyhow::Result<BTreeMap<String, InternalValue>>>()?,
		))
	}
}

impl TryFrom<InternalObject> for PublicObject {
	type Error = anyhow::Error;
	fn try_from(o: InternalObject) -> anyhow::Result<Self> {
		Ok(PublicObject(
			o.0.into_iter()
				.map(|(k, v)| v.try_into().map(|v: PublicValue| (k, v)))
				.collect::<anyhow::Result<BTreeMap<String, PublicValue>>>()?,
		))
	}
}

fn convert_bound<A, B>(b: Bound<A>) -> anyhow::Result<Bound<B>>
where
	B: TryFrom<A>,
	<B as TryFrom<A>>::Error: Display + Send + Sync + 'static,
{
	Ok(match b {
		Bound::Included(a) => Bound::Included(a.try_into().map_err(|e| anyhow::anyhow!("{}", e))?),
		Bound::Excluded(a) => Bound::Excluded(a.try_into().map_err(|e| anyhow::anyhow!("{}", e))?),
		Bound::Unbounded => Bound::Unbounded,
	})
}

impl TryFrom<PublicRange> for InternalRange {
	type Error = anyhow::Error;
	fn try_from(r: PublicRange) -> anyhow::Result<Self> {
		Ok(InternalRange {
			start: convert_bound(r.start)?,
			end: convert_bound(r.end)?,
		})
	}
}

impl TryFrom<InternalRange> for PublicRange {
	type Error = anyhow::Error;
	fn try_from(r: InternalRange) -> anyhow::Result<Self> {
		Ok(PublicRange {
			start: convert_bound(r.start)?,
			end: convert_bound(r.end)?,
		})
	}
}

impl TryFrom<PublicRecordId> for InternalRecordId {
	type Error = anyhow::Error;
	fn try_from(r: PublicRecordId) -> anyhow::Result<Self> {
		Ok(InternalRecordId {
			table: r.table,
			key: r.key.try_into()?,
		})
	}
}

impl TryFrom<InternalRecordId> for PublicRecordId {
	type Error = anyhow::Error;
	fn try_from(r: InternalRecordId) -> anyhow::Result<Self> {
		Ok(PublicRecordId {
			table: r.table,
			key: r.key.try_into()?,
		})
	}
}

impl TryFrom<PublicRecordIdKey> for InternalRecordIdKey {
	type Error = anyhow::Error;
	fn try_from(r: PublicRecordIdKey) -> anyhow::Result<Self> {
		match r {
			PublicRecordIdKey::Number(n) => Ok(InternalRecordIdKey::Number(n.into())),
			PublicRecordIdKey::String(s) => Ok(InternalRecordIdKey::String(s.into())),
			PublicRecordIdKey::Uuid(u) => Ok(InternalRecordIdKey::Uuid(u.into())),
			PublicRecordIdKey::Array(a) => Ok(InternalRecordIdKey::Array(a.try_into()?)),
			PublicRecordIdKey::Object(o) => Ok(InternalRecordIdKey::Object(o.try_into()?)),
			PublicRecordIdKey::Range(r) => {
				Ok(InternalRecordIdKey::Range(Box::new((*r).try_into()?)))
			}
		}
	}
}

impl TryFrom<InternalRecordIdKey> for PublicRecordIdKey {
	type Error = anyhow::Error;
	fn try_from(r: InternalRecordIdKey) -> anyhow::Result<Self> {
		match r {
			InternalRecordIdKey::Number(n) => Ok(PublicRecordIdKey::Number(n.into())),
			InternalRecordIdKey::String(s) => Ok(PublicRecordIdKey::String(s.into())),
			InternalRecordIdKey::Uuid(u) => Ok(PublicRecordIdKey::Uuid(u.into())),
			InternalRecordIdKey::Array(a) => Ok(PublicRecordIdKey::Array(a.try_into()?)),
			InternalRecordIdKey::Object(o) => Ok(PublicRecordIdKey::Object(o.try_into()?)),
			InternalRecordIdKey::Range(r) => {
				Ok(PublicRecordIdKey::Range(Box::new((*r).try_into()?)))
			}
		}
	}
}

impl TryFrom<PublicRecordIdKeyRange> for InternalRecordIdKeyRange {
	type Error = anyhow::Error;
	fn try_from(r: PublicRecordIdKeyRange) -> anyhow::Result<Self> {
		Ok(InternalRecordIdKeyRange {
			start: convert_bound(r.start)?,
			end: convert_bound(r.end)?,
		})
	}
}

impl TryFrom<InternalRecordIdKeyRange> for PublicRecordIdKeyRange {
	type Error = anyhow::Error;
	fn try_from(r: InternalRecordIdKeyRange) -> anyhow::Result<Self> {
		Ok(PublicRecordIdKeyRange {
			start: convert_bound(r.start)?,
			end: convert_bound(r.end)?,
		})
	}
}

impl From<PublicRegex> for InternalRegex {
	fn from(r: PublicRegex) -> Self {
		InternalRegex(r.0)
	}
}

impl From<InternalRegex> for PublicRegex {
	fn from(r: InternalRegex) -> Self {
		PublicRegex(r.0)
	}
}
