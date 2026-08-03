use std::ops::Bound;

use anyhow::anyhow;
use surrealdb_protocol::proto::v1 as proto;
use surrealdb_protocol::proto::v1::record_id_key::Id as RecordIdKeyId;
use surrealdb_protocol::proto::v1::record_id_key_bound::Bound as RecordIdKeyBoundInner;

use crate::{Array, Object, RecordId, RecordIdKey, RecordIdKeyRange, Table, Uuid};

impl TryFrom<RecordId> for proto::RecordId {
	type Error = anyhow::Error;

	fn try_from(value: RecordId) -> Result<Self, Self::Error> {
		Ok(proto::RecordId::new(value.table.into_string(), Some(value.key.try_into()?)))
	}
}

impl TryFrom<proto::RecordId> for RecordId {
	type Error = anyhow::Error;

	fn try_from(value: proto::RecordId) -> Result<Self, Self::Error> {
		let key = value.id.ok_or_else(|| anyhow!("Missing id in RecordId"))?;
		Ok(RecordId {
			table: Table::new(value.table),
			key: RecordIdKey::try_from(key)?,
		})
	}
}

impl TryFrom<RecordIdKey> for proto::RecordIdKey {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self, Self::Error> {
		let id = match value {
			RecordIdKey::Number(n) => RecordIdKeyId::Int64(n),
			RecordIdKey::String(s) => RecordIdKeyId::String(s),
			RecordIdKey::Uuid(u) => RecordIdKeyId::Uuid(u.0.into()),
			RecordIdKey::Array(a) => RecordIdKeyId::Array(a.try_into()?),
			RecordIdKey::Object(o) => RecordIdKeyId::Object(o.try_into()?),
			RecordIdKey::Range(r) => RecordIdKeyId::Range(Box::new((*r).try_into()?)),
		};
		Ok(proto::RecordIdKey {
			id: Some(id),
		})
	}
}

impl TryFrom<proto::RecordIdKey> for RecordIdKey {
	type Error = anyhow::Error;

	fn try_from(value: proto::RecordIdKey) -> Result<Self, Self::Error> {
		let id = value.id.ok_or_else(|| anyhow!("Missing id in RecordIdKey"))?;
		match id {
			RecordIdKeyId::Int64(n) => Ok(RecordIdKey::Number(n)),
			RecordIdKeyId::String(s) => Ok(RecordIdKey::String(s)),
			RecordIdKeyId::Uuid(u) => Ok(RecordIdKey::Uuid(Uuid(u.to_uuid()?))),
			RecordIdKeyId::Array(a) => Ok(RecordIdKey::Array(Array::try_from(a)?)),
			RecordIdKeyId::Object(o) => Ok(RecordIdKey::Object(Object::try_from(o)?)),
			RecordIdKeyId::Range(r) => {
				Ok(RecordIdKey::Range(Box::new(RecordIdKeyRange::try_from(*r)?)))
			}
			// Neither variant is representable: `RecordIdKey` has no numeric
			// key besides `Number(i64)`.
			RecordIdKeyId::Float64(_) => {
				Err(anyhow!("Unsupported RecordIdKey type: a floating-point record id key"))
			}
			RecordIdKeyId::Decimal(_) => {
				Err(anyhow!("Unsupported RecordIdKey type: a decimal record id key"))
			}
		}
	}
}

impl TryFrom<RecordIdKeyRange> for proto::RecordIdKeyRange {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKeyRange) -> Result<Self, Self::Error> {
		Ok(proto::RecordIdKeyRange {
			start: Some(Box::new(bound_to_proto(value.start)?)),
			end: Some(Box::new(bound_to_proto(value.end)?)),
		})
	}
}

impl TryFrom<proto::RecordIdKeyRange> for RecordIdKeyRange {
	type Error = anyhow::Error;

	fn try_from(value: proto::RecordIdKeyRange) -> Result<Self, Self::Error> {
		let start = value.start.ok_or_else(|| anyhow!("Missing start in RecordIdKeyRange"))?;
		let end = value.end.ok_or_else(|| anyhow!("Missing end in RecordIdKeyRange"))?;
		Ok(RecordIdKeyRange {
			start: bound_from_proto(*start)?,
			end: bound_from_proto(*end)?,
		})
	}
}

fn bound_to_proto(bound: Bound<RecordIdKey>) -> anyhow::Result<proto::RecordIdKeyBound> {
	let bound = match bound {
		Bound::Included(key) => RecordIdKeyBoundInner::Inclusive(Box::new(key.try_into()?)),
		Bound::Excluded(key) => RecordIdKeyBoundInner::Exclusive(Box::new(key.try_into()?)),
		Bound::Unbounded => RecordIdKeyBoundInner::Unbounded(proto::NullValue::default()),
	};
	Ok(proto::RecordIdKeyBound {
		bound: Some(bound),
	})
}

fn bound_from_proto(bound: proto::RecordIdKeyBound) -> anyhow::Result<Bound<RecordIdKey>> {
	let bound = bound.bound.ok_or_else(|| anyhow!("Missing bound in RecordIdKeyBound"))?;
	match bound {
		RecordIdKeyBoundInner::Inclusive(key) => Ok(Bound::Included(RecordIdKey::try_from(*key)?)),
		RecordIdKeyBoundInner::Exclusive(key) => Ok(Bound::Excluded(RecordIdKey::try_from(*key)?)),
		RecordIdKeyBoundInner::Unbounded(_) => Ok(Bound::Unbounded),
	}
}
