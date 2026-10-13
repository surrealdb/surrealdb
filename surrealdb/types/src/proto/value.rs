use std::collections::BTreeMap;
use std::ops::Bound;
use std::str::FromStr;

use anyhow::anyhow;
use surrealdb_protocol::proto::v1 as proto;
use surrealdb_protocol::proto::v1::value::Value as ValueInner;
use surrealdb_protocol::proto::v1::value_bound::Bound as ValueBoundInner;

use crate::{
	Array, Bytes, Datetime, Duration, File, Number, Object, Range, Regex, Set, Table, Uuid, Value,
};

impl TryFrom<Value> for proto::Value {
	type Error = anyhow::Error;

	/// Fallible because the wire's `Duration` counts seconds in an `i64` while
	/// `std::time::Duration` counts them in a `u64`, so the largest durations
	/// this crate can hold have no representation on the wire. Refusing one
	/// beats wrapping it into a negative duration.
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		Ok(match value {
			Value::None => proto::Value::none(),
			Value::Null => proto::Value::null(),
			Value::Bool(b) => proto::Value::bool(b),
			Value::Number(Number::Int(i)) => proto::Value::int64(i),
			Value::Number(Number::Float(f)) => proto::Value::float64(f),
			Value::Number(Number::Decimal(d)) => proto::Value::decimal(d.into()),
			Value::String(s) => proto::Value::string(s),
			Value::Bytes(b) => proto::Value::bytes(b.0),
			Value::Duration(d) => proto::Value::duration(d.0.try_into()?),
			Value::Datetime(dt) => proto::Value::datetime(proto::Datetime::from_chrono(dt.0)),
			Value::Uuid(u) => proto::Value::uuid(u.0.into()),
			Value::Geometry(g) => proto::Value::geometry(g.into()),
			Value::Table(t) => proto::Value::table(t.into_string()),
			Value::RecordId(r) => proto::Value::record_id(r.try_into()?),
			Value::File(f) => proto::Value::file(proto::File::new(f.bucket, f.key)),
			Value::Range(r) => proto::Value {
				value: Some(ValueInner::Range(Box::new((*r).try_into()?))),
			},
			Value::Regex(r) => proto::Value::regex(r.0.as_str().to_string()),
			Value::Object(o) => proto::Value::object(o.try_into()?),
			Value::Array(a) => proto::Value::array(a.try_into()?),
			Value::Set(s) => proto::Value::set(s.try_into()?),
		})
	}
}

impl TryFrom<proto::Value> for Value {
	type Error = anyhow::Error;

	fn try_from(value: proto::Value) -> Result<Self, Self::Error> {
		// An unset oneof means an unknown variant from a newer peer, or a
		// malformed message -- never `NONE`. See the note on `value.proto`'s
		// `Value` message.
		let inner = value
			.value
			.ok_or_else(|| anyhow!("unrecognised Value variant: this build cannot represent it"))?;
		match inner {
			ValueInner::None(_) => Ok(Value::None),
			ValueInner::Null(_) => Ok(Value::Null),
			ValueInner::Bool(b) => Ok(Value::Bool(b)),
			ValueInner::Int64(i) => Ok(Value::Number(Number::Int(i))),
			ValueInner::Float64(f) => Ok(Value::Number(Number::Float(f))),
			ValueInner::Decimal(d) => Ok(Value::Number(Number::Decimal(d.try_into()?))),
			ValueInner::String(s) => Ok(Value::String(s)),
			ValueInner::Bytes(b) => Ok(Value::Bytes(Bytes(b))),
			ValueInner::Duration(d) => Ok(Value::Duration(Duration(d.try_into()?))),
			ValueInner::Datetime(dt) => {
				let chrono_dt =
					dt.to_chrono().ok_or_else(|| anyhow!("Datetime out of representable range"))?;
				Ok(Value::Datetime(Datetime(chrono_dt)))
			}
			ValueInner::Uuid(u) => Ok(Value::Uuid(Uuid(u.to_uuid()?))),
			ValueInner::Geometry(g) => Ok(Value::Geometry(g.try_into()?)),
			ValueInner::Table(t) => Ok(Value::Table(Table::new(t))),
			ValueInner::RecordId(r) => Ok(Value::RecordId(r.try_into()?)),
			// A raw, unparsed record id awaiting server-side parsing. This
			// build has no "unparsed record id" value to decode it into.
			ValueInner::StringRecordId(_) => {
				Err(anyhow!("Unsupported Value type: an unparsed string record id"))
			}
			ValueInner::File(f) => Ok(Value::File(File {
				bucket: f.bucket,
				key: f.key,
			})),
			ValueInner::Range(r) => Ok(Value::Range(Box::new((*r).try_into()?))),
			ValueInner::Regex(s) => Ok(Value::Regex(Regex::from_str(&s)?)),
			ValueInner::Object(o) => Ok(Value::Object(o.try_into()?)),
			ValueInner::Array(a) => Ok(Value::Array(a.try_into()?)),
			ValueInner::Set(s) => Ok(Value::Set(s.try_into()?)),
		}
	}
}

impl TryFrom<Object> for proto::Object {
	type Error = anyhow::Error;

	fn try_from(value: Object) -> Result<Self, Self::Error> {
		let mut items = BTreeMap::new();
		for (key, value) in value.0 {
			items.insert(key, proto::Value::try_from(value)?);
		}
		Ok(items.into())
	}
}

impl TryFrom<proto::Object> for Object {
	type Error = anyhow::Error;

	fn try_from(value: proto::Object) -> Result<Self, Self::Error> {
		let mut map = BTreeMap::new();
		for entry in value.items {
			let value = entry
				.value
				.ok_or_else(|| anyhow!("Missing value in Object entry for key {:?}", entry.key))?;
			map.insert(entry.key, Value::try_from(value)?);
		}
		Ok(Object(map))
	}
}

impl TryFrom<Array> for proto::Array {
	type Error = anyhow::Error;

	fn try_from(value: Array) -> Result<Self, Self::Error> {
		Ok(proto::Array::new(try_values(value.0)?))
	}
}

impl TryFrom<proto::Array> for Array {
	type Error = anyhow::Error;

	fn try_from(value: proto::Array) -> Result<Self, Self::Error> {
		let values =
			value.values.into_iter().map(Value::try_from).collect::<anyhow::Result<Vec<_>>>()?;
		Ok(Array(values))
	}
}

impl TryFrom<Set> for proto::Set {
	type Error = anyhow::Error;

	fn try_from(value: Set) -> Result<Self, Self::Error> {
		Ok(proto::Set {
			values: try_values(value.0)?,
		})
	}
}

/// Converts a sequence of values, stopping at the first one the wire cannot
/// carry.
fn try_values(values: impl IntoIterator<Item = Value>) -> anyhow::Result<Vec<proto::Value>> {
	values.into_iter().map(proto::Value::try_from).collect()
}

impl TryFrom<proto::Set> for Set {
	type Error = anyhow::Error;

	fn try_from(value: proto::Set) -> Result<Self, Self::Error> {
		let values =
			value.values.into_iter().map(Value::try_from).collect::<anyhow::Result<_>>()?;
		Ok(Set(values))
	}
}

impl TryFrom<Range> for proto::Range {
	type Error = anyhow::Error;

	fn try_from(value: Range) -> Result<Self, Self::Error> {
		Ok(proto::Range {
			start: Some(Box::new(bound_to_proto(value.start)?)),
			end: Some(Box::new(bound_to_proto(value.end)?)),
		})
	}
}

impl TryFrom<proto::Range> for Range {
	type Error = anyhow::Error;

	fn try_from(value: proto::Range) -> Result<Self, Self::Error> {
		let start = value.start.ok_or_else(|| anyhow!("Missing start in Range"))?;
		let end = value.end.ok_or_else(|| anyhow!("Missing end in Range"))?;
		Ok(Range {
			start: bound_from_proto(*start)?,
			end: bound_from_proto(*end)?,
		})
	}
}

fn bound_to_proto(bound: Bound<Value>) -> anyhow::Result<proto::ValueBound> {
	let bound = match bound {
		Bound::Included(v) => ValueBoundInner::Inclusive(Box::new(v.try_into()?)),
		Bound::Excluded(v) => ValueBoundInner::Exclusive(Box::new(v.try_into()?)),
		Bound::Unbounded => ValueBoundInner::Unbounded(proto::NullValue::default()),
	};
	Ok(proto::ValueBound {
		bound: Some(bound),
	})
}

fn bound_from_proto(bound: proto::ValueBound) -> anyhow::Result<Bound<Value>> {
	let bound = bound.bound.ok_or_else(|| anyhow!("Missing bound in ValueBound"))?;
	match bound {
		ValueBoundInner::Inclusive(v) => Ok(Bound::Included(Value::try_from(*v)?)),
		ValueBoundInner::Exclusive(v) => Ok(Bound::Excluded(Value::try_from(*v)?)),
		ValueBoundInner::Unbounded(_) => Ok(Bound::Unbounded),
	}
}
