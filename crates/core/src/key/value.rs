use std::collections::{BTreeMap, Bound};
use std::fmt;
use std::fmt::Formatter;

use revision::revisioned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::kvs::impl_kv_value_revisioned;
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
	RecordIdKey, RecordIdKeyRange, Regex, Strand, Table, Uuid, Value,
};

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Value")]
pub(crate) enum KeyValue {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(KeyNumber),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(KeyArray),
	Object(KeyObject),
	Geometry(Geometry),
	Bytes(Bytes),
	RecordId(KeyRecordId),
	Table(Table),
	File(File),
	#[serde(skip)]
	Regex(Regex),
	Range(Box<Range>),
	#[serde(skip)]
	Closure(Box<Closure>),
	// Add new variants here
}

impl From<Value> for KeyValue {
	fn from(value: Value) -> Self {
		match value {
			Value::None => Self::None,
			Value::Null => Self::Null,
			Value::Bool(b) => Self::Bool(b),
			Value::Number(n) => Self::Number(n.into()),
			Value::Strand(s) => Self::Strand(s),
			Value::Duration(d) => Self::Duration(d),
			Value::Datetime(d) => Self::Datetime(d),
			Value::Uuid(u) => Self::Uuid(u),
			Value::Array(a) => Self::Array(a.into()),
			Value::Object(o) => Self::Object(o.into()),
			Value::Geometry(g) => Self::Geometry(g),
			Value::Bytes(b) => Self::Bytes(b),
			Value::RecordId(r) => Self::RecordId(r.into()),
			Value::Table(t) => Self::Table(t),
			Value::File(f) => Self::File(f),
			Value::Regex(r) => Self::Regex(r),
			Value::Range(r) => Self::Range(r),
			Value::Closure(c) => Self::Closure(c),
		}
	}
}

impl From<KeyValue> for Value {
	fn from(value: KeyValue) -> Self {
		match value {
			KeyValue::None => Self::None,
			KeyValue::Null => Self::Null,
			KeyValue::Bool(b) => Self::Bool(b),
			KeyValue::Number(n) => Self::Number(n.into()),
			KeyValue::Strand(s) => Self::Strand(s),
			KeyValue::Duration(d) => Self::Duration(d),
			KeyValue::Datetime(d) => Self::Datetime(d),
			KeyValue::Uuid(u) => Self::Uuid(u),
			KeyValue::Array(a) => Self::Array(a.into()),
			KeyValue::Object(o) => Self::Object(o.into()),
			KeyValue::Geometry(g) => Self::Geometry(g),
			KeyValue::Bytes(b) => Self::Bytes(b),
			KeyValue::RecordId(r) => Self::RecordId(r.into()),
			KeyValue::Table(t) => Self::Table(t),
			KeyValue::File(f) => Self::File(f),
			KeyValue::Regex(r) => Self::Regex(r),
			KeyValue::Range(r) => Self::Range(r),
			KeyValue::Closure(c) => Self::Closure(c),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct KeyArray(Vec<KeyValue>);

impl From<Array> for KeyArray {
	fn from(a: Array) -> Self {
		Self(a.0.into_iter().map(|i| i.into()).collect())
	}
}

impl From<KeyArray> for Array {
	fn from(a: KeyArray) -> Self {
		Self(a.0.into_iter().map(|i| i.into()).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct KeyObject(BTreeMap<String, KeyValue>);

impl From<Object> for KeyObject {
	fn from(o: Object) -> Self {
		Self(o.0.into_iter().map(|(k, v)| (k, v.into())).collect())
	}
}

impl From<KeyObject> for Object {
	fn from(o: KeyObject) -> Self {
		Self(o.0.into_iter().map(|(k, v)| (k, v.into())).collect())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct KeyRecordId {
	pub(super) table: String,
	pub(super) key: KeyRecordIdKey,
}

impl_kv_value_revisioned!(KeyRecordId);

impl From<RecordId> for KeyRecordId {
	fn from(r: RecordId) -> Self {
		Self {
			table: r.table,
			key: r.key.into(),
		}
	}
}

impl From<KeyRecordId> for RecordId {
	fn from(r: KeyRecordId) -> Self {
		Self {
			table: r.table,
			key: r.key.into(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) enum KeyRecordIdKey {
	Number(i64),
	//TODO: This should definitely be strand, not string as null bytes here can cause a lot of
	//issues.
	String(String),
	Uuid(Uuid),
	Array(KeyArray),
	Object(KeyObject),
	Range(Box<RecordIdKeyRange>),
}

impl_kv_value_revisioned!(KeyRecordIdKey);

impl From<RecordIdKey> for KeyRecordIdKey {
	fn from(r: RecordIdKey) -> Self {
		match r {
			RecordIdKey::Number(n) => Self::Number(n),
			RecordIdKey::String(s) => Self::String(s),
			RecordIdKey::Uuid(u) => Self::Uuid(u),
			RecordIdKey::Array(a) => Self::Array(a.into()),
			RecordIdKey::Object(o) => Self::Object(o.into()),
			RecordIdKey::Range(r) => Self::Range(Box::new(r.into())),
		}
	}
}

pub(crate) struct KeyRecordIdKeyRange {
	pub start: Bound<KeyRecordIdKey>,
	pub end: Bound<KeyRecordIdKey>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub(crate) struct KeyNumber(pub Number);

impl From<Number> for KeyNumber {
	fn from(n: Number) -> Self {
		Self(n)
	}
}

impl From<KeyNumber> for Number {
	fn from(n: KeyNumber) -> Self {
		n.0
	}
}

impl Serialize for KeyNumber {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let buf = self.0.as_decimal_buf().map_err(serde::ser::Error::custom)?;
		serializer.serialize_bytes(&buf)
	}
}

impl<'de> Deserialize<'de> for KeyNumber {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// A small visitor that accepts both borrowed and owned byte
		// buffers and forwards them to `from_decimal_buf`.
		struct NumberVisitor;

		impl serde::de::Visitor<'_> for NumberVisitor {
			type Value = KeyNumber;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("SurrealDB binary-encoded Number")
			}

			fn visit_bytes<E>(self, v: &[u8]) -> Result<KeyNumber, E>
			where
				E: serde::de::Error,
			{
				Ok(Number::from_decimal_buf(v).map_err(E::custom)?.into())
			}

			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<KeyNumber, E>
			where
				E: serde::de::Error,
			{
				self.visit_bytes(&v)
			}
		}

		deserializer.deserialize_bytes(NumberVisitor)
	}
}
