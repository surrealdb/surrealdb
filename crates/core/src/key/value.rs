use std::collections::{BTreeMap, Bound};
use std::fmt;
use std::fmt::{Debug, Display, Formatter, Write};

use crate::expr::escape::EscapeRid;
use crate::expr::fmt::Pretty;
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
	RecordIdKey, RecordIdKeyRange, Regex, Strand, Table, Uuid, Value,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Value")]
pub(crate) enum StoreKeyValue {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(StoreKeyNumber),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(StoreKeyArray),
	Object(StoreKeyObject),
	Geometry(Geometry),
	Bytes(Bytes),
	RecordId(StoreKeyRecordId),
	Table(Table),
	File(File),
	#[serde(skip)]
	Regex(Regex),
	Range(Box<Range>),
	#[serde(skip)]
	Closure(Box<Closure>),
	// Add new variants here
}

impl Display for StoreKeyValue {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match &self {
			Self::None => write!(f, "NONE"),
			Self::Null => write!(f, "NULL"),
			Self::Array(v) => write!(f, "{v}"),
			Self::Bool(v) => write!(f, "{v}"),
			Self::Bytes(v) => write!(f, "{v}"),
			Self::Datetime(v) => write!(f, "{v}"),
			Self::Duration(v) => write!(f, "{v}"),
			Self::Geometry(v) => write!(f, "{v}"),
			Self::Number(v) => write!(f, "{v}"),
			Self::Object(v) => write!(f, "{v}"),
			Self::Range(v) => write!(f, "{v}"),
			Self::Regex(v) => write!(f, "{v}"),
			Self::Strand(v) => write!(f, "{v}"),
			Self::RecordId(v) => write!(f, "{v}"),
			Self::Uuid(v) => write!(f, "{v}"),
			Self::Closure(v) => write!(f, "{v}"),
			Self::File(v) => write!(f, "{v}"),
			Self::Table(v) => write!(f, "{v}"),
		}
	}
}

impl From<Value> for StoreKeyValue {
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

impl From<StoreKeyValue> for Value {
	fn from(value: StoreKeyValue) -> Self {
		match value {
			StoreKeyValue::None => Self::None,
			StoreKeyValue::Null => Self::Null,
			StoreKeyValue::Bool(b) => Self::Bool(b),
			StoreKeyValue::Number(n) => Self::Number(n.into()),
			StoreKeyValue::Strand(s) => Self::Strand(s),
			StoreKeyValue::Duration(d) => Self::Duration(d),
			StoreKeyValue::Datetime(d) => Self::Datetime(d),
			StoreKeyValue::Uuid(u) => Self::Uuid(u),
			StoreKeyValue::Array(a) => Self::Array(a.into()),
			StoreKeyValue::Object(o) => Self::Object(o.into()),
			StoreKeyValue::Geometry(g) => Self::Geometry(g),
			StoreKeyValue::Bytes(b) => Self::Bytes(b),
			StoreKeyValue::RecordId(r) => Self::RecordId(r.into()),
			StoreKeyValue::Table(t) => Self::Table(t),
			StoreKeyValue::File(f) => Self::File(f),
			StoreKeyValue::Regex(r) => Self::Regex(r),
			StoreKeyValue::Range(r) => Self::Range(r),
			StoreKeyValue::Closure(c) => Self::Closure(c),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyArray(pub(crate) Vec<StoreKeyValue>);

impl From<StoreKeyValue> for StoreKeyArray {
	fn from(value: StoreKeyValue) -> Self {
		Self(vec![value])
	}
}

impl Display for StoreKeyArray {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Array::display(&self.0, f)
	}
}

impl From<Array> for StoreKeyArray {
	fn from(a: Array) -> Self {
		Self(a.0.into_iter().map(|i| i.into()).collect())
	}
}

impl From<StoreKeyArray> for Array {
	fn from(a: StoreKeyArray) -> Self {
		Self(a.0.into_iter().map(|i| i.into()).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyObject(BTreeMap<String, StoreKeyValue>);

impl Display for StoreKeyObject {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Object::display(f, &self.0)
	}
}

impl From<Object> for StoreKeyObject {
	fn from(o: Object) -> Self {
		Self(o.0.into_iter().map(|(k, v)| (k, v.into())).collect())
	}
}

impl From<StoreKeyObject> for Object {
	fn from(o: StoreKeyObject) -> Self {
		Self(o.0.into_iter().map(|(k, v)| (k, v.into())).collect())
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyRecordId {
	pub(super) table: String,
	pub(crate) key: StoreKeyRecordIdKey,
}

impl Display for StoreKeyRecordId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.table), self.key)
	}
}
impl From<RecordId> for StoreKeyRecordId {
	fn from(r: RecordId) -> Self {
		Self {
			table: r.table,
			key: r.key.into(),
		}
	}
}

impl From<StoreKeyRecordId> for RecordId {
	fn from(r: StoreKeyRecordId) -> Self {
		Self {
			table: r.table,
			key: r.key.into(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) enum StoreKeyRecordIdKey {
	Number(i64),
	//TODO: This should definitely be strand, not string as null bytes here can cause a lot of
	//issues.
	String(String),
	Uuid(Uuid),
	Array(StoreKeyArray),
	Object(StoreKeyObject),
	Range(Box<RecordIdKeyRange>),
}

impl Display for StoreKeyRecordIdKey {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Number(n) => write!(f, "{n}"),
			Self::String(v) => EscapeRid(v).fmt(f),
			Self::Uuid(uuid) => Display::fmt(uuid, f),
			Self::Object(object) => Display::fmt(object, f),
			Self::Array(array) => Display::fmt(array, f),
			Self::Range(rid) => Display::fmt(rid, f),
		}
	}
}

impl From<RecordIdKey> for StoreKeyRecordIdKey {
	fn from(r: RecordIdKey) -> Self {
		match r {
			RecordIdKey::Number(n) => Self::Number(n),
			RecordIdKey::String(s) => Self::String(s),
			RecordIdKey::Uuid(u) => Self::Uuid(u),
			RecordIdKey::Array(a) => Self::Array(a.into()),
			RecordIdKey::Object(o) => Self::Object(o.into()),
			RecordIdKey::Range(r) => Self::Range(Box::new((*r).into())),
		}
	}
}

impl From<StoreKeyRecordIdKey> for RecordIdKey {
	fn from(r: StoreKeyRecordIdKey) -> Self {
		match r {
			StoreKeyRecordIdKey::Number(n) => Self::Number(n),
			StoreKeyRecordIdKey::String(s) => Self::String(s),
			StoreKeyRecordIdKey::Uuid(u) => Self::Uuid(u),
			StoreKeyRecordIdKey::Array(a) => Self::Array(a.into()),
			StoreKeyRecordIdKey::Object(o) => Self::Object(o.into()),
			StoreKeyRecordIdKey::Range(r) => Self::Range(Box::new((*r).into())),
		}
	}
}

pub(crate) struct StoreKeyRecordIdKeyRange {
	pub start: Bound<StoreKeyRecordIdKey>,
	pub end: Bound<StoreKeyRecordIdKey>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub(crate) struct StoreKeyNumber(pub Number);

impl Display for StoreKeyNumber {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl From<Number> for StoreKeyNumber {
	fn from(n: Number) -> Self {
		Self(n)
	}
}

impl From<StoreKeyNumber> for Number {
	fn from(n: StoreKeyNumber) -> Self {
		n.0
	}
}

impl Serialize for StoreKeyNumber {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let buf = self.0.as_decimal_buf().map_err(serde::ser::Error::custom)?;
		serializer.serialize_bytes(&buf)
	}
}

impl<'de> Deserialize<'de> for StoreKeyNumber {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// A small visitor that accepts both borrowed and owned byte
		// buffers and forwards them to `from_decimal_buf`.
		struct NumberVisitor;

		impl serde::de::Visitor<'_> for NumberVisitor {
			type Value = StoreKeyNumber;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("SurrealDB binary-encoded Number")
			}

			fn visit_bytes<E>(self, v: &[u8]) -> Result<StoreKeyNumber, E>
			where
				E: serde::de::Error,
			{
				Ok(Number::from_decimal_buf(v).map_err(E::custom)?.into())
			}

			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<StoreKeyNumber, E>
			where
				E: serde::de::Error,
			{
				self.visit_bytes(&v)
			}
		}

		deserializer.deserialize_bytes(NumberVisitor)
	}
}
