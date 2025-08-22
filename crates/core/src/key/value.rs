//! StoreKeyValue and StoreKeyNumber encode Values for KV store keys.
//!
//! Numbers are serialized using a lexicographic encoding (see DecimalLexEncoder)
//! such that byte-wise ordering matches numeric ordering. This ensures index
//! scans and uniqueness checks behave consistently across Int/Float/Decimal
//! representations of the same value. Serialization is stream-friendly: the
//! numeric encoding guarantees an in-band terminator and additionally appends a
//! 0x00 byte so deserializers can read until the first zero.
//!
//! Some Value variants (eg Regex, Closure) are skipped for serde because they
//! are not used in key material.
use std::cmp::Ordering;
use std::collections::{BTreeMap, Bound};
use std::fmt;
use std::fmt::{Debug, Formatter};

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::kvs::KVKey;
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
	RecordIdKey, RecordIdKeyRange, Regex, Strand, Table, Uuid, Value,
};

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

impl KVKey for StoreKeyValue {
	type ValueType = ();
}

impl StoreKeyValue {
	pub(crate) fn is_none(&self) -> bool {
		matches!(self, Self::None)
	}
}

/// Ordered collection of values serialized for index keys and prefixes.
/// Numeric values are normalized via StoreKeyNumber’s lexicographic encoding
/// when present, and components are zero-terminated to allow safe concatenation
/// in composite keys.
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyArray(pub(crate) Vec<StoreKeyValue>);

impl From<StoreKeyValue> for StoreKeyArray {
	fn from(value: StoreKeyValue) -> Self {
		Self(vec![value])
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

/// Map-like structure for object values used in keys.
/// Field values use StoreKeyValue encoding; numeric fields are normalized so
/// that lexicographic byte order matches numeric order in indexes.
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyObject(BTreeMap<String, StoreKeyValue>);

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

/// Record identifier encoding for use inside keys.
/// Table and key are encoded as zero-terminated components so that the
/// concatenated key stream can be parsed unambiguously.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyRecordId {
	pub(super) table: String,
	pub(crate) key: StoreKeyRecordIdKey,
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

/// Key component for RecordId when serialized inside keys.
/// Variants preserve ordering semantics; composite types reuse StoreKey encodings.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) enum StoreKeyRecordIdKey {
	Number(i64),
	//TODO: This should definitely be strand, not string as null bytes here can cause a lot of
	//issues.
	String(String),
	Uuid(Uuid),
	Array(StoreKeyArray),
	Object(StoreKeyObject),
	Range(Box<StoreKeyRecordIdKeyRange>),
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

/// Range of RecordIdKey values used in key encoding. Bounds are expressed
/// with Included/Excluded/Unbounded and follow the same StoreKey encoding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash)]
pub(crate) struct StoreKeyRecordIdKeyRange {
	pub start: Bound<StoreKeyRecordIdKey>,
	pub end: Bound<StoreKeyRecordIdKey>,
}

impl From<RecordIdKeyRange> for StoreKeyRecordIdKeyRange {
	fn from(r: RecordIdKeyRange) -> Self {
		fn map_bound(b: Bound<RecordIdKey>) -> Bound<StoreKeyRecordIdKey> {
			match b {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			}
		}
		Self {
			start: map_bound(r.start),
			end: map_bound(r.end),
		}
	}
}

impl From<StoreKeyRecordIdKeyRange> for RecordIdKeyRange {
	fn from(r: StoreKeyRecordIdKeyRange) -> Self {
		fn map_bound(b: Bound<StoreKeyRecordIdKey>) -> Bound<RecordIdKey> {
			match b {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			}
		}
		Self {
			start: map_bound(r.start),
			end: map_bound(r.end),
		}
	}
}

// Custom partial comparison for ranges: compares start bound first, then end
// bound, respecting Included/Excluded/Unbounded semantics.
impl PartialOrd for StoreKeyRecordIdKeyRange {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		fn compare_bounds(
			a: &Bound<StoreKeyRecordIdKey>,
			b: &Bound<StoreKeyRecordIdKey>,
		) -> Option<Ordering> {
			match a {
				Bound::Unbounded => match b {
					Bound::Unbounded => Some(Ordering::Equal),
					_ => Some(Ordering::Less),
				},
				Bound::Included(a) => match b {
					Bound::Unbounded => Some(Ordering::Greater),
					Bound::Included(b) => a.partial_cmp(b),
					Bound::Excluded(_) => Some(Ordering::Less),
				},
				Bound::Excluded(a) => match b {
					Bound::Excluded(b) => a.partial_cmp(b),
					_ => Some(Ordering::Greater),
				},
			}
		}
		match compare_bounds(&self.start, &other.end) {
			Some(Ordering::Equal) => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Hash)]
pub(crate) struct StoreKeyNumber(pub Number);

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

/// Serializes as a zero-terminated sequence of u8 bytes produced by
/// Number::as_decimal_buf(), which preserves lexicographic order for numbers.
impl Serialize for StoreKeyNumber {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let buf = self.0.as_decimal_buf().map_err(serde::ser::Error::custom)?;
		let mut seq = serializer.serialize_seq(None)?;
		for b in buf {
			seq.serialize_element(&b)?
		}
		seq.end()
	}
}

/// Deserializes a zero-terminated sequence of u8 bytes into a StoreKeyNumber.
/// The visitor reads until the first 0x00 terminator, then delegates to
/// Number::from_decimal_buf() which understands the DecimalLexEncoder format.
impl<'de> Deserialize<'de> for StoreKeyNumber {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NumberVisitor;

		impl<'de> Visitor<'de> for NumberVisitor {
			type Value = StoreKeyNumber;

			fn expecting(&self, f: &mut Formatter) -> fmt::Result {
				f.write_str("zero-terminated sequence of u8 encoding a StoreKeyNumber")
			}

			// Primary path: we serialized as a sequence of u8 (no length prefix),
			// so deserialize by streaming u8s until the first 0x00 terminator.
			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: SeqAccess<'de>,
			{
				let mut buf = Vec::with_capacity(16);
				while let Some(b) = seq.next_element::<u8>()? {
					buf.push(b);
					if b == 0x00 {
						break; // stop at the terminator
					}
				}

				// Require terminator to ensure we didn’t split across fields.
				match buf.last() {
					Some(0x00) => {}
					_ => {
						return Err(de::Error::custom(
							"unterminated numeric encoding (missing 0x00)",
						));
					}
				}

				crate::val::Number::from_decimal_buf(&buf)
					.map(StoreKeyNumber)
					.map_err(de::Error::custom)
			}

			// Compatibility path: if deserializer offers a contiguous byte slice.
			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				crate::val::Number::from_decimal_buf(v).map(StoreKeyNumber).map_err(E::custom)
			}

			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				self.visit_bytes(&v)
			}
		}

		// Allow both sequence and bytes forms.
		deserializer.deserialize_any(NumberVisitor)
	}
}
