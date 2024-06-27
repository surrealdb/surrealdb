use std::{
	borrow::Borrow,
	cmp::{Ordering, PartialEq, PartialOrd},
	collections::{btree_map::IterMut, BTreeMap},
	fmt,
	ops::Deref,
	time::Duration,
};

use chrono::{DateTime, Utc};
use revision::revisioned;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod de;
mod deserializer;
mod ser;
mod serializer;
mod sql;

pub use serializer::Serializer;

// Keeping bytes implementation minimal since it might be a good idea to use bytes crate here
// instead of a plain Vec<u8>.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub struct Bytes(Vec<u8>);

impl Bytes {
	pub fn copy_from_slice(slice: &[u8]) -> Self {
		slice.to_vec().into()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}
}

impl PartialEq<[u8]> for Bytes {
	fn eq(&self, other: &[u8]) -> bool {
		self.0 == other
	}
}

impl PartialOrd<[u8]> for Bytes {
	fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
		self.0.as_slice().partial_cmp(other)
	}
}

impl Deref for Bytes {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.0.as_slice()
	}
}

impl From<Vec<u8>> for Bytes {
	fn from(value: Vec<u8>) -> Self {
		Bytes(value)
	}
}

// Keeping the Datetime wrapped, the chrono is still pre 1.0 so we can't gaurentee stability here,
// best to keep most methods interal with maybe some functions from coverting between chrono types explicitly marked as unstable.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub struct Datetime(DateTime<Utc>);

/// The key of a [`RecordId`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RecordIdKey {
	/// A string record id, like in `user:tkwse1j5o0anqjxonvzx`.
	String(String),
	/// A integer record id like in `temperature:17493`
	Integer(i64),
	/// An object record id like in `weather:{ location: 'London', date: d'2022-08-29T08:03:39' }`.
	Object(Object),
	/// An array record id like in `temperature:['London', d'2022-08-29T08:03:39']`.
	Array(Vec<Value>),
}

impl From<Object> for RecordIdKey {
	fn from(value: Object) -> Self {
		RecordIdKey::Object(value)
	}
}

impl From<String> for RecordIdKey {
	fn from(value: String) -> Self {
		RecordIdKey::String(value)
	}
}

impl From<&str> for RecordIdKey {
	fn from(value: &str) -> Self {
		RecordIdKey::String(value.to_owned())
	}
}

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		RecordIdKey::Integer(value)
	}
}

impl From<Vec<Value>> for RecordIdKey {
	fn from(value: Vec<Value>) -> Self {
		RecordIdKey::Array(value)
	}
}

/// Struct representing a record id.
///
/// Record id's consist of a table name and a key.
/// For example the record id `user:tkwse1j5o0anqjxonvzx` has the table `user` and the key `tkwse1j5o0anqjxonvzx`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordId {
	#[serde(rename = "tb")]
	table: String,
	#[serde(rename = "id")]
	key: RecordIdKey,
}

impl RecordId {
	pub fn from_table_key<S, K>(table: S, key: K) -> Self
	where
		S: Into<String>,
		K: Into<RecordIdKey>,
	{
		RecordId {
			table: table.into(),
			key: key.into(),
		}
	}

	pub fn table(&self) -> &str {
		self.table.as_str()
	}

	pub fn key(&self) -> &RecordIdKey {
		&self.key
	}
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Number {
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
}

#[derive(Debug)]
pub struct ObjectIterMut<'a>(IterMut<'a, String, Value>);

impl<'a> Iterator for ObjectIterMut<'a> {
	type Item = (&'a String, &'a mut Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
}

impl<'a> std::iter::FusedIterator for ObjectIterMut<'a> {}

impl<'a> ExactSizeIterator for ObjectIterMut<'a> {
	fn len(&self) -> usize {
		<IterMut<'a, String, Value> as ExactSizeIterator>::len(&self.0)
	}
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub struct Object(pub(crate) BTreeMap<String, Value>);

impl Object {
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	pub fn clear(&mut self) {
		self.0.clear()
	}

	pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&Value>
	where
		String: Borrow<Q>,
		Q: Ord,
	{
		self.0.get(key)
	}

	pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Value>
	where
		String: Borrow<Q>,
		Q: Ord,
	{
		self.0.get_mut(key)
	}

	pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
	where
		String: Borrow<Q>,
		Q: Ord,
	{
		self.0.contains_key(key)
	}

	pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<Value>
	where
		String: Borrow<Q>,
		Q: Ord,
	{
		self.0.remove(key)
	}

	pub fn remove_entry<Q: ?Sized>(&mut self, key: &Q) -> Option<(String, Value)>
	where
		String: Borrow<Q>,
		Q: Ord,
	{
		self.0.remove_entry(key)
	}

	pub fn iter_mut(&mut self) -> ObjectIterMut<'_> {
		ObjectIterMut(self.0.iter_mut())
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn insert<V>(&mut self, key: String, value: V) -> Option<Value>
	where
		V: Into<Value>,
	{
		self.0.insert(key, value.into())
	}
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
	None,
	Bool(bool),
	Number(Number),
	Object(Object),
	String(String),
	Array(Vec<Value>),
	Uuid(Uuid),
	Datetime(Datetime),
	Duration(Duration),
	Bytes(Bytes),
	RecordId(RecordId),
}

impl Value {
	pub fn int(v: i64) -> Self {
		Value::Number(Number::Integer(v))
	}

	pub fn float(v: f64) -> Self {
		Value::Number(Number::Float(v))
	}

	pub fn decimal(v: Decimal) -> Self {
		Value::Number(Number::Decimal(v))
	}
}

macro_rules! impl_convert(
	($(($variant:ident($ty:ty), $as:ident,$as_mut:ident, $into:ident)),*$(,)?) => {
		impl Value{

			$(
			#[doc = concat!("Get a reference to the internal ",stringify!($ty)," if the value is of that type")]
			pub fn $as(&self) -> Option<&$ty>{
				if let Value::$variant(ref x) = self{
					Some(x)
				}else{
					None
				}
			}

			#[doc = concat!("Get a reference to the internal ",stringify!($ty)," if the value is of that type")]
			pub fn $as_mut(&mut self) -> Option<&mut $ty>{
				if let Value::$variant(ref mut x) = self{
					Some(x)
				}else{
					None
				}
			}

			#[doc = concat!("Convert the value to ",stringify!($ty)," if the value is of that type")]
			pub fn $into(self) -> Option<$ty>{
				if let Value::$variant(x) = self{
					Some(x)
				}else{
					None
				}
			}
			)*
		}

		$(
		impl From<$ty> for Value {
			fn from(v: $ty) -> Self{
				Value::$variant(v)
			}
		}

		impl TryFrom<Value> for $ty {
			type Error = ConversionError;

			fn try_from(v: Value) -> Result<Self, Self::Error>{
				v.$into().ok_or(ConversionError{
					from: stringify!($variant),
					expected: stringify!($ty),
				})
			}
		}
		)*
	};
);

impl_convert!(
	(Bool(bool), as_bool, as_bool_mut, into_bool),
	(Number(Number), as_number, as_number_mut, into_number),
	(Uuid(Uuid), as_uuid, as_uuid_mut, into_uuid),
	(Datetime(Datetime), as_datetime, as_datetime_mut, into_dateime),
	(Duration(Duration), as_duration, as_duration_mut, into_duration),
	(Bytes(Bytes), as_bytes, as_bytes_mut, into_bytes),
	(String(String), as_string, as_string_mut, into_string),
	(RecordId(RecordId), as_record_id, as_record_id_mut, into_record_id),
	(Object(Object), as_object, as_object_mut, into_object),
);

pub struct ConversionError {
	from: &'static str,
	expected: &'static str,
}

impl fmt::Display for ConversionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(
			f,
			"failed to convert into `{}` from value with type `{:?}`",
			self.expected, self.from
		)
	}
}
