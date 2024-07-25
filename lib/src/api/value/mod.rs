use std::{
	borrow::Borrow,
	cmp::{Ordering, PartialEq, PartialOrd},
	collections::{btree_map, BTreeMap},
	fmt,
	iter::FusedIterator,
	ops::Deref,
	time::Duration,
};

use chrono::{DateTime, Utc};
use revision::revisioned;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod core;
mod de;
mod deserializer;
mod ser;
mod serializer;

pub(crate) use core::ToCore;

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
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
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Number {
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
}

impl Number {
	pub fn cource_into_i64(self) -> Option<i64> {
		match self {
			Self::Integer(x) => Some(x),
			Self::Float(x) if x.fract() == x => Some(x as i64),
			Self::Decimal(x) => x.try_into().ok(),
			_ => None,
		}
	}
}

#[derive(Debug)]
pub struct IterMut<'a> {
	iter: btree_map::IterMut<'a, String, Value>,
}

impl<'a> Iterator for IterMut<'a> {
	type Item = (&'a String, &'a mut Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}
}

impl<'a> std::iter::FusedIterator for IterMut<'a> {}

impl<'a> ExactSizeIterator for IterMut<'a> {
	fn len(&self) -> usize {
		self.iter.len()
	}
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub struct Object(pub(crate) BTreeMap<String, Value>);

impl Object {
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	pub fn clear(&mut self) {
		self.0.clear()
	}

	pub fn get<Q>(&self, key: &Q) -> Option<&Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get(key)
	}

	pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get_mut(key)
	}

	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		String: Borrow<Q>,
		Q: ?Sized + Ord,
	{
		self.0.contains_key(key)
	}

	pub fn remove<Q>(&mut self, key: &Q) -> Option<Value>
	where
		String: Borrow<Q>,
		Q: ?Sized + Ord,
	{
		self.0.remove(key)
	}

	pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(String, Value)>
	where
		String: Borrow<Q>,
		Q: ?Sized + Ord,
	{
		self.0.remove_entry(key)
	}

	pub fn iter(&self) -> Iter<'_> {
		Iter {
			iter: self.0.iter(),
		}
	}

	pub fn iter_mut(&mut self) -> IterMut<'_> {
		IterMut {
			iter: self.0.iter_mut(),
		}
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn insert<V>(&mut self, key: String, value: V) -> Option<Value>
	where
		V: Into<Value>,
	{
		self.0.insert(key, value.into())
	}
}

struct IntoIter {
	iter: <BTreeMap<String, Value> as IntoIterator>::IntoIter,
}

impl Iterator for IntoIter {
	type Item = (String, Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}
}

impl DoubleEndedIterator for IntoIter {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back()
	}
}

impl ExactSizeIterator for IntoIter {
	fn len(&self) -> usize {
		self.iter.len()
	}
}

impl FusedIterator for IntoIter {}

impl IntoIterator for Object {
	type Item = <IntoIter as Iterator>::Item;

	type IntoIter = IntoIter;

	fn into_iter(self) -> Self::IntoIter {
		IntoIter {
			iter: self.0.into_iter(),
		}
	}
}

#[derive(Clone)]
struct Iter<'a> {
	iter: <&'a BTreeMap<String, Value> as IntoIterator>::IntoIter,
}

impl<'a> IntoIterator for &'a Object {
	type Item = (&'a String, &'a Value);

	type IntoIter = Iter<'a>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

impl<'a> Iterator for Iter<'a> {
	type Item = (&'a String, &'a Value);

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.iter.size_hint()
	}

	fn last(self) -> Option<Self::Item>
	where
		Self: Sized,
	{
		self.iter.last()
	}

	fn min(mut self) -> Option<Self::Item> {
		self.iter.next()
	}

	fn max(mut self) -> Option<Self::Item> {
		self.iter.next_back()
	}
}

impl FusedIterator for Iter<'_> {}

impl<'a> DoubleEndedIterator for Iter<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.iter.next_back()
	}
}

impl<'a> ExactSizeIterator for Iter<'a> {
	fn len(&self) -> usize {
		self.iter.len()
	}
}

#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
pub enum Value {
	#[default]
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

impl From<i64> for Value {
	fn from(value: i64) -> Self {
		Value::int(value)
	}
}

impl From<f64> for Value {
	fn from(value: f64) -> Self {
		Value::float(value)
	}
}

impl From<Decimal> for Value {
	fn from(value: Decimal) -> Self {
		Value::decimal(value)
	}
}

impl From<&str> for Value {
	fn from(value: &str) -> Self {
		Value::from(value.to_string())
	}
}

macro_rules! impl_convert(
	($(($variant:ident($ty:ty), $is:ident, $as:ident,$as_mut:ident, $into:ident)),*$(,)?) => {
		impl Value{

			$(
			#[doc = concat!("Return whether the value contains a ",stringify!($ty),".")]
			pub fn $is(&self) -> bool{
				matches!(&self, Value::$variant(_))
			}

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

		impl From<Option<$ty>> for Value {
			fn from(v: Option<$ty>) -> Self{
				if let Some(v) = v {
					Value::$variant(v)
				}else{
					Value::None
				}
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
	(Bool(bool), is_bool, as_bool, as_bool_mut, into_bool),
	(Number(Number), is_number, as_number, as_number_mut, into_number),
	(Uuid(Uuid), is_uuid, as_uuid, as_uuid_mut, into_uuid),
	(Datetime(Datetime), is_datetime, as_datetime, as_datetime_mut, into_dateime),
	(Duration(Duration), is_duration, as_duration, as_duration_mut, into_duration),
	(Bytes(Bytes), is_bytes, as_bytes, as_bytes_mut, into_bytes),
	(String(String), is_string, as_string, as_string_mut, into_string),
	(RecordId(RecordId), is_record_id, as_record_id, as_record_id_mut, into_record_id),
	(Object(Object), is_object, as_object, as_object_mut, into_object),
	(Array(Vec<Value>), is_array, as_array, as_array_mut, into_array),
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
