use std::{
	borrow::Borrow,
	collections::{btree_map::IterMut, BTreeMap},
	fmt,
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Bytes(Vec<u8>);

#[derive(Debug, Clone)]
pub struct Datetime(DateTime<Utc>);

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RecordIdKey {
	String(String),
	Integer(i64),
	Object(Object),
	Array(Vec<Value>),
}

#[derive(Debug, Clone)]
pub struct RecordId {
	table: String,
	key: RecordIdKey,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Default, Clone)]
pub struct Object(BTreeMap<String, Value>);

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
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Value {
	Null,
	Bool(bool),
	Number(Number),
	Object(Object),
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
					from: Kind::$variant,
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
	(RecordId(RecordId), as_record_id, as_record_id_mut, into_record_id),
);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Kind {
	Null,
	Bool,
	Number,
	Object,
	Array,
	Uuid,
	Datetime,
	Duration,
	Bytes,
	RecordId,
}

pub struct ConversionError {
	from: Kind,
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
