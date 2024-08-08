use revision::revisioned;
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, ser::Serializer, Deserialize, Deserializer, Serialize};
use std::{
	cmp::{Ordering, PartialEq, PartialOrd},
	fmt,
	ops::Deref,
	str::FromStr,
	time::Duration,
};
use surrealdb_core::{
	sql::{
		Array as CoreArray, Datetime as CoreDatetime, Id as CoreId, Number as CoreNumber,
		Thing as CoreThing, Value as CoreValue,
	},
	syn,
};
use uuid::Uuid;

mod object;
pub use object::{Iter, IterMut, Object};

use crate::Error;

pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
	Ok(surrealdb_core::sql::from_value(value.0)?)
}

pub fn to_value<T: Serialize>(value: &T) -> Result<Value, Error> {
	Ok(Value(surrealdb_core::sql::to_value(value)?))
}

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

transparent_wrapper!(
	#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
	pub struct Datetime(CoreDatetime)
);

transparent_wrapper!(
	/// The key of a [`RecordId`].
	#[derive(Debug, Clone, PartialEq, PartialOrd)]
	#[non_exhaustive]
	pub struct RecordIdKey(CoreId)
);

impl From<Object> for RecordIdKey {
	fn from(value: Object) -> Self {
		Self::from_inner(CoreId::Object(value.into_inner()))
	}
}

impl From<String> for RecordIdKey {
	fn from(value: String) -> Self {
		Self(CoreId::String(value))
	}
}

impl From<&String> for RecordIdKey {
	fn from(value: &String) -> Self {
		Self(CoreId::String(value.clone()))
	}
}

impl From<&str> for RecordIdKey {
	fn from(value: &str) -> Self {
		Self(CoreId::String(value.to_owned()))
	}
}

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		Self(CoreId::Number(value))
	}
}

impl From<Vec<Value>> for RecordIdKey {
	fn from(value: Vec<Value>) -> Self {
		let res = Value::array_to_core(value);
		let mut array = CoreArray::default();
		array.0 = res;
		Self(CoreId::Array(array))
	}
}

impl From<RecordIdKey> for Value {
	fn from(key: RecordIdKey) -> Self {
		match key.0 {
			CoreId::String(x) => Value::from(x),
			CoreId::Number(x) => Value::int(x),
			CoreId::Object(x) => Value::from(Object::from_inner(x)),
			CoreId::Array(x) => Value::from(Value::core_to_array(x.0)),
			_ => panic!("lib recieved generate variant of record id"),
		}
	}
}

impl FromStr for Value {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(Value::from_inner(surrealdb_core::syn::value(s)?))
	}
}

#[derive(Debug)]
pub struct RecordIdKeyFromValueError(());

impl fmt::Display for RecordIdKeyFromValueError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(f,"tried to convert a value to a record id key with a value type that is not allowed in a record id key")
	}
}

impl TryFrom<Value> for RecordIdKey {
	type Error = RecordIdKeyFromValueError;

	fn try_from(key: Value) -> Result<Self, Self::Error> {
		match key.0 {
			CoreValue::Strand(x) => Ok(RecordIdKey::from_inner(CoreId::String(x.0))),
			CoreValue::Number(CoreNumber::Int(x)) => Ok(RecordIdKey::from_inner(CoreId::Number(x))),
			CoreValue::Object(x) => Ok(RecordIdKey::from_inner(CoreId::Object(x))),
			CoreValue::Array(x) => Ok(RecordIdKey::from_inner(CoreId::Array(x))),
			_ => Err(RecordIdKeyFromValueError(())),
		}
	}
}

transparent_wrapper!(
	/// Struct representing a record id.
	///
	/// Record id's consist of a table name and a key.
	/// For example the record id `user:tkwse1j5o0anqjxonvzx` has the table `user` and the key `tkwse1j5o0anqjxonvzx`.
	#[derive(Debug, Clone, PartialEq, PartialOrd)]
	pub struct RecordId(CoreThing)
);

impl RecordId {
	pub fn from_table_key<S, K>(table: S, key: K) -> Self
	where
		S: Into<String>,
		K: Into<RecordIdKey>,
	{
		let tb = table.into();
		let key = key.into();
		Self(CoreThing::from((tb, key.0)))
	}

	pub fn table(&self) -> &str {
		&self.0.tb
	}

	pub fn key(&self) -> &RecordIdKey {
		RecordIdKey::from_inner_ref(&self.0.id)
	}
}

impl FromStr for RecordId {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		syn::thing(s).map_err(crate::Error::Db).map(RecordId::from_inner)
	}
}

impl<S, I> From<(S, I)> for RecordId
where
	S: Into<String>,
	RecordIdKey: From<I>,
{
	fn from(value: (S, I)) -> Self {
		Self::from_table_key(value.0, value.1)
	}
}

transparent_wrapper!(
	/// The number type of surrealql.
	/// Can contain either a 64 bit float, 64 bit integer or a decimal.
	#[derive(Debug, Clone, PartialEq, PartialOrd)]
	pub struct Number(CoreNumber)
);

impl Number {
	pub fn cource_into_i64(self) -> Option<i64> {
		match self.0 {
			CoreNumber::Int(x) => Some(x),
			CoreNumber::Float(x) if x.fract() == x => Some(x as i64),
			CoreNumber::Decimal(x) => x.try_into().ok(),
			_ => None,
		}
	}
}

transparent_wrapper!(
	#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
	pub struct Value(CoreValue)
);

impl Serialize for Value {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.0.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for Value {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Ok(Self(Deserialize::deserialize(deserializer)?))
	}
}

impl Value {
	pub fn int(v: i64) -> Self {
		Self(CoreValue::Number(CoreNumber::Int(v)))
	}

	pub fn float(v: f64) -> Self {
		Self(CoreValue::Number(CoreNumber::Float(v)))
	}

	pub fn decimal(v: Decimal) -> Self {
		Self(CoreValue::Number(CoreNumber::Decimal(v)))
	}

	pub fn is_none(&self) -> bool {
		matches!(self.0, CoreValue::None)
	}

	#[doc = concat!("Return whether the value contains a ",stringify!(Vec<Value>),".")]
	pub fn is_array(&self) -> bool {
		matches!(&self.0, CoreValue::Array(_))
	}
	#[doc = concat!("Get a reference to the internal ",stringify!(Vec<Value>)," if the value is of that type")]
	pub fn as_array(&self) -> Option<&Vec<Value>> {
		if let CoreValue::Array(ref x) = self.0 {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			let res = unsafe { std::mem::transmute::<&Vec<CoreValue>, &Vec<Value>>(&x.0) };
			Some(res)
		} else {
			None
		}
	}
	#[doc = concat!("Get a reference to the internal ",stringify!(Vec<Value>)," if the value is of that type")]
	pub fn as_array_mut(&mut self) -> Option<&mut Vec<Value>> {
		if let CoreValue::Array(ref mut x) = self.0 {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			let res =
				unsafe { std::mem::transmute::<&mut Vec<CoreValue>, &mut Vec<Value>>(&mut x.0) };
			Some(res)
		} else {
			None
		}
	}
	#[doc = concat!("Convert the value to ",stringify!(Vec<Value>)," if the value is of that type")]
	pub fn into_array(self) -> Option<Vec<Value>> {
		if let CoreValue::Array(x) = self.0 {
			let res = unsafe { std::mem::transmute::<Vec<CoreValue>, Vec<Value>>(x.0) };
			Some(res)
		} else {
			None
		}
	}

	pub(crate) fn core_to_array(v: Vec<CoreValue>) -> Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<CoreValue>, Vec<Value>>(v)
		}
	}

	pub(crate) fn core_to_array_ref(v: &Vec<CoreValue>) -> &Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<CoreValue>, &Vec<Value>>(v)
		}
	}

	pub(crate) fn core_to_array_mut(v: &mut Vec<CoreValue>) -> &mut Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<CoreValue>, &mut Vec<Value>>(v)
		}
	}

	pub(crate) fn array_to_core(v: Vec<Value>) -> Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<Value>, Vec<CoreValue>>(v)
		}
	}

	pub(crate) fn array_to_core_ref(v: &Vec<Value>) -> &Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<Value>, &Vec<CoreValue>>(v)
		}
	}

	pub(crate) fn array_to_core_mut(v: &mut Vec<Value>) -> &mut Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<Value>, &mut Vec<CoreValue>>(v)
		}
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

impl<T> From<Vec<T>> for Value
where
	Value: From<T>,
{
	fn from(value: Vec<T>) -> Self {
		let mut array = CoreArray::default();
		let res = value.into_iter().map(Value::from).map(Value::into_inner).collect();
		array.0 = res;
		Value::from_inner(CoreValue::Array(array))
	}
}

macro_rules! impl_convert_wrapper(
	($(($variant:ident($ty:ty), $is:ident, $as:ident,$as_mut:ident, $into:ident)),*$(,)?) => {
		impl Value{

			$(
			#[doc = concat!("Return whether the value contains a ",stringify!($ty),".")]
			pub fn $is(&self) -> bool{
				matches!(&self.0, CoreValue::$variant(_))
			}

			#[doc = concat!("Get a reference to the internal ",stringify!($ty)," if the value is of that type")]
			pub fn $as(&self) -> Option<&$ty>{
				if let CoreValue::$variant(ref x) = self.0{
					Some(<$ty>::from_inner_ref(x))
				}else{
					None
				}
			}

			#[doc = concat!("Get a reference to the internal ",stringify!($ty)," if the value is of that type")]
			pub fn $as_mut(&mut self) -> Option<&mut $ty>{
				if let CoreValue::$variant(ref mut x) = self.0{
					Some(<$ty>::from_inner_mut(x))
				}else{
					None
				}
			}

			#[doc = concat!("Convert the value to ",stringify!($ty)," if the value is of that type")]
			pub fn $into(self) -> Option<$ty>{
				if let CoreValue::$variant(x) = self.0{
					Some(<$ty>::from_inner(x))
				}else{
					None
				}
			}
			)*
		}

		$(
		impl From<$ty> for Value {
			fn from(v: $ty) -> Self{
				Self(CoreValue::$variant(v.into_inner()))
			}
		}

		impl From<Option<$ty>> for Value {
			fn from(v: Option<$ty>) -> Self{
				if let Some(v) = v {
					Self(CoreValue::$variant(v.into_inner()))
				}else{
					Self(CoreValue::None)
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

macro_rules! impl_convert_primitive(
	($(($variant:ident($ty:ty), $is:ident, $as:ident,$as_mut:ident, $into:ident)),*$(,)?) => {
		impl Value{
			$(
			#[doc = concat!("Return whether the value contains a ",stringify!($ty),".")]
			pub fn $is(&self) -> bool{
				matches!(&self.0, CoreValue::$variant(_))
			}

			#[doc = concat!("Get a reference to the internal ",stringify!($ty)," if the value is of that type")]
			pub fn $as(&self) -> Option<&$ty>{
				if let CoreValue::$variant(ref x) = self.0{
					Some(<$ty>::from_inner_ref(x))
				}else{
					None
				}
			}

			#[doc = concat!("Get a reference to the internal ",stringify!($ty)," if the value is of that type")]
			pub fn $as_mut(&mut self) -> Option<&mut $ty>{
				if let CoreValue::$variant(ref mut x) = self.0{
					Some(<$ty>::from_inner_mut(x))
				}else{
					None
				}
			}

			#[doc = concat!("Convert the value to ",stringify!($ty)," if the value is of that type")]
			pub fn $into(self) -> Option<$ty>{
				if let CoreValue::$variant(x) = self.0{
					Some(<$ty>::from_inner(x))
				}else{
					None
				}
			}
			)*
		}

		$(
		impl From<$ty> for Value {
			fn from(v: $ty) -> Self{
				Self(CoreValue::$variant(v.into_inner()))
			}
		}

		impl From<Option<$ty>> for Value {
			fn from(v: Option<$ty>) -> Self{
				if let Some(v) = v {
					Self(CoreValue::$variant(v.into_inner()))
				}else{
					Self(CoreValue::None)
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

impl_convert_wrapper!(
	(Number(Number), is_number, as_number, as_number_mut, into_number),
	//(Uuid(Uuid), is_uuid, as_uuid, as_uuid_mut, into_uuid),
	//(Datetime(Datetime), is_datetime, as_datetime, as_datetime_mut, into_dateime),
	//(Bytes(Bytes), is_bytes, as_bytes, as_bytes_mut, into_bytes),
	(Thing(RecordId), is_record_id, as_record_id, as_record_id_mut, into_record_id),
	(Object(Object), is_object, as_object, as_object_mut, into_object),
);

impl_convert_primitive!(
	(Bool(bool), is_bool, as_bool, as_bool_mut, into_bool),
	(Strand(String), is_string, as_string, as_string_mut, into_string),
	(Duration(Duration), is_duration, as_duration, as_duration_mut, into_duration),
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

/// The action performed on a record
///
/// This is used in live query notifications.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Action {
	Create,
	Update,
	Delete,
}

/// A live query notification
///
/// Live queries return a stream of notifications. The notification contains an `action` that triggered the change in the database record and `data` itself.
/// For deletions the data is the record before it was deleted. For everything else, it's the newly created record or updated record depending on whether
/// the action is create or update.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub struct Notification<R> {
	pub query_id: Uuid,
	pub action: Action,
	pub data: R,
}

impl Notification<Value> {
	pub fn map_deserialize<R>(self) -> Result<Notification<R>, crate::api::Error>
	where
		R: DeserializeOwned,
	{
		let data = R::deserialize(self.data)?;
		Ok(Notification {
			query_id: self.query_id,
			action: self.action,
			data,
		})
	}
}
