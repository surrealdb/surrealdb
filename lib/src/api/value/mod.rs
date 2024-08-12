use crate::Error;
use revision::revisioned;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
	cmp::{Ordering, PartialEq, PartialOrd},
	fmt,
	ops::Deref,
	str::FromStr,
};
use surrealdb_core::{
	dbs::Action as CoreAction,
	sql::{
		Array as CoreArray, Datetime as CoreDatetime, Id as CoreId, Number as CoreNumber,
		Thing as CoreThing, Value as CoreValue,
	},
	syn,
};
use uuid::Uuid;

mod obj;
pub use obj::{IntoIter, Iter, IterMut, Object};

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
	#[derive( Clone, Eq, PartialEq, Ord, PartialOrd)]
	pub struct Datetime(CoreDatetime)
);

transparent_wrapper!(
	/// The key of a [`RecordId`].
	#[derive( Clone, PartialEq, PartialOrd)]
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
			CoreId::String(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Number(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Object(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Array(x) => Value::from_inner(CoreValue::from(x)),
			_ => panic!("lib recieved generate variant of record id"),
		}
	}
}

impl From<RecordId> for Value {
	fn from(key: RecordId) -> Self {
		Value::from_inner(CoreValue::Thing(key.0))
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
	#[derive( Clone, PartialEq, PartialOrd)]
	pub struct RecordId(CoreThing)
);
impl_serialize_wrapper!(RecordId);

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
	#[derive( Clone, PartialEq, PartialOrd)]
	pub struct Number(CoreNumber)
);
impl_serialize_wrapper!(Number);

impl Number {
	#[doc(hidden)]
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
	#[derive( Clone, Default, PartialEq, PartialOrd)]
	pub struct Value(pub(crate) CoreValue)
);
impl_serialize_wrapper!(Value);

impl Value {
	// TODO: Check if all of theses are actually used.
	#[allow(dead_code)]
	pub(crate) fn core_to_array(v: Vec<CoreValue>) -> Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<CoreValue>, Vec<Value>>(v)
		}
	}

	#[allow(dead_code)]
	pub(crate) fn core_to_array_ref(v: &Vec<CoreValue>) -> &Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<CoreValue>, &Vec<Value>>(v)
		}
	}

	#[allow(dead_code)]
	pub(crate) fn core_to_array_mut(v: &mut Vec<CoreValue>) -> &mut Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<CoreValue>, &mut Vec<Value>>(v)
		}
	}

	#[allow(dead_code)]
	pub(crate) fn array_to_core(v: Vec<Value>) -> Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<Value>, Vec<CoreValue>>(v)
		}
	}

	#[allow(dead_code)]
	pub(crate) fn array_to_core_ref(v: &Vec<Value>) -> &Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<Value>, &Vec<CoreValue>>(v)
		}
	}

	#[allow(dead_code)]
	pub(crate) fn array_to_core_mut(v: &mut Vec<Value>) -> &mut Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<Value>, &mut Vec<CoreValue>>(v)
		}
	}
}

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

impl Action {
	pub(crate) fn from_core(action: CoreAction) -> Self {
		match action {
			CoreAction::Create => Self::Create,
			CoreAction::Update => Self::Update,
			CoreAction::Delete => Self::Delete,
			_ => panic!("unimplemented variant of action"),
		}
	}
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

impl Notification<CoreValue> {
	pub fn map_deserialize<R>(self) -> Result<Notification<R>, crate::api::Error>
	where
		R: DeserializeOwned,
	{
		let data = surrealdb_core::sql::from_value(self.data).map_err(|e| {
			crate::api::Error::FromValue {
				value: Value::from_inner(e.value),
				error: e.error,
			}
		})?;
		Ok(Notification {
			query_id: self.query_id,
			action: self.action,
			data,
		})
	}
}
