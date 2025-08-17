use std::cmp::{Ordering, PartialEq, PartialOrd};
use std::fmt;
use std::ops::{Deref, Index};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use revision::revisioned;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use surrealdb_core::dbs::Action as CoreAction;
use surrealdb_core::{syn, val};
use uuid::Uuid;

use crate::Result;
use crate::error::Api as ApiError;

mod convert;
pub(crate) use convert::{from_value as from_core_value, to_value as to_core_value};
mod obj;
pub use obj::{IntoIter, Iter, IterMut, Object};

pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T> {
	convert::from_value(value.0)
}

pub fn to_value<T: Serialize + 'static>(value: T) -> Result<Value> {
	convert::to_value(value).map(Value)
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
	#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
	pub struct Datetime(val::Datetime)
);
impl_serialize_wrapper!(Datetime);

impl Datetime {
	pub fn now() -> Self {
		Datetime(val::Datetime::now())
	}
}

impl From<DateTime<Utc>> for Datetime {
	fn from(v: DateTime<Utc>) -> Self {
		Self(v.into())
	}
}

transparent_wrapper!(
	/// The key of a [`RecordId`].
	#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
	#[non_exhaustive]
	pub struct RecordIdKey(val::RecordIdKey)
);
impl_serialize_wrapper!(RecordIdKey);

impl From<Object> for RecordIdKey {
	fn from(value: Object) -> Self {
		Self::from_inner(val::RecordIdKey::Object(value.into_inner()))
	}
}

impl TryFrom<RecordIdKey> for Object {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let val::RecordIdKey::Object(x) = value.0 {
			Ok(Object::from_inner(x))
		} else {
			Err(anyhow::Error::new(ApiError::FromValue {
				value: value.into(),
				error: String::from("inner value is not an object"),
			}))
		}
	}
}

//TODO: Null byte validity
impl From<String> for RecordIdKey {
	fn from(value: String) -> Self {
		Self(val::RecordIdKey::String(value))
	}
}

impl TryFrom<RecordIdKey> for String {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let val::RecordIdKey::String(x) = value.0 {
			Ok(x)
		} else {
			Err(anyhow::Error::new(ApiError::FromValue {
				value: value.into(),
				error: String::from("inner value is not a string"),
			}))
		}
	}
}

impl From<&String> for RecordIdKey {
	fn from(value: &String) -> Self {
		Self(val::RecordIdKey::String(value.clone()))
	}
}

impl From<&str> for RecordIdKey {
	fn from(value: &str) -> Self {
		Self(val::RecordIdKey::String(value.to_owned()))
	}
}

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		Self(val::RecordIdKey::Number(value))
	}
}

impl TryFrom<RecordIdKey> for i64 {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let val::RecordIdKey::Number(x) = value.0 {
			Ok(x)
		} else {
			Err(anyhow::Error::new(ApiError::FromValue {
				value: value.into(),
				error: String::from("inner value is not a number"),
			}))
		}
	}
}

impl From<Uuid> for RecordIdKey {
	fn from(value: Uuid) -> Self {
		Self(val::RecordIdKey::Uuid(value.into()))
	}
}

impl TryFrom<RecordIdKey> for Uuid {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let val::RecordIdKey::Uuid(x) = value.0 {
			Ok(*x)
		} else {
			Err(anyhow::Error::new(ApiError::FromValue {
				value: value.into(),
				error: String::from("inner value is not a UUID"),
			}))
		}
	}
}

impl From<Vec<Value>> for RecordIdKey {
	fn from(value: Vec<Value>) -> Self {
		let res = Value::array_to_core(value);
		Self(val::RecordIdKey::Array(val::Array(res)))
	}
}

#[expect(clippy::fallible_impl_from)]
impl From<RecordIdKey> for Value {
	fn from(key: RecordIdKey) -> Self {
		match key.0 {
			val::RecordIdKey::String(x) => Value::from_inner(val::Value::from(x)),
			val::RecordIdKey::Number(x) => Value::from_inner(val::Value::from(x)),
			val::RecordIdKey::Object(x) => Value::from_inner(val::Value::from(x)),
			val::RecordIdKey::Array(x) => Value::from_inner(val::Value::from(x)),
			val::RecordIdKey::Uuid(x) => Value::from_inner(val::Value::from(x)),
			_ => panic!("lib recieved generate variant of record id"),
		}
	}
}

impl From<RecordId> for Value {
	fn from(key: RecordId) -> Self {
		Value::from_inner(val::Value::RecordId(key.0))
	}
}

impl FromStr for Value {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self> {
		Ok(Value::from_inner(surrealdb_core::syn::value(s)?))
	}
}

#[derive(Debug)]
pub struct RecordIdKeyFromValueError(());

impl fmt::Display for RecordIdKeyFromValueError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(
			f,
			"tried to convert a value to a record id key with a value type that is not allowed in a record id key"
		)
	}
}

impl TryFrom<Value> for RecordIdKey {
	type Error = RecordIdKeyFromValueError;

	fn try_from(key: Value) -> std::result::Result<Self, Self::Error> {
		match key.0 {
			val::Value::Strand(x) => {
				Ok(RecordIdKey::from_inner(val::RecordIdKey::String(x.into_string())))
			}
			val::Value::Number(val::Number::Int(x)) => {
				Ok(RecordIdKey::from_inner(val::RecordIdKey::Number(x)))
			}
			val::Value::Object(x) => Ok(RecordIdKey::from_inner(val::RecordIdKey::Object(x))),
			val::Value::Array(x) => Ok(RecordIdKey::from_inner(val::RecordIdKey::Array(x))),
			_ => Err(RecordIdKeyFromValueError(())),
		}
	}
}

transparent_wrapper!(
	/// Struct representing a record id.
	///
	/// Record id's consist of a table name and a key.
	/// For example the record id `user:tkwse1j5o0anqjxonvzx` has the table `user` and the key `tkwse1j5o0anqjxonvzx`.
	#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
	pub struct RecordId(val::RecordId)
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
		Self(val::RecordId::new(tb, key.0))
	}

	pub fn table(&self) -> &str {
		&self.0.table
	}

	pub fn key(&self) -> &RecordIdKey {
		RecordIdKey::from_inner_ref(&self.0.key)
	}
}

impl FromStr for RecordId {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self> {
		syn::record_id(s).map(RecordId::from_inner)
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
	pub struct Number(val::Number)
);
impl_serialize_wrapper!(Number);

transparent_wrapper!(
	#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
	pub struct Value(pub(crate) val::Value)
);
impl_serialize_wrapper!(Value);

impl Value {
	#[expect(dead_code)]
	pub(crate) fn core_to_array(v: Vec<val::Value>) -> Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<val::Value>, Vec<Value>>(v)
		}
	}

	#[expect(dead_code)]
	pub(crate) fn core_to_array_ref(v: &Vec<val::Value>) -> &Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<val::Value>, &Vec<Value>>(v)
		}
	}

	#[expect(dead_code)]
	pub(crate) fn core_to_array_mut(v: &mut Vec<val::Value>) -> &mut Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<val::Value>, &mut Vec<Value>>(v)
		}
	}

	pub(crate) fn array_to_core(v: Vec<Value>) -> Vec<val::Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<Value>, Vec<val::Value>>(v)
		}
	}

	#[expect(dead_code)]
	pub(crate) fn array_to_core_ref(v: &Vec<Value>) -> &Vec<val::Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<Value>, &Vec<val::Value>>(v)
		}
	}

	#[expect(dead_code)]
	pub(crate) fn array_to_core_mut(v: &mut Vec<Value>) -> &mut Vec<val::Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<Value>, &mut Vec<val::Value>>(v)
		}
	}
}

impl Index<usize> for Value {
	type Output = Self;

	fn index(&self, index: usize) -> &Self::Output {
		match &self.0 {
			val::Value::Array(map) => {
				map.0.get(index).map(Self::from_inner_ref).unwrap_or(&Value(val::Value::None))
			}
			_ => &Value(val::Value::None),
		}
	}
}

impl Index<&str> for Value {
	type Output = Self;

	fn index(&self, index: &str) -> &Self::Output {
		match &self.0 {
			val::Value::Object(map) => {
				map.0.get(index).map(Self::from_inner_ref).unwrap_or(&Value(val::Value::None))
			}
			_ => &Value(val::Value::None),
		}
	}
}

impl Value {
	/// Accesses the value found at a certain field
	/// if an object, and a certain index if an array.
	/// Will not err if no value is found at this point,
	/// instead returning a Value::None. If an Option<&Value>
	/// is desired, the .into_option() method can be used
	/// to perform the conversion.
	pub fn get<Idx>(&self, index: Idx) -> &Value
	where
		Value: Index<Idx, Output = Value>,
	{
		self.index(index)
	}

	/// Converts a Value into an Option<&Value>, returning
	/// a Some in all cases except Value::None.
	pub fn into_option(&self) -> Option<&Value> {
		match self {
			Value(val::Value::None) => None,
			v => Some(v),
		}
	}

	/// Checks to see if a Value is a Value::None.
	pub fn is_none(&self) -> bool {
		matches!(&self, Value(val::Value::None))
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
	#[allow(dead_code, reason = "Used by other engines except the HTTP one")]
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
/// Live queries return a stream of notifications. The notification contains an `action` that
/// triggered the change in the database record and `data` itself. For deletions the data is the
/// record before it was deleted. For everything else, it's the newly created record or updated
/// record depending on whether the action is create or update.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub struct Notification<R> {
	pub query_id: Uuid,
	pub action: Action,
	pub data: R,
}
