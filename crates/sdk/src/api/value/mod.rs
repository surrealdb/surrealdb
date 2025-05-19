use crate::{Result, error::Api as ApiError};
use chrono::{DateTime, Utc};
use revision::revisioned;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
	cmp::{Ordering, PartialEq, PartialOrd},
	fmt,
	ops::{Deref, Index},
	str::FromStr,
};
use surrealdb_core::{
	dbs::Action as CoreAction,
	expr::{
		Array as CoreArray, Datetime as CoreDatetime, Id as CoreId, Number as CoreNumber,
		Thing as CoreThing, Value as CoreValue,
	},
	syn,
};
use uuid::Uuid;

mod obj;
pub use obj::{IntoIter, Iter, IterMut, Object};

pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T> {
	surrealdb_core::expr::from_value(value.0)
}

pub fn to_value<T: Serialize + 'static>(value: T) -> Result<Value> {
	Ok(Value(surrealdb_core::expr::to_value(value)?))
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
	#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
	pub struct Datetime(CoreDatetime)
);
impl_serialize_wrapper!(Datetime);

impl From<DateTime<Utc>> for Datetime {
	fn from(v: DateTime<Utc>) -> Self {
		Self(v.into())
	}
}

transparent_wrapper!(
	/// The key of a [`RecordId`].
	#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
	#[non_exhaustive]
	pub struct RecordIdKey(CoreId)
);
impl_serialize_wrapper!(RecordIdKey);

impl From<Object> for RecordIdKey {
	fn from(value: Object) -> Self {
		Self::from_inner(CoreId::Object(value.into_inner()))
	}
}

impl TryFrom<RecordIdKey> for Object {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let CoreId::Object(x) = value.0 {
			Ok(Object::from_inner(x))
		} else {
			Err(anyhow::Error::new(ApiError::FromValue {
				value: value.into(),
				error: String::from("inner value is not an object"),
			}))
		}
	}
}

impl From<String> for RecordIdKey {
	fn from(value: String) -> Self {
		Self(CoreId::String(value))
	}
}

impl TryFrom<RecordIdKey> for String {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let CoreId::String(x) = value.0 {
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

impl TryFrom<RecordIdKey> for i64 {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let CoreId::Number(x) = value.0 {
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
		Self(CoreId::Uuid(value.into()))
	}
}

impl TryFrom<RecordIdKey> for Uuid {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let CoreId::Uuid(x) = value.0 {
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
		let mut array = CoreArray::default();
		array.0 = res;
		Self(CoreId::Array(array))
	}
}

#[expect(clippy::fallible_impl_from)]
impl From<RecordIdKey> for Value {
	fn from(key: RecordIdKey) -> Self {
		match key.0 {
			CoreId::String(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Number(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Object(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Array(x) => Value::from_inner(CoreValue::from(x)),
			CoreId::Uuid(x) => Value::from_inner(CoreValue::from(x)),
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
	#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self> {
		syn::thing(s).map(RecordId::from_inner)
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

transparent_wrapper!(
	#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
	pub struct Value(pub(crate) CoreValue)
);
impl_serialize_wrapper!(Value);

impl Value {
	#[expect(dead_code)]
	pub(crate) fn core_to_array(v: Vec<CoreValue>) -> Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<Vec<CoreValue>, Vec<Value>>(v)
		}
	}

	#[expect(dead_code)]
	pub(crate) fn core_to_array_ref(v: &Vec<CoreValue>) -> &Vec<Value> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<CoreValue>, &Vec<Value>>(v)
		}
	}

	#[expect(dead_code)]
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

	#[expect(dead_code)]
	pub(crate) fn array_to_core_ref(v: &Vec<Value>) -> &Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&Vec<Value>, &Vec<CoreValue>>(v)
		}
	}

	#[expect(dead_code)]
	pub(crate) fn array_to_core_mut(v: &mut Vec<Value>) -> &mut Vec<CoreValue> {
		unsafe {
			// SAFETY: Because Value is `repr(transparent)` transmuting between value and corevalue
			// is safe.
			std::mem::transmute::<&mut Vec<Value>, &mut Vec<CoreValue>>(v)
		}
	}
}

impl Index<usize> for Value {
	type Output = Self;

	fn index(&self, index: usize) -> &Self::Output {
		match &self.0 {
			CoreValue::Array(map) => {
				map.0.get(index).map(Self::from_inner_ref).unwrap_or(&Value(CoreValue::None))
			}
			_ => &Value(CoreValue::None),
		}
	}
}

impl Index<&str> for Value {
	type Output = Self;

	fn index(&self, index: &str) -> &Self::Output {
		match &self.0 {
			CoreValue::Object(map) => {
				map.0.get(index).map(Self::from_inner_ref).unwrap_or(&Value(CoreValue::None))
			}
			_ => &Value(CoreValue::None),
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
			Value(CoreValue::None) => None,
			v => Some(v),
		}
	}

	/// Checks to see if a Value is a Value::None.
	pub fn is_none(&self) -> bool {
		matches!(&self, Value(CoreValue::None))
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
	pub fn map_deserialize<R>(self) -> Result<Notification<R>>
	where
		R: DeserializeOwned,
	{
		let data = surrealdb_core::expr::from_value(self.data)?;
		Ok(Notification {
			query_id: self.query_id,
			action: self.action,
			data,
		})
	}
}
