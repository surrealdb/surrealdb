use crate::{Result, error::Api as ApiError};
use anyhow::anyhow;
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
	expr::{Array, Datetime, Id, Number, Object, Thing as RecordId, TryFromValue, Value},
	sql::SqlValue,
	syn,
};
use uuid::Uuid;

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
	/// The key of a [`RecordId`].
	#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
	#[non_exhaustive]
	pub struct RecordIdKey(Id)
);
impl_serialize_wrapper!(RecordIdKey);

impl From<Object> for RecordIdKey {
	fn from(object: Object) -> Self {
		Self::from_inner(Id::Object(object))
	}
}

impl TryFrom<RecordIdKey> for Object {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let Id::Object(object) = value.0 {
			Ok(object)
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
		Self(Id::String(value))
	}
}

impl TryFrom<RecordIdKey> for String {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let Id::String(x) = value.0 {
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
		Self(Id::String(value.clone()))
	}
}

impl From<&str> for RecordIdKey {
	fn from(value: &str) -> Self {
		Self(Id::String(value.to_owned()))
	}
}

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		Self(Id::Number(value))
	}
}

impl TryFrom<RecordIdKey> for i64 {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let Id::Number(x) = value.0 {
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
		Self(Id::Uuid(value.into()))
	}
}

impl TryFrom<RecordIdKey> for Uuid {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self> {
		if let Id::Uuid(x) = value.0 {
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
	fn from(values: Vec<Value>) -> Self {
		Self(Id::Array(Array(values)))
	}
}

#[expect(clippy::fallible_impl_from)]
impl From<RecordIdKey> for Value {
	fn from(key: RecordIdKey) -> Self {
		match key.0 {
			Id::String(x) => Value::from(x),
			Id::Number(x) => Value::from(x),
			Id::Object(x) => Value::from(x),
			Id::Array(x) => Value::from(x),
			Id::Uuid(x) => Value::from(x),
			_ => panic!("lib recieved generate variant of record id"),
		}
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

impl TryFromValue for RecordIdKey {
	fn try_from_value(key: Value) -> std::result::Result<Self, anyhow::Error> {
		match key {
			Value::Strand(x) => Ok(RecordIdKey::from_inner(Id::String(x.0))),
			Value::Number(Number::Int(x)) => Ok(RecordIdKey::from_inner(Id::Number(x))),
			Value::Object(x) => Ok(RecordIdKey::from_inner(Id::Object(x))),
			Value::Array(x) => Ok(RecordIdKey::from_inner(Id::Array(x))),
			_ => Err(anyhow!("failed to convert value into a record id key: {key}")),
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
