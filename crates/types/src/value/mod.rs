/// Array value types for SurrealDB
pub mod array;
/// Binary data value types for SurrealDB
pub mod bytes;
/// Datetime value types for SurrealDB
pub mod datetime;
/// Duration value types for SurrealDB
pub mod duration;
/// File reference value types for SurrealDB
pub mod file;
/// Geometric value types for SurrealDB
pub mod geometry;
/// JSON value types for SurrealDB
pub mod into_json;
/// Numeric value types for SurrealDB
pub mod number;
/// Object value types for SurrealDB
pub mod object;
/// Range value types for SurrealDB
pub mod range;
/// Record identifier value types for SurrealDB
pub mod record_id;
/// Regular expression value types for SurrealDB
pub mod regex;
/// UUID value types for SurrealDB
pub mod uuid;

use std::cmp::Ordering;
use std::ops::Index;

use revision::revisioned;
pub use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use self::array::Array;
pub use self::bytes::Bytes;
pub use self::datetime::Datetime;
pub use self::duration::Duration;
pub use self::file::File;
pub use self::geometry::Geometry;
pub use self::number::Number;
pub use self::object::Object;
pub use self::range::Range;
pub use self::record_id::{RecordId, RecordIdKey, RecordIdKeyRange};
pub use self::regex::Regex;
pub use self::uuid::Uuid;
use crate::{Kind, SurrealValue};

/// Marker type for value conversions from Value::None
///
/// This type represents the absence of a value in SurrealDB.
/// It is used as a marker type for type-safe conversions.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct SurrealNone;

/// Marker type for value conversions from Value::Null
///
/// This type represents a null value in SurrealDB.
/// It is used as a marker type for type-safe conversions.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct SurrealNull;

/// Represents a value in SurrealDB
///
/// This enum contains all possible value types that can be stored in SurrealDB.
/// Each variant corresponds to a different data type supported by the database.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub enum Value {
	/// Represents the absence of a value
	#[default]
	None,
	/// Represents a null value
	Null,
	/// A boolean value (true or false)
	Bool(bool),
	/// A numeric value (integer, float, or decimal)
	Number(Number),
	/// A string value
	String(String),
	/// A duration value representing a time span
	Duration(Duration),
	/// A datetime value representing a point in time
	Datetime(Datetime),
	/// A UUID value
	Uuid(Uuid),
	/// An array of values
	Array(Array),
	/// An object containing key-value pairs
	Object(Object),
	/// A geometric value (point, line, polygon, etc.)
	Geometry(Geometry),
	/// Binary data
	Bytes(Bytes),
	/// A record identifier
	RecordId(RecordId),
	/// A file reference
	File(File),
	/// A range of values
	Range(Box<Range>),
	/// A regular expression
	Regex(Regex),
}

impl Eq for Value {}

impl PartialOrd for Value {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}

impl Value {
	/// Returns the kind of this value
	///
	/// This method maps each value variant to its corresponding `Kind`.
	pub fn kind(&self) -> Kind {
		match self {
			Value::None => Kind::None,
			Value::Null => Kind::Null,
			Value::Bool(_) => Kind::Bool,
			Value::Number(_) => Kind::Number,
			Value::String(_) => Kind::String,
			Value::Duration(_) => Kind::Duration,
			Value::Datetime(_) => Kind::Datetime,
			Value::Uuid(_) => Kind::Uuid,
			Value::Array(_) => Kind::Array(Box::new(Kind::Any), None),
			Value::Object(_) => Kind::Object,
			Value::Geometry(_) => Kind::Geometry(Vec::new()),
			Value::Bytes(_) => Kind::Bytes,
			Value::RecordId(_) => Kind::Record(Vec::new()),
			Value::File(_) => Kind::File(Vec::new()),
			Value::Range(_) => Kind::Range,
			Value::Regex(_) => Kind::Regex,
		}
	}

	/// Returns the first value in the array.
	///
	/// Returns `None` if the value is not an array or the array is empty.
	pub fn first(&self) -> Option<Value> {
		match self {
			Value::Array(arr) => arr.first().cloned(),
			_ => None,
		}
	}

	/// Check if this Value is NONE or NULL
	pub fn is_nullish(&self) -> bool {
		matches!(self, Value::None | Value::Null)
	}

	/// Accesses the value found at a certain field
	/// if an object, and a certain index if an array.
	/// Will not err if no value is found at this point,
	/// instead returning a Value::None. If an Option<&Value>
	/// is desired, the .into_option() method can be used
	/// to perform the conversion.
	pub fn get<Idx>(&self, index: Idx) -> &Value
	where
		Value: Indexable<Idx>,
	{
		Indexable::get(self, index)
	}

	/// Removes the value at the given index.
	///
	/// Returns `Some(Value)` if the value was removed, `None` if the value was not found.
	pub fn remove<Idx>(&mut self, index: Idx) -> Value
	where
		Value: Indexable<Idx>,
	{
		Indexable::remove(self, index)
	}

	/// Checks if this value is of the specified type
	///
	/// Returns `true` if the value can be converted to the given type `T`.
	pub fn is<T: SurrealValue>(&self) -> bool {
		T::is_value(self)
	}

	/// Converts this value to the specified type
	///
	/// Returns `Ok(T)` if the conversion is successful, `Err(anyhow::Error)` otherwise.
	pub fn into_t<T: SurrealValue>(self) -> anyhow::Result<T> {
		T::from_value(self)
	}

	/// Creates a value from the specified type
	///
	/// Converts the given value of type `T` into a `Value`.
	pub fn from_t<T: SurrealValue>(value: T) -> Value {
		value.into_value()
	}
}

impl Index<usize> for Value {
	type Output = Self;

	fn index(&self, index: usize) -> &Self::Output {
		match &self {
			Value::Array(map) => map.0.get(index).unwrap_or(&Value::None),
			_ => &Value::None,
		}
	}
}

impl Index<&str> for Value {
	type Output = Self;

	fn index(&self, index: &str) -> &Self::Output {
		match &self {
			Value::Object(map) => map.0.get(index).unwrap_or(&Value::None),
			_ => &Value::None,
		}
	}
}

impl PartialEq<&Value> for Value {
	fn eq(&self, other: &&Value) -> bool {
		self == *other
	}
}

impl PartialEq<Value> for &Value {
	fn eq(&self, other: &Value) -> bool {
		**self == *other
	}
}

/// Trait for values that can be indexed
pub trait Indexable<Idx> {
	/// Get the value at the given index.
	fn get(&self, index: Idx) -> &Value;
	/// Remove the value at the given index.
	fn remove(&mut self, index: Idx) -> Value;
}

impl Indexable<usize> for Value {
	fn get(&self, index: usize) -> &Value {
		match self {
			Value::Array(arr) => arr.index(index),
			_ => &Value::None,
		}
	}
	fn remove(&mut self, index: usize) -> Value {
		match self {
			Value::Array(arr) => arr.remove(index),
			_ => Value::None,
		}
	}
}

impl Indexable<&str> for Value {
	fn get(&self, index: &str) -> &Value {
		match self {
			Value::Object(obj) => match obj.get(index) {
				Some(v) => v,
				None => &Value::None,
			},
			_ => &Value::None,
		}
	}

	fn remove(&mut self, index: &str) -> Value {
		match self {
			Value::Object(obj) => match obj.remove(index) {
				Some(v) => v,
				None => Value::None,
			},
			_ => Value::None,
		}
	}
}
