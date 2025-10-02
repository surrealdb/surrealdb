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
use std::fmt::{self, Display, Write};
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
use crate::sql::ToSql;
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
		match (self, other) {
			// Same variant comparisons - delegate to inner type
			(Value::None, Value::None) => Ordering::Equal,
			(Value::Null, Value::Null) => Ordering::Equal,
			(Value::Bool(a), Value::Bool(b)) => a.cmp(b),
			(Value::Number(a), Value::Number(b)) => a.cmp(b),
			(Value::String(a), Value::String(b)) => a.cmp(b),
			(Value::Duration(a), Value::Duration(b)) => a.cmp(b),
			(Value::Datetime(a), Value::Datetime(b)) => a.cmp(b),
			(Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
			(Value::Array(a), Value::Array(b)) => a.cmp(b),
			(Value::Object(a), Value::Object(b)) => a.cmp(b),
			(Value::Geometry(a), Value::Geometry(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
			(Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
			(Value::RecordId(a), Value::RecordId(b)) => a.cmp(b),
			(Value::File(a), Value::File(b)) => a.cmp(b),
			(Value::Range(a), Value::Range(b)) => a.cmp(b),
			(Value::Regex(a), Value::Regex(b)) => a.cmp(b),

			// Different variant types - define a total order
			// Order: None < Null < Bool < Number < String < Duration < Datetime < Uuid
			//        < Array < Object < Geometry < Bytes < RecordId < File < Range < Regex
			(Value::None, _) => Ordering::Less,
			(_, Value::None) => Ordering::Greater,

			(Value::Null, _) => Ordering::Less,
			(_, Value::Null) => Ordering::Greater,

			(
				Value::Bool(_),
				Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Bool(_),
			) => Ordering::Greater,

			(
				Value::Number(_),
				Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Number(_),
			) => Ordering::Greater,

			(
				Value::String(_),
				Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::String(_),
			) => Ordering::Greater,

			(
				Value::Duration(_),
				Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Duration(_),
			) => Ordering::Greater,

			(
				Value::Datetime(_),
				Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Uuid(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Datetime(_),
			) => Ordering::Greater,

			(
				Value::Uuid(_),
				Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Array(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Uuid(_),
			) => Ordering::Greater,

			(
				Value::Array(_),
				Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Array(_),
			) => Ordering::Greater,

			(
				Value::Object(_),
				Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Object(_),
			) => Ordering::Greater,

			(
				Value::Geometry(_),
				Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Geometry(_),
			) => Ordering::Greater,

			(
				Value::Bytes(_),
				Value::RecordId(_) | Value::File(_) | Value::Range(_) | Value::Regex(_),
			) => Ordering::Less,
			(
				Value::RecordId(_) | Value::File(_) | Value::Range(_) | Value::Regex(_),
				Value::Bytes(_),
			) => Ordering::Greater,

			(Value::RecordId(_), Value::File(_) | Value::Range(_) | Value::Regex(_)) => {
				Ordering::Less
			}
			(Value::File(_) | Value::Range(_) | Value::Regex(_), Value::RecordId(_)) => {
				Ordering::Greater
			}

			(Value::File(_), Value::Range(_) | Value::Regex(_)) => Ordering::Less,
			(Value::Range(_) | Value::Regex(_), Value::File(_)) => Ordering::Greater,

			(Value::Range(_), Value::Regex(_)) => Ordering::Less,
			(Value::Regex(_), Value::Range(_)) => Ordering::Greater,
		}
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

	/// Check if this Value is empty.
	pub fn is_empty(&self) -> bool {
		match self {
			Value::None => true,
			Value::Null => true,
			Value::String(s) => s.is_empty(),
			Value::Bytes(b) => b.is_empty(),
			Value::Object(obj) => obj.is_empty(),
			Value::Array(arr) => arr.is_empty(),
			_ => false,
		}
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

impl FromIterator<Value> for Value {
	fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
		Value::Array(Array::from_iter(iter))
	}
}

impl Display for Value {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Value::None => write!(f, "NONE"),
			Value::Null => write!(f, "NULL"),
			Value::Array(v) => write!(f, "{v}"),
			Value::Bool(v) => write!(f, "{v}"),
			Value::Bytes(v) => write!(f, "{v}"),
			Value::Datetime(v) => write!(f, "{v}"),
			Value::Duration(v) => write!(f, "{v}"),
			Value::Geometry(v) => write!(f, "{v}"),
			Value::Number(v) => write!(f, "{v}"),
			Value::Object(v) => write!(f, "{v}"),
			Value::Range(v) => write!(f, "{v}"),
			Value::Regex(v) => write!(f, "{v}"),
			Value::String(v) => write!(f, "{v}"),
			Value::RecordId(v) => write!(f, "{v}"),
			Value::Uuid(v) => write!(f, "{v}"),
			Value::File(v) => write!(f, "{v}"),
		}
	}
}

impl ToSql for Value {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		match self {
			Value::None => f.write_str("NONE"),
			Value::Null => f.write_str("NULL"),
			Value::Bool(v) => v.fmt_sql(f),
			Value::Number(v) => v.fmt_sql(f),
			Value::String(v) => v.fmt_sql(f),
			Value::Duration(v) => v.fmt_sql(f),
			Value::Datetime(v) => v.fmt_sql(f),
			Value::Uuid(v) => v.fmt_sql(f),
			Value::Array(v) => v.fmt_sql(f),
			Value::Object(v) => v.fmt_sql(f),
			Value::Geometry(v) => v.fmt_sql(f),
			Value::Bytes(v) => v.fmt_sql(f),
			Value::RecordId(v) => v.fmt_sql(f),
			Value::File(v) => v.fmt_sql(f),
			Value::Range(v) => v.fmt_sql(f),
			Value::Regex(v) => v.fmt_sql(f),
		}
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	#[rstest]
	#[case::none(Value::None, true)]
	#[case::null(Value::Null, true)]
	#[case::string(Value::String("".to_string()), true)]
	#[case::string(Value::String("hello".to_string()), false)]
	#[case::bytes(Value::Bytes(Bytes::default()), true)]
	#[case::bytes(Value::Bytes(Bytes::new(vec![1, 2, 3])), false)]
	#[case::object(Value::Object(Object::default()), true)]
	#[case::object(Value::Object(Object::from_iter([("key".to_string(), Value::String("value".to_string()))])), false)]
	#[case::array(Value::Array(Array::new()), true)]
	#[case::array(Value::Array(Array::from_values(vec![Value::String("hello".to_string())])), false)]
	#[case::geometry(Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))), false)]
	#[case::record_id(Value::RecordId(RecordId::new("test", "key")), false)]
	#[case::file(Value::File(File::default()), false)]
	#[case::range(Value::Range(Box::new(Range::unbounded())), false)]
	#[case::regex(Value::Regex("hello".parse().unwrap()), false)]
	#[case::duration(Value::Duration(Duration::new(1, 0)), false)]
	#[case::datetime(Value::Datetime(Datetime::from_timestamp(1, 0).unwrap()), false)]
	#[case::uuid(Value::Uuid(Uuid::new_v4()), false)]
	#[case::bool(Value::Bool(true), false)]
	#[case::bool(Value::Bool(false), false)]
	#[case::number(Value::Number(Number::Int(1)), false)]
	#[case::number(Value::Number(Number::Float(1.0)), false)]
	#[case::number(Value::Number(Number::Decimal(Decimal::new(1, 0))), false)]
	fn test_is_empty(#[case] value: Value, #[case] expected: bool) {
		assert_eq!(value.is_empty(), expected);
	}

	// Test ordering between same variants
	#[rstest]
	#[case::none_eq_none(Value::None, Value::None, Ordering::Equal)]
	#[case::null_eq_null(Value::Null, Value::Null, Ordering::Equal)]
	#[case::bool_false_lt_true(Value::Bool(false), Value::Bool(true), Ordering::Less)]
	#[case::bool_true_gt_false(Value::Bool(true), Value::Bool(false), Ordering::Greater)]
	#[case::bool_true_eq_true(Value::Bool(true), Value::Bool(true), Ordering::Equal)]
	#[case::string_a_lt_b(Value::String("a".to_string()), Value::String("b".to_string()), Ordering::Less)]
	#[case::string_b_gt_a(Value::String("b".to_string()), Value::String("a".to_string()), Ordering::Greater)]
	#[case::string_a_eq_a(Value::String("a".to_string()), Value::String("a".to_string()), Ordering::Equal)]
	fn test_value_ordering_same_variant(
		#[case] left: Value,
		#[case] right: Value,
		#[case] expected: Ordering,
	) {
		assert_eq!(left.cmp(&right), expected);
		assert_eq!(left.partial_cmp(&right), Some(expected));
	}

	// Test ordering between numbers (cross-type numeric comparisons)
	#[rstest]
	#[case::int_0_lt_int_1(
		Value::Number(Number::Int(0)),
		Value::Number(Number::Int(1)),
		Ordering::Less
	)]
	#[case::int_1_gt_int_0(
		Value::Number(Number::Int(1)),
		Value::Number(Number::Int(0)),
		Ordering::Greater
	)]
	#[case::int_5_eq_int_5(
		Value::Number(Number::Int(5)),
		Value::Number(Number::Int(5)),
		Ordering::Equal
	)]
	#[case::float_0_lt_float_1(
		Value::Number(Number::Float(0.0)),
		Value::Number(Number::Float(1.0)),
		Ordering::Less
	)]
	#[case::int_5_lt_float_5_5(
		Value::Number(Number::Int(5)),
		Value::Number(Number::Float(5.5)),
		Ordering::Less
	)]
	#[case::int_5_eq_float_5(
		Value::Number(Number::Int(5)),
		Value::Number(Number::Float(5.0)),
		Ordering::Equal
	)]
	#[case::float_5_eq_int_5(
		Value::Number(Number::Float(5.0)),
		Value::Number(Number::Int(5)),
		Ordering::Equal
	)]
	#[case::int_10_gt_float_9_9(
		Value::Number(Number::Int(10)),
		Value::Number(Number::Float(9.9)),
		Ordering::Greater
	)]
	#[case::decimal_5_eq_int_5(
		Value::Number(Number::Decimal(Decimal::new(5, 0))),
		Value::Number(Number::Int(5)),
		Ordering::Equal
	)]
	#[case::int_5_eq_decimal_5(
		Value::Number(Number::Int(5)),
		Value::Number(Number::Decimal(Decimal::new(5, 0))),
		Ordering::Equal
	)]
	#[case::decimal_5_5_gt_int_5(
		Value::Number(Number::Decimal(Decimal::new(55, 1))), // 5.5
		Value::Number(Number::Int(5)),
		Ordering::Greater
	)]
	fn test_value_ordering_numbers(
		#[case] left: Value,
		#[case] right: Value,
		#[case] expected: Ordering,
	) {
		assert_eq!(left.cmp(&right), expected);
		assert_eq!(left.partial_cmp(&right), Some(expected));
	}

	// Test ordering between different variant types
	// Order: None < Null < Bool < Number < String < Duration < Datetime < Uuid
	//        < Array < Object < Geometry < Bytes < RecordId < File < Range < Regex
	#[rstest]
	#[case::none_lt_null(Value::None, Value::Null, Ordering::Less)]
	#[case::null_gt_none(Value::Null, Value::None, Ordering::Greater)]
	#[case::null_lt_bool(Value::Null, Value::Bool(false), Ordering::Less)]
	#[case::bool_gt_null(Value::Bool(true), Value::Null, Ordering::Greater)]
	#[case::bool_lt_number(Value::Bool(true), Value::Number(Number::Int(0)), Ordering::Less)]
	#[case::number_gt_bool(Value::Number(Number::Int(100)), Value::Bool(true), Ordering::Greater)]
	#[case::number_lt_string(
		Value::Number(Number::Int(100)),
		Value::String("a".to_string()),
		Ordering::Less
	)]
	#[case::string_gt_number(
		Value::String("a".to_string()),
		Value::Number(Number::Int(100)),
		Ordering::Greater
	)]
	#[case::string_lt_duration(
		Value::String("z".to_string()),
		Value::Duration(Duration::new(1, 0)),
		Ordering::Less
	)]
	#[case::duration_gt_string(
		Value::Duration(Duration::new(1, 0)),
		Value::String("z".to_string()),
		Ordering::Greater
	)]
	#[case::duration_lt_datetime(
		Value::Duration(Duration::new(100, 0)),
		Value::Datetime(Datetime::from_timestamp(1, 0).unwrap()),
		Ordering::Less
	)]
	#[case::datetime_gt_duration(
		Value::Datetime(Datetime::from_timestamp(1, 0).unwrap()),
		Value::Duration(Duration::new(100, 0)),
		Ordering::Greater
	)]
	#[case::datetime_lt_uuid(
		Value::Datetime(Datetime::from_timestamp(1000000, 0).unwrap()),
		Value::Uuid(Uuid::new_v4()),
		Ordering::Less
	)]
	#[case::uuid_gt_datetime(
		Value::Uuid(Uuid::new_v4()),
		Value::Datetime(Datetime::from_timestamp(1000000, 0).unwrap()),
		Ordering::Greater
	)]
	#[case::uuid_lt_array(Value::Uuid(Uuid::new_v4()), Value::Array(Array::new()), Ordering::Less)]
	#[case::array_gt_uuid(
		Value::Array(Array::new()),
		Value::Uuid(Uuid::new_v4()),
		Ordering::Greater
	)]
	#[case::array_lt_object(
		Value::Array(Array::new()),
		Value::Object(Object::default()),
		Ordering::Less
	)]
	#[case::object_gt_array(
		Value::Object(Object::default()),
		Value::Array(Array::new()),
		Ordering::Greater
	)]
	#[case::object_lt_geometry(
		Value::Object(Object::default()),
		Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
		Ordering::Less
	)]
	#[case::geometry_gt_object(
		Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
		Value::Object(Object::default()),
		Ordering::Greater
	)]
	#[case::geometry_lt_bytes(
		Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
		Value::Bytes(Bytes::default()),
		Ordering::Less
	)]
	#[case::bytes_gt_geometry(
		Value::Bytes(Bytes::default()),
		Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
		Ordering::Greater
	)]
	#[case::bytes_lt_record(
		Value::Bytes(Bytes::default()),
		Value::RecordId(RecordId::new("test", "key")),
		Ordering::Less
	)]
	#[case::record_gt_bytes(
		Value::RecordId(RecordId::new("test", "key")),
		Value::Bytes(Bytes::default()),
		Ordering::Greater
	)]
	#[case::record_lt_file(
		Value::RecordId(RecordId::new("test", "key")),
		Value::File(File::default()),
		Ordering::Less
	)]
	#[case::file_gt_record(
		Value::File(File::default()),
		Value::RecordId(RecordId::new("test", "key")),
		Ordering::Greater
	)]
	#[case::file_lt_range(
		Value::File(File::default()),
		Value::Range(Box::new(Range::unbounded())),
		Ordering::Less
	)]
	#[case::range_gt_file(
		Value::Range(Box::new(Range::unbounded())),
		Value::File(File::default()),
		Ordering::Greater
	)]
	#[case::range_lt_regex(
		Value::Range(Box::new(Range::unbounded())),
		Value::Regex("test".parse().unwrap()),
		Ordering::Less
	)]
	#[case::regex_gt_range(
		Value::Regex("test".parse().unwrap()),
		Value::Range(Box::new(Range::unbounded())),
		Ordering::Greater
	)]
	fn test_value_ordering_cross_variant(
		#[case] left: Value,
		#[case] right: Value,
		#[case] expected: Ordering,
	) {
		assert_eq!(left.cmp(&right), expected);
		assert_eq!(left.partial_cmp(&right), Some(expected));
	}

	// Test equality with cross-type numeric comparisons
	#[rstest]
	#[case::int_eq_float(Value::Number(Number::Int(5)), Value::Number(Number::Float(5.0)))]
	#[case::float_eq_int(Value::Number(Number::Float(5.0)), Value::Number(Number::Int(5)))]
	#[case::int_eq_decimal(
		Value::Number(Number::Int(5)),
		Value::Number(Number::Decimal(Decimal::new(5, 0)))
	)]
	#[case::decimal_eq_int(
		Value::Number(Number::Decimal(Decimal::new(5, 0))),
		Value::Number(Number::Int(5))
	)]
	#[case::none_eq_none(Value::None, Value::None)]
	#[case::null_eq_null(Value::Null, Value::Null)]
	fn test_value_equality(#[case] left: Value, #[case] right: Value) {
		assert_eq!(left, right);
		assert_eq!(right, left);
		assert_eq!(left.cmp(&right), Ordering::Equal);
	}

	// Test inequality
	#[rstest]
	#[case::none_ne_null(Value::None, Value::Null)]
	#[case::int_ne_float(Value::Number(Number::Int(5)), Value::Number(Number::Float(5.5)))]
	#[case::bool_ne_number(Value::Bool(true), Value::Number(Number::Int(1)))]
	#[case::string_ne_number(Value::String("5".to_string()), Value::Number(Number::Int(5)))]
	fn test_value_inequality(#[case] left: Value, #[case] right: Value) {
		assert_ne!(left, right);
		assert_ne!(right, left);
		assert_ne!(left.cmp(&right), Ordering::Equal);
	}

	// Test that sorting works correctly
	#[test]
	fn test_value_sorting() {
		let mut values = vec![
			Value::String("b".to_string()),
			Value::None,
			Value::Number(Number::Int(10)),
			Value::Null,
			Value::Bool(true),
			Value::Number(Number::Int(5)),
			Value::String("a".to_string()),
			Value::Bool(false),
			Value::Number(Number::Float(7.5)),
		];

		values.sort();

		assert_eq!(values[0], Value::None);
		assert_eq!(values[1], Value::Null);
		assert_eq!(values[2], Value::Bool(false));
		assert_eq!(values[3], Value::Bool(true));
		assert_eq!(values[4], Value::Number(Number::Int(5)));
		assert_eq!(values[5], Value::Number(Number::Float(7.5)));
		assert_eq!(values[6], Value::Number(Number::Int(10)));
		assert_eq!(values[7], Value::String("a".to_string()));
		assert_eq!(values[8], Value::String("b".to_string()));
	}
}
