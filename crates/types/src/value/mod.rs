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
mod format;
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
/// Set value types for SurrealDB
pub mod set;
/// Table value types for SurrealDB
pub mod table;
/// UUID value types for SurrealDB
pub mod uuid;

use std::cmp::Ordering;
use std::ops::Index;

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
pub use self::set::Set;
pub use self::table::Table;
pub use self::uuid::Uuid;
use crate::sql::{SqlFormat, ToSql};
use crate::utils::escape::QuoteStr;
use crate::{Kind, SurrealValue};

/// Marker type for value conversions from Value::None
///
/// This type represents the absence of a value in SurrealDB.
/// It is used as a marker type for type-safe conversions.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SurrealNone;

/// Marker type for value conversions from Value::Null
///
/// This type represents a null value in SurrealDB.
/// It is used as a marker type for type-safe conversions.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SurrealNull;

/// Represents a value in SurrealDB
///
/// This enum contains all possible value types that can be stored in SurrealDB.
/// Each variant corresponds to a different data type supported by the database.

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
	/// Binary data
	Bytes(Bytes),
	/// A duration value representing a time span
	Duration(Duration),
	/// A datetime value representing a point in time
	Datetime(Datetime),
	/// A UUID value
	Uuid(Uuid),
	/// A geometric value (point, line, polygon, etc.)
	Geometry(Geometry),
	/// A table value
	Table(Table),
	/// A record identifier
	RecordId(RecordId),
	/// A file reference
	File(File),
	/// A range of values
	Range(Box<Range>),
	/// A regular expression
	Regex(Regex),
	/// An array of values
	Array(Array),
	/// An object containing key-value pairs
	Object(Object),
	/// A set of values
	Set(Set),
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
			(Value::Set(a), Value::Set(b)) => a.cmp(b),
			(Value::Object(a), Value::Object(b)) => a.cmp(b),
			(Value::Geometry(a), Value::Geometry(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
			(Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
			(Value::Table(a), Value::Table(b)) => a.cmp(b),
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Uuid(_)
				| Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Datetime(_),
			) => Ordering::Greater,

			(
				Value::Uuid(_),
				Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
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
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Set(_),
			) => Ordering::Less,
			(
				Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Set(_),
				Value::Array(_),
			) => Ordering::Greater,

			(
				Value::Set(_),
				Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Set(_),
			) => Ordering::Greater,

			(
				Value::Object(_),
				Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Object(_),
			) => Ordering::Greater,

			(
				Value::Geometry(_),
				Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Geometry(_),
			) => Ordering::Greater,

			(
				Value::Bytes(_),
				Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
			) => Ordering::Less,
			(
				Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_),
				Value::Bytes(_),
			) => Ordering::Greater,

			(Value::Table(t), Value::RecordId(record_id)) => t.cmp(&record_id.table),
			(Value::RecordId(record_id), Value::Table(t)) => record_id.table.cmp(t),

			(Value::Table(_), Value::File(_) | Value::Range(_) | Value::Regex(_)) => Ordering::Less,
			(Value::File(_) | Value::Range(_) | Value::Regex(_), Value::Table(_)) => {
				Ordering::Greater
			}

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
			Value::Set(_) => Kind::Set(Box::new(Kind::Any), None),
			Value::Object(_) => Kind::Object,
			Value::Geometry(_) => Kind::Geometry(Vec::new()),
			Value::Bytes(_) => Kind::Bytes,
			Value::Table(_) => Kind::Table(Vec::new()),
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
			Value::Set(set) => set.is_empty(),
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

	/// Check if this value matches the specified kind
	///
	/// Returns `true` if the value conforms to the given kind specification.
	/// This includes checking nested types for arrays, objects, records, etc.
	pub fn is_kind(&self, kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::None => self.is_none(),
			Kind::Null => self.is_null(),
			Kind::Bool => self.is_bool(),
			Kind::Bytes => self.is_bytes(),
			Kind::Datetime => self.is_datetime(),
			Kind::Decimal => self.is_decimal(),
			Kind::Duration => self.is_duration(),
			Kind::Float => self.is_float(),
			Kind::Int => self.is_int(),
			Kind::Number => self.is_number(),
			Kind::Object => self.is_object(),
			Kind::String => self.is_string(),
			Kind::Uuid => self.is_uuid(),
			Kind::Regex => matches!(self, Value::Regex(_)),
			Kind::Table(table) => self.is_table_and(|t| table.is_empty() || table.contains(t)),
			Kind::Record(table) => self.is_record_and(|r| r.is_table_type(table)),
			Kind::Geometry(kinds) => {
				self.is_geometry_and(|g| kinds.is_empty() || kinds.contains(&g.kind()))
			}
			Kind::Either(kinds) => kinds.iter().any(|k| self.is_kind(k)),
			Kind::Set(kind, max) => {
				self.is_set_and(|set| {
					// Check max length if specified
					if let Some(max_len) = max
						&& set.len() > *max_len as usize
					{
						return false;
					}
					// Check all elements match the kind
					set.iter().all(|v| v.is_kind(kind))
				})
			}
			Kind::Array(kind, max) => {
				self.is_array_and(|arr| {
					// Check max length if specified
					if let Some(max_len) = max
						&& arr.len() > *max_len as usize
					{
						return false;
					}
					// Check all elements match the kind
					arr.iter().all(|v| v.is_kind(kind))
				})
			}
			Kind::Range => self.is_range(),
			Kind::Literal(literal) => literal.matches(self),
			Kind::File(bucket) => {
				self.is_file_and(|f| bucket.is_empty() || bucket.contains(&f.bucket().to_string()))
			}
			Kind::Function(_, _) => {
				// Functions are not a value type
				false
			}
		}
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

impl ToSql for Value {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Value::None => f.push_str("NONE"),
			Value::Null => f.push_str("NULL"),
			Value::Bool(v) => v.fmt_sql(f, fmt),
			Value::Number(v) => v.fmt_sql(f, fmt),
			Value::String(v) => {
				QuoteStr(v.as_str()).fmt_sql(f, fmt);
			}
			Value::Duration(v) => v.fmt_sql(f, fmt),
			Value::Datetime(v) => v.fmt_sql(f, fmt),
			Value::Uuid(v) => v.fmt_sql(f, fmt),
			Value::Array(v) => v.fmt_sql(f, fmt),
			Value::Object(v) => v.fmt_sql(f, fmt),
			Value::Geometry(v) => v.fmt_sql(f, fmt),
			Value::Bytes(v) => v.fmt_sql(f, fmt),
			Value::Table(v) => v.fmt_sql(f, fmt),
			Value::RecordId(v) => v.fmt_sql(f, fmt),
			Value::File(v) => v.fmt_sql(f, fmt),
			Value::Range(v) => v.fmt_sql(f, fmt),
			Value::Regex(v) => v.fmt_sql(f, fmt),
			Value::Set(v) => v.fmt_sql(f, fmt),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use rstest::rstest;

	use super::*;
	use crate::{GeometryKind, Kind, KindLiteral, object};

	#[rstest]
	#[case::none(Value::None, true)]
	#[case::null(Value::Null, true)]
	#[case::string(Value::String("".to_string()), true)]
	#[case::string(Value::String("hello".to_string()), false)]
	#[case::bytes(Value::Bytes(Bytes::default()), true)]
	#[case::bytes(Value::Bytes(Bytes::from(::bytes::Bytes::from(vec![1_u8, 2, 3]))), false)]
	#[case::object(Value::Object(Object::default()), true)]
	#[case::object(Value::Object(Object::from_iter([("key".to_string(), Value::String("value".to_string()))])), false)]
	#[case::array(Value::Array(Array::new()), true)]
	#[case::array(Value::Array(Array::from(vec![Value::String("hello".to_string())])), false)]
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
			Value::Table("test".into()),
			Value::RecordId(RecordId::new("test", "key")),
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

	#[rstest]
	// None and Null
	#[case::none(Value::None, "NONE")]
	#[case::null(Value::Null, "NULL")]
	// Booleans
	#[case::bool(Value::Bool(true), "true")]
	#[case::bool(Value::Bool(false), "false")]
	// Numbers - integers
	#[case::number(Value::Number(Number::Int(0)), "0")]
	#[case::number(Value::Number(Number::Int(5)), "5")]
	#[case::number(Value::Number(Number::Int(-5)), "-5")]
	#[case::number(Value::Number(Number::Int(i64::MAX)), "9223372036854775807")]
	#[case::number(Value::Number(Number::Int(i64::MIN)), "-9223372036854775808")]
	// Numbers - floats
	#[case::number(Value::Number(Number::Float(0.0)), "0f")]
	#[case::number(Value::Number(Number::Float(5.0)), "5f")]
	#[case::number(Value::Number(Number::Float(-5.5)), "-5.5f")]
	#[case::number(Value::Number(Number::Float(3.12345)), "3.12345f")]
	// Numbers - decimals
	#[case::number(Value::Number(Number::Decimal(Decimal::new(0, 0))), "0dec")]
	#[case::number(Value::Number(Number::Decimal(Decimal::new(5, 0))), "5dec")]
	#[case::number(Value::Number(Number::Decimal(Decimal::new(-5, 0))), "-5dec")]
	#[case::number(Value::Number(Number::Decimal(Decimal::new(12345, 2))), "123.45dec")]
	// Strings - basic
	#[case::string(Value::String("".to_string()), "''")]
	#[case::string(Value::String("hello".to_string()), "'hello'")]
	#[case::string(Value::String("hello world".to_string()), "'hello world'")]
	// Strings - escaping
	#[case::string(Value::String("escap'd".to_string()), "\"escap'd\"")]
	#[case::string(Value::String("\"escaped\"".to_string()), "'\"escaped\"'")]
	#[case::string(Value::String("mix'd \"quotes\"".to_string()), "\"mix'd \\\"quotes\\\"\"")]
	#[case::string(Value::String("tab\there".to_string()), "'tab\there'")]
	#[case::string(Value::String("new\nline".to_string()), "'new\nline'")]
	// Strings - unicode
	#[case::string(Value::String("你好".to_string()), "'你好'")]
	#[case::string(Value::String("emoji 🎉".to_string()), "'emoji 🎉'")]
	// Durations
	#[case::duration(Value::Duration(Duration::new(0, 0)), "0ns")]
	#[case::duration(Value::Duration(Duration::new(1, 0)), "1s")]
	#[case::duration(Value::Duration(Duration::new(60, 0)), "1m")]
	#[case::duration(Value::Duration(Duration::new(3600, 0)), "1h")]
	#[case::duration(Value::Duration(Duration::new(90, 0)), "1m30s")]
	// Datetimes
	#[case::datetime(Value::Datetime(Datetime::from_timestamp(0, 0).unwrap()), "d'1970-01-01T00:00:00Z'")]
	#[case::datetime(Value::Datetime(Datetime::from_timestamp(1, 0).unwrap()), "d'1970-01-01T00:00:01Z'")]
	#[case::datetime(Value::Datetime(Datetime::from_timestamp(1234567890, 0).unwrap()), "d'2009-02-13T23:31:30Z'")]
	// UUIDs
	#[case::uuid(Value::Uuid(Uuid::nil()), "u'00000000-0000-0000-0000-000000000000'")]
	// Arrays - basic
	#[case::array(Value::Array(Array::new()), "[]")]
	#[case::array(Value::Array(vec![Value::Number(Number::Int(1))].into()), "[1]")]
	#[case::array(Value::Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2)), Value::Number(Number::Int(3))].into()), "[1, 2, 3]")]
	// Arrays - mixed types
	#[case::array(Value::Array(vec![Value::String("hello".to_string()), Value::Number(Number::Int(42)), Value::Bool(true)].into()), "['hello', 42, true]")]
	// Arrays - nested
	#[case::array(Value::Array(vec![Value::Array(vec![Value::Number(Number::Int(1))].into())].into()), "[[1]]")]
	#[case::array(Value::Array(vec![Value::Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))].into()), Value::Array(vec![Value::Number(Number::Int(3))].into())].into()), "[[1, 2], [3]]")]
	// Objects - basic
	#[case::object(Value::Object(Object::default()), "{  }")]
	#[case::object(Value::Object(object! {
		"hello": "world".to_string(),
	}), "{ hello: 'world' }")]
	// Objects - multiple keys
	#[case::object(Value::Object(object! {
		"name": "John".to_string(),
		"age": 30,
	}), "{ age: 30, name: 'John' }")]
	// Objects - nested
	#[case::object(Value::Object(object! {
		"user": object! {
			"name": "Jane".to_string(),
		}
	}), "{ user: { name: 'Jane' } }")]
	// Objects - arrays in objects
	#[case::object(Value::Object(object! {
		"items": vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))],
	}), "{ items: [1, 2] }")]
	// Geometry
	#[case::geometry(Value::Geometry(Geometry::Point(geo::Point::new(0.0, 0.0))), "(0f, 0f)")]
	#[case::geometry(Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))), "(1f, 2f)")]
	#[case::geometry(Value::Geometry(Geometry::Point(geo::Point::new(-123.45, 67.89))), "(-123.45f, 67.89f)")]
	// Bytes
	#[case::bytes(Value::Bytes(Bytes::default()), "b\"\"")]
	#[case::bytes(Value::Bytes(Bytes::from(::bytes::Bytes::from(vec![1_u8, 2, 3]))), "b\"010203\"")]
	#[case::bytes(Value::Bytes(Bytes::from(::bytes::Bytes::from(vec![255_u8, 0, 128]))), "b\"FF0080\"")]
	// Tables
	#[case::table(Value::Table("test".into()), "test")]
	#[case::table(Value::Table("escap'd".into()), "`escap'd`")]
	// Record IDs
	#[case::record_id(Value::RecordId(RecordId::new("test", "key")), "test:key")]
	#[case::record_id(Value::RecordId(RecordId::new("user", 123)), "user:123")]
	#[case::record_id(Value::RecordId(RecordId::new("table", "complex_id")), "table:complex_id")]
	// Ranges
	#[case::range(Value::Range(Box::new(Range::unbounded())), "..")]
	#[case::range(
		Value::Range(Box::new(Range::new(
			std::ops::Bound::Included(Value::Number(Number::Int(1))),
			std::ops::Bound::Excluded(Value::Number(Number::Int(10)))
		))),
		"1..10"
	)]
	#[case::range(
		Value::Range(Box::new(Range::new(
			std::ops::Bound::Included(Value::Number(Number::Int(0))),
			std::ops::Bound::Included(Value::Number(Number::Int(100)))
		))),
		"0..=100"
	)]
	#[case::range(
		Value::Range(Box::new(Range::new(
			std::ops::Bound::Unbounded,
			std::ops::Bound::Excluded(Value::Number(Number::Int(50)))
		))),
		"..50"
	)]
	#[case::range(
		Value::Range(Box::new(Range::new(
			std::ops::Bound::Included(Value::Number(Number::Int(10))),
			std::ops::Bound::Unbounded
		))),
		"10.."
	)]
	// Regex
	#[case::regex(Value::Regex("hello".parse().unwrap()), "/hello/")]
	#[case::regex(Value::Regex("[a-z]+".parse().unwrap()), "/[a-z]+/")]
	#[case::regex(Value::Regex("^test$".parse().unwrap()), "/^test$/")]
	fn test_to_sql(#[case] value: Value, #[case] expected_sql: &str) {
		assert_eq!(&value.to_sql(), expected_sql);
	}

	#[rstest]
	#[case::none(Value::None, vec![Kind::None, Kind::Any], vec![Kind::Null])]
	#[case::null(Value::Null, vec![Kind::Null, Kind::Any], vec![Kind::None])]
	#[case::bool(
		Value::Bool(true),
		vec![Kind::Bool, Kind::Any, Kind::Literal(KindLiteral::Bool(true))],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::Bool(false))]
	)]
	#[case::bool_false(
		Value::Bool(false),
		vec![Kind::Bool, Kind::Any, Kind::Literal(KindLiteral::Bool(false))],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::Bool(true))]
	)]
	#[case::bytes(Value::Bytes(Bytes::default()), vec![Kind::Bytes, Kind::Any], vec![Kind::None, Kind::Null])]
	#[case::datetime(
		Value::Datetime(Datetime::default()),
		vec![Kind::Datetime, Kind::Any],
		vec![Kind::None, Kind::Null]
	)]
	#[case::duration(
		Value::Duration(Duration::default()),
		vec![
			Kind::Duration,
			Kind::Any,
			Kind::Literal(KindLiteral::Duration(Duration::default()))
		],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::Duration(Duration::new(1, 0)))]
	)]
	#[case::int(
		Value::Number(Number::Int(0)),
		vec![Kind::Int, Kind::Number, Kind::Any, Kind::Literal(KindLiteral::Integer(0))],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::Integer(1))]
	)]
	#[case::float(
		Value::Number(Number::Float(0.0)),
		vec![Kind::Float, Kind::Number, Kind::Any, Kind::Literal(KindLiteral::Float(0.0))],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::Float(1.0))]
	)]
	#[case::decimal(
		Value::Number(Number::Decimal(Decimal::default())),
		vec![
			Kind::Decimal,
			Kind::Number,
			Kind::Any,
			Kind::Literal(KindLiteral::Decimal(Decimal::default()))
		],
		vec![Kind::None, Kind::Null]
	)]
	#[case::string(
		Value::String("".to_string()),
		vec![Kind::String, Kind::Any, Kind::Literal(KindLiteral::String("".to_string()))],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::String("foo".to_string()))]
	)]
	#[case::string(
		Value::String("foo".to_string()),
		vec![Kind::String, Kind::Any, Kind::Literal(KindLiteral::String("foo".to_string()))],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::String("bar".to_string()))]
	)]
	#[case::uuid(Value::Uuid(Uuid::new_v4()), vec![Kind::Uuid, Kind::Any], vec![Kind::None, Kind::Null])]
	#[case::regex(
		Value::Regex("hello".parse().unwrap()),
		vec![Kind::Regex, Kind::Any],
		vec![Kind::None, Kind::Null]
	)]
	#[case::table(
		Value::Table("test".into()),
		vec![Kind::Table(vec!["test".into()]), Kind::Table(vec![]), Kind::Any],
		vec![Kind::None, Kind::Null]
	)]
	#[case::record(
		Value::RecordId(RecordId::new("test", "key")),
		vec![Kind::Record(vec!["test".into()]), Kind::Record(vec![]), Kind::Any],
		vec![Kind::None, Kind::Null, Kind::Record(vec!["other".into()])]
	)]
	#[case::record_multi_table(
		Value::RecordId(RecordId::new("user", "id")),
		vec![Kind::Record(vec!["user".into(), "admin".into()]), Kind::Any],
		vec![Kind::Record(vec!["post".into(), "comment".into()])]
	)]
	#[case::geometry_point(
		Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
		vec![Kind::Geometry(vec![GeometryKind::Point]), Kind::Geometry(vec![]), Kind::Any],
		vec![Kind::None, Kind::Null, Kind::Geometry(vec![GeometryKind::Line])]
	)]
	#[case::geometry_line(
		Value::Geometry(Geometry::Line(geo::LineString::from(vec![(0.0, 0.0), (1.0, 1.0)]))),
		vec![Kind::Geometry(vec![GeometryKind::Line]), Kind::Geometry(vec![]), Kind::Any],
		vec![Kind::Geometry(vec![GeometryKind::Point])]
	)]
	#[case::geometry_polygon(
		Value::Geometry(Geometry::Polygon(geo::Polygon::new(
			geo::LineString::from(vec![
				(0.0, 0.0),
				(1.0, 0.0),
				(1.0, 1.0),
				(0.0, 1.0),
				(0.0, 0.0)
			]),
			vec![]
		))),
		vec![Kind::Geometry(vec![GeometryKind::Polygon]), Kind::Geometry(vec![]), Kind::Any],
		vec![Kind::Geometry(vec![GeometryKind::Point])]
	)]
	#[case::geometry_multi(
		Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
		vec![Kind::Geometry(vec![GeometryKind::Point, GeometryKind::Line]), Kind::Any],
		vec![Kind::Geometry(vec![GeometryKind::Line, GeometryKind::Polygon])]
	)]
	#[case::array_empty(
		Value::Array(Array::new()),
		vec![
			Kind::Array(Box::new(Kind::Any), None),
			Kind::Array(Box::new(Kind::String), None),
			Kind::Any
		],
		vec![Kind::None, Kind::Null]
	)]
	#[case::array_strings(
		Value::Array(Array::from(vec![
			Value::String("a".to_string()),
			Value::String("b".to_string())
		])),
		vec![
			Kind::Array(Box::new(Kind::String), None),
			Kind::Array(Box::new(Kind::String), Some(5)),
			Kind::Any
		],
		vec![Kind::Array(Box::new(Kind::Int), None), Kind::Array(Box::new(Kind::String), Some(1))]
	)]
	#[case::array_mixed_fails(
		Value::Array(Array::from(vec![
			Value::String("a".to_string()),
			Value::Number(Number::Int(1))
		])),
		vec![Kind::Array(Box::new(Kind::Any), None), Kind::Any],
		vec![Kind::Array(Box::new(Kind::String), None), Kind::Array(Box::new(Kind::Int), None)]
	)]
	#[case::array_with_max(
		Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3))
		])),
		vec![
			Kind::Array(Box::new(Kind::Int), Some(3)),
			Kind::Array(Box::new(Kind::Int), Some(10)),
			Kind::Any
		],
		vec![Kind::Array(Box::new(Kind::Int), Some(2))]
	)]
	#[case::range(
		Value::Range(Box::new(Range::unbounded())),
		vec![Kind::Range, Kind::Any],
		vec![Kind::None, Kind::Null]
	)]
	#[case::file_empty_bucket(
		Value::File(File::default()),
		vec![Kind::File(vec!["".to_string()]), Kind::File(vec![]), Kind::Any],
		vec![Kind::None, Kind::Null]
	)]
	#[case::file_specific_bucket(
		Value::File(File::new("mybucket", "file.txt")),
		vec![Kind::File(vec!["mybucket".to_string()]), Kind::File(vec![]), Kind::Any],
		vec![Kind::File(vec!["other".to_string()])]
	)]
	#[case::object_empty(
		Value::Object(Object::default()),
		vec![Kind::Object, Kind::Any, Kind::Literal(KindLiteral::Object(BTreeMap::new()))],
		vec![Kind::None, Kind::Null]
	)]
	#[case::object_with_fields(
		Value::Object(object! { key: "value".to_string() }),
		vec![
			Kind::Object,
			Kind::Any,
			Kind::Literal(KindLiteral::Object(BTreeMap::from([(
				"key".to_string(),
				Kind::String
			)])))
		],
		vec![Kind::None, Kind::Null, Kind::Literal(KindLiteral::Object(BTreeMap::new()))]
	)]
	#[case::literal_array(
		Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::String("test".to_string())
		])),
		vec![Kind::Literal(KindLiteral::Array(vec![Kind::Int, Kind::String])), Kind::Any],
		vec![
			Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Int])),
			Kind::Literal(KindLiteral::Array(vec![Kind::Int]))
		]
	)]
	#[case::either_string_or_int(
		Value::String("test".to_string()),
		vec![Kind::Either(vec![Kind::String, Kind::Int]), Kind::Any],
		vec![Kind::Either(vec![Kind::Int, Kind::Bool])]
	)]
	#[case::either_int_matches(
		Value::Number(Number::Int(42)),
		vec![Kind::Either(vec![Kind::String, Kind::Int]), Kind::Any],
		vec![Kind::Either(vec![Kind::String, Kind::Bool])]
	)]
	#[case::option_kind_none(
		Value::None,
		vec![Kind::option(Kind::String), Kind::Any],
		vec![Kind::String]
	)]
	#[case::option_kind_value(
		Value::String("test".to_string()),
		vec![Kind::option(Kind::String), Kind::Any],
		vec![Kind::Int]
	)]
	fn test_is_kind(#[case] value: Value, #[case] kinds: Vec<Kind>, #[case] not_kinds: Vec<Kind>) {
		for kind in kinds {
			assert!(value.is_kind(&kind), "{value:?} is not a {kind}");
		}
		for kind in not_kinds {
			assert!(!value.is_kind(&kind), "{value:?} is a {kind} but should not be");
		}
	}
}
