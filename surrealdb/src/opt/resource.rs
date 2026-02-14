use std::ops::{self, Bound};

use crate::types::{
	Array, Kind, Object, RecordId, RecordIdKey, RecordIdKeyRange, SurrealValue, Table, ToSql,
	Value, Variables,
};
use crate::{Error, Result};

/// A table range.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRange {
	pub table: Table,
	pub range: RecordIdKeyRange,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Direction {
	Out,
	In,
	Both,
}

/// A database resource
///
/// A resource is a location, or a range of locations, from which data can be
/// fetched.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Resource {
	/// Table name
	Table(Table),
	/// Record ID
	RecordId(RecordId),
	/// An object
	Object(Object),
	/// An array
	Array(Vec<Value>),
	/// A range of id's on a table.
	Range(QueryRange),
}

impl Resource {
	/// Add a range to the resource, this only works if the resource is a table.
	pub fn with_range(self, range: RecordIdKeyRange) -> Result<Self> {
		match self {
			Resource::Table(table) => Ok(Resource::Range(QueryRange {
				table,
				range,
			})),
			Resource::RecordId(_) => {
				Err(Error::internal("Tried to add a range to an record-id resource".to_string()))
			}
			Resource::Object(_) => {
				Err(Error::internal("Tried to add a range to an object resource".to_string()))
			}
			Resource::Array(_) => {
				Err(Error::internal("Tried to add a range to an array resource".to_string()))
			}
			Resource::Range(_) => Err(Error::internal(
				"Tried to add a range to a resource which was already a range".to_string(),
			)),
		}
	}

	pub fn is_single_recordid(&self) -> bool {
		match self {
			Resource::RecordId(rid) => !matches!(rid.key, RecordIdKey::Range(_)),
			_ => false,
		}
	}

	pub(crate) fn for_sql_query(&self, variables: &mut Variables) -> Result<&'static str> {
		match self {
			Resource::Table(table) => {
				variables.insert("_table".to_string(), Value::Table(table.clone()));
				Ok("$_table")
			}
			Resource::RecordId(record_id) => {
				variables.insert("_record_id".to_string(), Value::RecordId(record_id.clone()));
				Ok("$_record_id")
			}
			Resource::Object(object) => {
				variables.insert("_object".to_string(), Value::Object(object.clone()));
				Ok("$_object")
			}
			Resource::Array(array) => {
				variables.insert("_array".to_string(), Value::Array(Array::from(array.clone())));
				Ok("$_array")
			}
			Resource::Range(query_range) => {
				// Create a RecordId with the range as the key
				let range_record_id = RecordId::new(
					query_range.table.clone(),
					RecordIdKey::Range(Box::new(query_range.range.clone())),
				);
				variables.insert("_range".to_string(), Value::RecordId(range_record_id));
				Ok("$_range")
			}
		}
	}
}

impl SurrealValue for Resource {
	fn kind_of() -> Kind {
		Kind::Either(vec![
			Kind::String,
			Kind::Record(vec![]),
			Kind::Object,
			Kind::Array(Box::new(Kind::Any), None),
			Kind::Range,
			Kind::None,
		])
	}

	fn is_value(value: &Value) -> bool {
		matches!(
			value,
			Value::String(_)
				| Value::RecordId(_)
				| Value::Object(_)
				| Value::Array(_)
				| Value::Range(_)
				| Value::None
		)
	}

	fn into_value(self) -> Value {
		match self {
			Resource::Table(x) => Value::Table(x),
			Resource::RecordId(x) => Value::RecordId(x),
			Resource::Object(x) => Value::Object(x),
			Resource::Array(x) => Value::Array(Array::from(x)),
			Resource::Range(QueryRange {
				table,
				range,
			}) => Value::RecordId(RecordId::new(table, range)),
		}
	}

	fn from_value(value: Value) -> Result<Self> {
		Err(crate::Error::internal(format!("Invalid resource: {}", value.to_sql())))
	}
}

impl From<RecordId> for Resource {
	fn from(thing: RecordId) -> Self {
		Self::RecordId(thing)
	}
}

impl From<&RecordId> for Resource {
	fn from(thing: &RecordId) -> Self {
		Self::RecordId(thing.clone())
	}
}

impl From<Object> for Resource {
	fn from(object: Object) -> Self {
		Self::Object(object)
	}
}

impl From<&Object> for Resource {
	fn from(object: &Object) -> Self {
		Self::Object(object.clone())
	}
}

impl From<Vec<Value>> for Resource {
	fn from(array: Vec<Value>) -> Self {
		Self::Array(array)
	}
}

impl From<&[Value]> for Resource {
	fn from(array: &[Value]) -> Self {
		Self::Array(array.to_vec())
	}
}

impl From<&str> for Resource {
	fn from(s: &str) -> Self {
		Self::Table(s.into())
	}
}

impl From<&String> for Resource {
	fn from(s: &String) -> Self {
		Self::Table(s.clone().into())
	}
}

impl From<String> for Resource {
	fn from(s: String) -> Self {
		Self::Table(s.into())
	}
}

impl From<Table> for Resource {
	fn from(table: Table) -> Self {
		Self::Table(table)
	}
}

impl From<QueryRange> for Resource {
	fn from(value: QueryRange) -> Self {
		Self::Range(value)
	}
}

impl<T, I> From<(T, I)> for Resource
where
	T: Into<String>,
	I: Into<RecordIdKey>,
{
	fn from((table, id): (T, I)) -> Self {
		let record_id = RecordId::new(table.into(), id.into());
		Self::RecordId(record_id)
	}
}

/// Holds the `start` and `end` bounds of a range query
#[derive(Debug, PartialEq, Clone)]
pub struct KeyRange {
	pub(crate) start: Bound<RecordIdKey>,
	pub(crate) end: Bound<RecordIdKey>,
}

impl<T> From<(Bound<T>, Bound<T>)> for KeyRange
where
	T: Into<RecordIdKey>,
{
	fn from((start, end): (Bound<T>, Bound<T>)) -> Self {
		Self {
			start: match start {
				Bound::Included(idx) => Bound::Included(idx.into()),
				Bound::Excluded(idx) => Bound::Excluded(idx.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match end {
				Bound::Included(idx) => Bound::Included(idx.into()),
				Bound::Excluded(idx) => Bound::Excluded(idx.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
		}
	}
}

impl<T> From<ops::Range<T>> for KeyRange
where
	T: Into<RecordIdKey>,
{
	fn from(
		ops::Range {
			start,
			end,
		}: ops::Range<T>,
	) -> Self {
		Self {
			start: Bound::Included(start.into()),
			end: Bound::Excluded(end.into()),
		}
	}
}

impl<T> From<ops::RangeInclusive<T>> for KeyRange
where
	T: Into<RecordIdKey>,
{
	fn from(range: ops::RangeInclusive<T>) -> Self {
		let (start, end) = range.into_inner();
		Self {
			start: Bound::Included(start.into()),
			end: Bound::Included(end.into()),
		}
	}
}

impl<T> From<ops::RangeFrom<T>> for KeyRange
where
	T: Into<RecordIdKey>,
{
	fn from(
		ops::RangeFrom {
			start,
		}: ops::RangeFrom<T>,
	) -> Self {
		Self {
			start: Bound::Included(start.into()),
			end: Bound::Unbounded,
		}
	}
}

impl<T> From<ops::RangeTo<T>> for KeyRange
where
	T: Into<RecordIdKey>,
{
	fn from(
		ops::RangeTo {
			end,
		}: ops::RangeTo<T>,
	) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Excluded(end.into()),
		}
	}
}

impl<T> From<ops::RangeToInclusive<T>> for KeyRange
where
	T: Into<RecordIdKey>,
{
	fn from(
		ops::RangeToInclusive {
			end,
		}: ops::RangeToInclusive<T>,
	) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Included(end.into()),
		}
	}
}

impl From<ops::RangeFull> for KeyRange {
	fn from(_: ops::RangeFull) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}
}

/// A trait for types which can be used as a resource selection for a query.
pub trait IntoResource<Output>: into_resource::Sealed<Output> {}

mod into_resource {
	pub trait Sealed<Output> {
		fn into_resource(self) -> super::Result<super::Resource>;
	}
}

/// A trait for types which can be used as a resource selection for a query that
/// returns an `Option`.
pub trait CreateResource<Output>: create_resource::Sealed<Output> {}

mod create_resource {
	pub trait Sealed<Output> {
		fn into_resource(self) -> super::Result<super::Resource>;
	}
}

fn no_colon(a: &str) -> Result<()> {
	if a.contains(':') {
		return Err(Error::internal(format!(
			"Table name `{a}` contained a colon (:), this is dissallowed to avoid confusion with record-id's try `Table(\"{a}\")` instead."
		)));
	}
	Ok(())
}

// IntoResource

impl IntoResource<Value> for Resource {}
impl into_resource::Sealed<Value> for Resource {
	fn into_resource(self) -> Result<Resource> {
		Ok(self)
	}
}

impl<R> IntoResource<Option<R>> for Object {}
impl<R> into_resource::Sealed<Option<R>> for Object {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

impl<R> IntoResource<Option<R>> for RecordId {}
impl<R> into_resource::Sealed<Option<R>> for RecordId {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

impl<R> IntoResource<Option<R>> for &RecordId {}
impl<R> into_resource::Sealed<Option<R>> for &RecordId {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.clone().into())
	}
}

impl<R, T, I> IntoResource<Option<R>> for (T, I)
where
	T: Into<String>,
	I: Into<RecordIdKey>,
{
}
impl<R, T, I> into_resource::Sealed<Option<R>> for (T, I)
where
	T: Into<String>,
	I: Into<RecordIdKey>,
{
	fn into_resource(self) -> Result<Resource> {
		let record_id = RecordId::new(self.0.into(), self.1);
		Ok(Resource::RecordId(record_id))
	}
}

impl<R> IntoResource<Vec<R>> for Vec<Value> {}
impl<R> into_resource::Sealed<Vec<R>> for Vec<Value> {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

impl<R> IntoResource<Vec<R>> for QueryRange {}
impl<R> into_resource::Sealed<Vec<R>> for QueryRange {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

impl<R> IntoResource<Vec<R>> for &str {}
impl<R> into_resource::Sealed<Vec<R>> for &str {
	fn into_resource(self) -> Result<Resource> {
		no_colon(self)?;
		Ok(self.into())
	}
}

impl<R> IntoResource<Vec<R>> for String {}
impl<R> into_resource::Sealed<Vec<R>> for String {
	fn into_resource(self) -> Result<Resource> {
		no_colon(&self)?;
		Ok(self.into())
	}
}

impl<R> IntoResource<Vec<R>> for &String {}
impl<R> into_resource::Sealed<Vec<R>> for &String {
	fn into_resource(self) -> Result<Resource> {
		no_colon(self)?;
		Ok(self.into())
	}
}

// CreateResource

impl CreateResource<Value> for Resource {}
impl create_resource::Sealed<Value> for Resource {
	fn into_resource(self) -> Result<Resource> {
		Ok(self)
	}
}

impl<R> CreateResource<Option<R>> for Object {}
impl<R> create_resource::Sealed<Option<R>> for Object {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

impl<R> CreateResource<Option<R>> for RecordId {}
impl<R> create_resource::Sealed<Option<R>> for RecordId {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

impl<R> CreateResource<Option<R>> for &RecordId {}
impl<R> create_resource::Sealed<Option<R>> for &RecordId {
	fn into_resource(self) -> Result<Resource> {
		Ok(self.clone().into())
	}
}

impl<R, T, I> CreateResource<Option<R>> for (T, I)
where
	T: Into<String>,
	I: Into<RecordIdKey>,
{
}
impl<R, T, I> create_resource::Sealed<Option<R>> for (T, I)
where
	T: Into<String>,
	I: Into<RecordIdKey>,
{
	fn into_resource(self) -> Result<Resource> {
		Ok(self.into())
	}
}

// impl<T, R> CreateResource<Option<R>> for Table<T> where T: Into<String> {}
// impl<T, R> create_resource::Sealed<Option<R>> for Table<T>
// where
// 	T: Into<String>,
// {
// 	fn into_resource(self) -> Result<Resource> {
// 		let t = self.0.into();
// 		Ok(t.into())
// 	}
// }

impl<R> CreateResource<Option<R>> for &str {}
impl<R> create_resource::Sealed<Option<R>> for &str {
	fn into_resource(self) -> Result<Resource> {
		no_colon(self)?;
		Ok(self.into())
	}
}

impl<R> CreateResource<Option<R>> for String {}
impl<R> create_resource::Sealed<Option<R>> for String {
	fn into_resource(self) -> Result<Resource> {
		no_colon(&self)?;
		Ok(self.into())
	}
}

impl<R> CreateResource<Option<R>> for &String {}
impl<R> create_resource::Sealed<Option<R>> for &String {
	fn into_resource(self) -> Result<Resource> {
		no_colon(self)?;
		Ok(self.into())
	}
}
