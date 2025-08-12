use std::ops::{self, Bound};

use crate::api::Result;
use crate::api::err::Error;
use crate::core::val;
use crate::{Object, RecordId, RecordIdKey, Value};

/// A wrapper type to assert that you ment to use a string as a table name.
///
/// To prevent some possible errors, by defauit [`IntoResource`] does not allow
/// `:` in table names as this might be an indication that the user might have
/// intended to use a record id instead. If you wrap your table name string in
/// this tupe the [`IntoResource`] trait will accept any table names.
#[derive(Debug)]
pub struct Table<T>(pub T);

impl<T> Table<T>
where
	T: Into<String>,
{
	#[allow(dead_code)]
	pub(crate) fn into_core(self) -> val::Table {
		//  TODO: Null byte validity
		unsafe { val::Table::new_unchecked(self.0.into()) }
	}

	/// Add a range of keys to the table.
	pub fn with_range<R>(self, range: R) -> QueryRange
	where
		KeyRange: From<R>,
	{
		let range = KeyRange::from(range);
		let res = val::RecordIdKeyRange {
			start: range.start.map(RecordIdKey::into_inner),
			end: range.end.map(RecordIdKey::into_inner),
		};
		let res = val::RecordId::new(self.0.into(), Box::new(res));
		QueryRange(res)
	}
}

transparent_wrapper!(
	/// A table range.
	#[derive(Clone, PartialEq)]
	pub struct QueryRange(val::RecordId)
);

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
	Table(String),
	/// Record ID
	RecordId(RecordId),
	/// An object
	Object(Object),
	/// An array
	Array(Vec<Value>),
	/// A range of id's on a table.
	Range(QueryRange),
	/// Unspecified resource
	Unspecified,
}

impl Resource {
	/// Add a range to the resource, this only works if the resource is a table.
	pub fn with_range(self, range: KeyRange) -> Result<Self> {
		match self {
			Resource::Table(table) => Ok(Resource::Range(Table(table).with_range(range))),
			Resource::RecordId(_) => Err(Error::RangeOnRecordId.into()),
			Resource::Object(_) => Err(Error::RangeOnObject.into()),
			Resource::Array(_) => Err(Error::RangeOnArray.into()),
			Resource::Range(_) => Err(Error::RangeOnRange.into()),
			Resource::Unspecified => Err(Error::RangeOnUnspecified.into()),
		}
	}

	#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
	pub(crate) fn into_core_value(self) -> val::Value {
		match self {
			Resource::Table(x) => Table(x).into_core().into(),
			Resource::RecordId(x) => x.into_inner().into(),
			Resource::Object(x) => x.into_inner().into(),
			Resource::Array(x) => Value::array_to_core(x).into(),
			Resource::Range(x) => x.into_inner().into(),
			Resource::Unspecified => val::Value::None,
		}
	}
	pub fn is_single_recordid(&self) -> bool {
		match self {
			Resource::RecordId(rid) => {
				!matches!(rid.into_inner_ref().key, val::RecordIdKey::Range(_))
			}
			_ => false,
		}
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
		Resource::from(s.to_string())
	}
}

impl From<&String> for Resource {
	fn from(s: &String) -> Self {
		Self::from(s.as_str())
	}
}

impl From<String> for Resource {
	fn from(s: String) -> Self {
		Resource::Table(s)
	}
}

impl From<QueryRange> for Resource {
	fn from(value: QueryRange) -> Self {
		Resource::Range(value)
	}
}

impl<T, I> From<(T, I)> for Resource
where
	T: Into<String>,
	I: Into<RecordIdKey>,
{
	fn from((table, id): (T, I)) -> Self {
		let record_id = RecordId::from_table_key(table, id);
		Self::RecordId(record_id)
	}
}

impl From<()> for Resource {
	fn from(_value: ()) -> Self {
		Self::Unspecified
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
		return Err(Error::TableColonId {
			table: a.to_string(),
		}
		.into());
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
		Ok(self.into())
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

impl<T, R> IntoResource<Vec<R>> for Table<T> where T: Into<String> {}
impl<T, R> into_resource::Sealed<Vec<R>> for Table<T>
where
	T: Into<String>,
{
	fn into_resource(self) -> Result<Resource> {
		let t = self.0.into();
		Ok(t.into())
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

impl<R> IntoResource<Vec<R>> for () {}
impl<R> into_resource::Sealed<Vec<R>> for () {
	fn into_resource(self) -> Result<Resource> {
		Ok(Resource::Unspecified)
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

impl<T, R> CreateResource<Option<R>> for Table<T> where T: Into<String> {}
impl<T, R> create_resource::Sealed<Option<R>> for Table<T>
where
	T: Into<String>,
{
	fn into_resource(self) -> Result<Resource> {
		let t = self.0.into();
		Ok(t.into())
	}
}

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
