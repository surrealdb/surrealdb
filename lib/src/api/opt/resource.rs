use crate::{
	api::{err::Error, Result},
	value::ToCore,
	Object, RecordId, RecordIdKey, Value,
};
use std::ops::{self, Bound};
use surrealdb_core::{
	sql::{Dir, Table as CoreTable, Value as CoreValue},
	syn,
};

#[derive(Debug)]
pub struct Table<T>(pub T);

impl<T> Table<T>
where
	T: Into<String>,
{
	pub(crate) fn to_core(self) -> CoreTable {
		let mut t = CoreTable::default();
		t.0 = self.0.into();
		t
	}
}

/// A table range.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRange {
	/// The table name,
	pub(crate) table: String,
	pub(crate) range: KeyRange,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
	pub(crate) from: RecordId,
	pub(crate) dir: Dir,
	pub(crate) tables: Vec<String>,
}

/// A database resource
///
/// A resource is a location, or a range of locations, from which data can be fetched.
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
	/// Edges
	Edge(Edge),
	/// A range of id's on a table.
	Range(QueryRange),
}

impl Resource {
	/// Add a range to the resource, this only works if the resource is a table.
	pub fn with_range(self, range: KeyRange) -> Result<Self> {
		match self {
			Resource::Table(table) => Ok(Resource::Range(QueryRange {
				table,
				range,
			})),
			Resource::RecordId(_) => Err(Error::RangeOnRecordId.into()),
			Resource::Object(_) => Err(Error::RangeOnObject.into()),
			Resource::Array(_) => Err(Error::RangeOnArray.into()),
			Resource::Edge(_) => Err(Error::RangeOnEdges.into()),
			Resource::Range(_) => Err(Error::RangeOnRange.into()),
		}
	}

	pub(crate) fn into_core_value(self) -> CoreValue {
		match self {
			Resource::Table(x) => Table(x).to_core().into(),
			Resource::RecordId(x) => x.to_core().into(),
			Resource::Object(x) => x.to_core().into(),
			Resource::Array(x) => x.to_core().into(),
			Resource::Edge(x) => x.to_core().into(),
			Resource::Range(x) => x.to_core().into(),
		}
	}
}

impl<T> From<Table<T>> for Resource
where
	T: Into<String>,
{
	fn from(table: Table<T>) -> Self {
		Self::Table(table.0.into())
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
		match syn::thing(s) {
			Ok(thing) => Self::RecordId(ToCore::from_core(thing).unwrap()),
			Err(_) => Self::Table(s.into()),
		}
	}
}

impl From<&String> for Resource {
	fn from(s: &String) -> Self {
		Self::from(s.as_str())
	}
}

impl From<String> for Resource {
	fn from(s: String) -> Self {
		match syn::thing(s.as_str()) {
			Ok(thing) => Self::RecordId(ToCore::from_core(thing).unwrap()),
			Err(_) => Self::Table(s),
		}
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

/// A database resource into which can be inserted.
#[derive(Debug)]
pub enum InsertResource {
	RecordId(RecordId),
	Table(String),
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
