use crate::ErrorKind;
use crate::Result;
use serde::Serialize;
use std::ops;
use std::ops::Bound;
use surrealdb::sql;
use surrealdb::sql::Array;
use surrealdb::sql::Edges;
use surrealdb::sql::Id;
use surrealdb::sql::Object;
use surrealdb::sql::Table;
use surrealdb::sql::Thing;
use surrealdb::sql::Value;

/// A database resource
#[derive(Serialize)]
#[serde(untagged)]
#[derive(Debug)]
pub enum DbResource {
	/// Table name
	Table(Table),
	/// Record ID
	RecordId(Thing),
	/// An object
	Object(Object),
	/// An array
	Array(Array),
	/// Edges
	Edges(Edges),
}

impl DbResource {
	pub(crate) fn with_range(self, range: Range<Id>) -> Result<Value> {
		match self {
			DbResource::Table(Table(table)) => Ok(sql::Range {
				tb: table,
				beg: range.start,
				end: range.end,
			}
			.into()),
			DbResource::RecordId(..) => Err(ErrorKind::RangeUnsupported.with_context("record IDs")),
			DbResource::Object(..) => Err(ErrorKind::RangeUnsupported.with_context("objects")),
			DbResource::Array(..) => Err(ErrorKind::RangeUnsupported.with_context("arrays")),
			DbResource::Edges(..) => Err(ErrorKind::RangeUnsupported.with_context("edges")),
		}
	}
}

impl From<DbResource> for Value {
	fn from(resource: DbResource) -> Self {
		match resource {
			DbResource::Table(resource) => resource.into(),
			DbResource::RecordId(resource) => resource.into(),
			DbResource::Object(resource) => resource.into(),
			DbResource::Array(resource) => resource.into(),
			DbResource::Edges(resource) => resource.into(),
		}
	}
}

/// A trait for converting inputs into database resources
pub trait Resource<Response>: Sized {
	/// Converts an input into a database resource
	fn into_db_resource(self) -> Result<DbResource>;
}

impl<R> Resource<Option<R>> for Object {
	fn into_db_resource(self) -> Result<DbResource> {
		Ok(DbResource::Object(self))
	}
}

impl<R> Resource<Option<R>> for Thing {
	fn into_db_resource(self) -> Result<DbResource> {
		Ok(DbResource::RecordId(self))
	}
}

impl<R, T, I> Resource<Option<R>> for (T, I)
where
	T: Into<String>,
	I: Into<Id>,
{
	fn into_db_resource(self) -> Result<DbResource> {
		let (table, id) = self;
		let record_id = (table.into(), id.into());
		Ok(DbResource::RecordId(record_id.into()))
	}
}

impl<R> Resource<Vec<R>> for Array {
	fn into_db_resource(self) -> Result<DbResource> {
		Ok(DbResource::Array(self))
	}
}

impl<R> Resource<Vec<R>> for Edges {
	fn into_db_resource(self) -> Result<DbResource> {
		Ok(DbResource::Edges(self))
	}
}

impl<R> Resource<Vec<R>> for Table {
	fn into_db_resource(self) -> Result<DbResource> {
		Ok(DbResource::Table(self))
	}
}

fn blacklist_colon(input: &str) -> Result<()> {
	match input.contains(':') {
		true => Err(ErrorKind::SyntaxUnsupported.with_message("table:id")),
		false => Ok(()),
	}
}

impl<R> Resource<Vec<R>> for &str {
	fn into_db_resource(self) -> Result<DbResource> {
		blacklist_colon(self)?;
		Ok(DbResource::Table(Table(self.to_owned())))
	}
}

impl<R> Resource<Vec<R>> for &String {
	fn into_db_resource(self) -> Result<DbResource> {
		blacklist_colon(self)?;
		Ok(DbResource::Table(Table(self.to_owned())))
	}
}

impl<R> Resource<Vec<R>> for String {
	fn into_db_resource(self) -> Result<DbResource> {
		blacklist_colon(&self)?;
		Ok(DbResource::Table(Table(self)))
	}
}

/// Holds the `start` and `end` bounds of a range query
#[derive(Debug)]
pub struct Range<T> {
	pub(crate) start: Bound<T>,
	pub(crate) end: Bound<T>,
}

impl<T> From<(Bound<T>, Bound<T>)> for Range<Id>
where
	T: Into<Id>,
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

impl<T> From<ops::Range<T>> for Range<Id>
where
	T: Into<Id>,
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

impl<T> From<ops::RangeInclusive<T>> for Range<Id>
where
	T: Into<Id>,
{
	fn from(range: ops::RangeInclusive<T>) -> Self {
		let (start, end) = range.into_inner();
		Self {
			start: Bound::Included(start.into()),
			end: Bound::Included(end.into()),
		}
	}
}

impl<T> From<ops::RangeFrom<T>> for Range<Id>
where
	T: Into<Id>,
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

impl<T> From<ops::RangeTo<T>> for Range<Id>
where
	T: Into<Id>,
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

impl<T> From<ops::RangeToInclusive<T>> for Range<Id>
where
	T: Into<Id>,
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

impl From<ops::RangeFull> for Range<Id> {
	fn from(_: ops::RangeFull) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}
}
