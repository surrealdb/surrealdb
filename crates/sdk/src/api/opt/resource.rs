use crate::{
	api::{conn::LiveQueryParams, err::Error, Result}, RecordIdKey
};
use std::ops::{self, Bound};
use surrealdb_core::expr::{
	Array, Edges as Edges, Id as Id, IdRange as IdRange, Object, Table as CoreTable, Thing as RecordId, Value, Values
};
use surrealdb_core::sql::Table as SqlTable;

/// A wrapper type to assert that you ment to use a string as a table name.
///
/// To prevent some possible errors, by defauit [`IntoResource`] does not allow `:` in table names
/// as this might be an indication that the user might have intended to use a record id instead.
/// If you wrap your table name string in this tupe the [`IntoResource`] trait will accept any
/// table names.
#[derive(Debug)]
pub struct Table<T>(pub T);

impl<T> Table<T>
where
	T: Into<String>,
{
	pub(crate) fn into_core(self) -> CoreTable {
		let mut t = CoreTable::default();
		t.0 = self.0.into();
		t
	}

	/// Add a range of keys to the table.
	pub fn with_range<R>(self, range: R) -> QueryRange
	where
		KeyRange: From<R>,
	{
		let range = KeyRange::from(range);
		let res = IdRange {
			beg: range.start.map(RecordIdKey::into_inner),
			end: range.end.map(RecordIdKey::into_inner),
		};
		let res = RecordId::from((self.0.into(), res));
		QueryRange(res)
	}
}

transparent_wrapper!(
	/// A table range.
	#[derive(Clone, PartialEq)]
	pub struct QueryRange(RecordId)
);

transparent_wrapper!(
	/// A query edge
	#[derive(Clone, PartialEq)]
	pub struct Edge(Edges)
);

/// A database resource
///
/// A resource is a location, or a range of locations, from which data can be fetched.
// #[derive(Debug, Clone, PartialEq)]
// #[non_exhaustive]
// pub enum Resource {
// 	/// Table name
// 	Table(String),
// 	/// Record ID
// 	RecordId(RecordId),
// 	/// An object
// 	Object(Object),
// 	/// An array
// 	Array(Vec<Value>),
// 	/// Edges
// 	Edge(Edge),
// 	/// A range of id's on a table.
// 	Range(QueryRange),
// 	/// Unspecified resource
// 	Unspecified,
// }
pub trait Resource: Send + Sync + resource::Sealed {
	fn kind(&self) -> &'static str;
	fn into_values(self) -> Values;
}

mod resource {
	pub trait Sealed {}
}

pub trait RangeableResource: Resource {
	/// Add a range to the resource, this only works if the resource is a table.
	fn with_range(self, range: KeyRange) -> RecordId;
}

pub trait InsertableResource: Resource {
	fn table_name(&self) -> &str;
	fn default_content(&self) -> Option<Value>;
}

pub trait CreatableResource: Resource {}

pub trait SubscribableResource: Resource {
	/// Converts the resource into a live query parameters
	fn into_live_query_params(self) -> LiveQueryParams;
}


macro_rules! impl_resource_for_table_type {
	($type:ty) => {
		impl resource::Sealed for $type {}
		impl Resource for $type {
			fn kind(&self) -> &'static str {
				"table"
			}

			fn into_values(self) -> Values {
				Values(vec![Value::from(CoreTable::from(self))])
			}
		}
	};
}

impl_resource_for_table_type!(String);
impl_resource_for_table_type!(&str);
impl_resource_for_table_type!(CoreTable);


impl CreatableResource for String {}
impl CreatableResource for &str {}
impl CreatableResource for CoreTable {}

impl InsertableResource for String {
	fn table_name(&self) -> &str {
		self.as_str()
	}

	fn default_content(&self) -> Option<Value> {
		None
	}
}

impl RangeableResource for String {
	fn with_range(self, range: KeyRange) -> RecordId {
		let range = KeyRange::from(range);
		let res = IdRange {
			beg: range.start.map(RecordIdKey::into_inner),
			end: range.end.map(RecordIdKey::into_inner),
		};
		RecordId::from((self, res))
	}
}

impl InsertableResource for &str {
	fn table_name(&self) -> &str {
		self
	}

	fn default_content(&self) -> Option<Value> {
		None
	}
}

impl RangeableResource for &str {
	fn with_range(self, range: KeyRange) -> RecordId {
		let range = KeyRange::from(range);
		let res = IdRange {
			beg: range.start.map(RecordIdKey::into_inner),
			end: range.end.map(RecordIdKey::into_inner),
		};
		RecordId::from((self, res))
	}
}

impl InsertableResource for CoreTable {
	fn table_name(&self) -> &str {
		self.as_str()
	}

	fn default_content(&self) -> Option<Value> {
		None
	}
}

impl RangeableResource for CoreTable {
	fn with_range(self, range: KeyRange) -> RecordId {
		let range = KeyRange::from(range);
		let res = IdRange {
			beg: range.start.map(RecordIdKey::into_inner),
			end: range.end.map(RecordIdKey::into_inner),
		};
		RecordId::from((self.as_str(), res))
	}
}


impl resource::Sealed for RecordId {}
impl Resource for RecordId {
	fn kind(&self) -> &'static str {
		"record_id"
	}

	fn into_values(self) -> Values {
		Values(vec![Value::from(self)])
	}
}

impl InsertableResource for RecordId {
	fn table_name(&self) -> &str {
		self.tb.as_str()
	}

	fn default_content(&self) -> Option<Value> {
		let mut map = Object::default();
		map.insert("id".to_string(), self.id.clone().into());
		Some(Value::Object(map))
	}
}

impl resource::Sealed for Object {}
impl Resource for Object {
	fn kind(&self) -> &'static str {
		"object"
	}

	fn into_values(self) -> Values {
		Values(vec![Value::from(self)])
	}
}

impl resource::Sealed for Vec<Value> {}
impl Resource for Vec<Value> {
	fn kind(&self) -> &'static str {
		"array"
	}

	fn into_values(self) -> Values {
		Values(self)
	}
}

impl resource::Sealed for Edge {}
impl Resource for Edge {
	fn kind(&self) -> &'static str {
		"edge"
	}

	fn into_values(self) -> Values {
		Values(vec![Value::from(self.into_inner())])
	}
}

// impl resource::Sealed for QueryRange {}
// impl Resource for QueryRange {
// 	fn kind(&self) -> &'static str {
// 		"range"
// 	}

// 	fn into_values(self) -> Values {
// 		vec![Value::from(self)]
// 	}
// }


// impl Resource {
// 	/// Add a range to the resource, this only works if the resource is a table.
// 	pub fn with_range(self, range: KeyRange) -> Result<Self> {
// 		match self {
// 			Resource::Table(table) => Ok(Resource::Range(Table(table).with_range(range))),
// 			Resource::RecordId(_) => Err(Error::InvalidRangeOnResource("RecordId".to_string()).into()),
// 			Resource::Object(_) => Err(Error::InvalidRangeOnResource("Object".to_string()).into()),
// 			Resource::Array(_) => Err(Error::InvalidRangeOnResource("Array".to_string()).into()),
// 			Resource::Edge(_) => Err(Error::InvalidRangeOnResource("Edge".to_string()).into()),
// 			Resource::Range(_) => Err(Error::InvalidRangeOnResource("Range".to_string()).into()),
// 			Resource::Unspecified => Err(Error::InvalidRangeOnResource("Unspecified".to_string()).into()),
// 		}
// 	}

// 	#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
// 	pub(crate) fn into_value(self) -> Value {

// 		match self {
// 			Resource::Table(x) => CoreTable(x).into(),
// 			Resource::RecordId(x) => x.into(),
// 			Resource::Object(x) => x.into(),
// 			Resource::Array(x) => Value::Array(Array(x)),
// 			Resource::Edge(x) => x.into_inner().into(),
// 			Resource::Range(x) => x.into_inner().into(),
// 			Resource::Unspecified => Value::None,
// 		}
// 	}
// 	pub fn is_single_recordid(&self) -> bool {
// 		match self {
// 			Resource::RecordId(rid) => !matches!(rid.into_inner_ref().id, Id::Range(_)),
// 			_ => false,
// 		}
// 	}
// }

// impl From<RecordId> for Resource {
// 	fn from(thing: RecordId) -> Self {
// 		Self::RecordId(thing)
// 	}
// }

// impl From<&RecordId> for Resource {
// 	fn from(thing: &RecordId) -> Self {
// 		Self::RecordId(thing.clone())
// 	}
// }

// impl From<Object> for Resource {
// 	fn from(object: Object) -> Self {
// 		Self::Object(object)
// 	}
// }

// impl From<&Object> for Resource {
// 	fn from(object: &Object) -> Self {
// 		Self::Object(object.clone())
// 	}
// }

// impl From<Vec<Value>> for Resource {
// 	fn from(array: Vec<Value>) -> Self {
// 		Self::Array(array)
// 	}
// }

// impl From<&[Value]> for Resource {
// 	fn from(array: &[Value]) -> Self {
// 		Self::Array(array.to_vec())
// 	}
// }

// impl From<&str> for Resource {
// 	fn from(s: &str) -> Self {
// 		Resource::from(s.to_string())
// 	}
// }

// impl From<&String> for Resource {
// 	fn from(s: &String) -> Self {
// 		Self::from(s.as_str())
// 	}
// }

// impl From<String> for Resource {
// 	fn from(s: String) -> Self {
// 		Resource::Table(s)
// 	}
// }

// impl From<Edge> for Resource {
// 	fn from(value: Edge) -> Self {
// 		Resource::Edge(value)
// 	}
// }

// impl From<QueryRange> for Resource {
// 	fn from(value: QueryRange) -> Self {
// 		Resource::Range(value)
// 	}
// }

// impl<T, I> From<(T, I)> for Resource
// where
// 	T: Into<String>,
// 	I: Into<RecordIdKey>,
// {
// 	fn from((table, id): (T, I)) -> Self {
// 		let record_id = RecordId::from_table_key(table, id);
// 		Self::RecordId(record_id)
// 	}
// }

// impl From<()> for Resource {
// 	fn from(_value: ()) -> Self {
// 		Self::Unspecified
// 	}
// }

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

fn no_colon(a: &str) -> Result<()> {
	if a.contains(':') {
		return Err(Error::TableColonId {
			table: a.to_string(),
		}
		.into());
	}
	Ok(())
}
