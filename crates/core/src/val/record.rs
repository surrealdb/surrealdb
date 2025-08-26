//! Record module for SurrealDB
//!
//! This module provides the `Record` type which represents a database record with metadata.
//! Records can contain both data and metadata about the record type (e.g., whether it's an edge).
//! The data can be stored in either mutable or read-only form for performance optimization.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use revision::error::Error;
use revision::{Revisioned, revisioned};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize, Serializer};

use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

/// Represents a record stored in the database
///
/// A `Record` contains both the actual data and optional metadata about the record.
/// The metadata can include information such as the record type (e.g., Edge for graph edges).
/// The data can be stored in either mutable or read-only form to optimize performance
/// based on usage patterns.
///
/// # Examples
///
/// ```no_compile
/// use surrealdb_core::val::{record::{Record, Data}, Value, Object};
///
/// // Create a new record with mutable data
/// let data = Data::Mutable(Value::Object(Object::default()));
/// let record = Record::new(data);
///
/// // Check if it's an edge record
/// assert!(!record.is_edge());
/// ```
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Record {
	/// Optional metadata about the record (e.g., record type)
	pub(crate) metadata: Option<Metadata>,
	/// The actual data stored in the record
	pub(crate) data: Data,
}

// Enable revisioned serialization for the Record type
impl_kv_value_revisioned!(Record);

impl Record {
	/// Creates a new record with the given data and no metadata
	///
	/// # Arguments
	///
	/// * `data` - The data to store in the record
	///
	/// # Returns
	///
	/// A new `Record` instance with the specified data and no metadata
	pub(crate) fn new(data: Data) -> Self {
		Self {
			metadata: None,
			data,
		}
	}

	/// Checks if this record represents an edge in a graph
	///
	/// This method checks the metadata to determine if the record type
	/// is set to `RecordType::Edge`.
	///
	/// # Returns
	///
	/// `true` if the record is an edge, `false` otherwise
	pub const fn is_edge(&self) -> bool {
		matches!(
			&self.metadata,
			Some(Metadata {
				record_type: Some(RecordType::Edge),
				..
			})
		)
	}

	/// Converts the record's data to read-only format and returns an Arc reference
	///
	/// If the data is currently mutable, it will be wrapped in an `Arc` to make it
	/// read-only. This is useful for performance optimization when the data won't
	/// be modified further.
	///
	/// # Returns
	///
	/// An Arc reference to the record with read-only data
	pub(crate) fn into_read_only(mut self) -> Arc<Self> {
		if let Data::Mutable(value) = &mut self.data {
			let value = mem::take(value);
			let arc = Arc::new(value);
			self.data = Data::ReadOnly(arc);
		}
		Arc::new(self)
	}

	/// Sets the record type in the metadata
	///
	/// This method updates or creates the metadata to include the specified record type.
	/// If metadata already exists, it will be updated. If no metadata exists, new metadata
	/// will be created with the specified record type.
	///
	/// # Arguments
	///
	/// * `rtype` - The record type to set
	pub(crate) fn set_record_type(&mut self, rtype: RecordType) {
		match &mut self.metadata {
			Some(metadata) => {
				metadata.record_type = Some(rtype);
			}
			metadata => {
				*metadata = Some(Metadata {
					record_type: Some(rtype),
				});
			}
		}
	}
}

/// Represents the data stored in a record
///
/// The data can be stored in two formats:
/// - `Mutable`: Direct ownership of the value, allowing modifications
/// - `ReadOnly`: Shared ownership via `Arc`, optimized for read-only access
///
/// This design allows for performance optimization based on usage patterns.
/// Mutable data is used when the value needs to be modified, while read-only
/// data is used when the value will only be read, allowing for better sharing
/// and reduced memory usage.
#[derive(Clone, Debug)]
pub(crate) enum Data {
	/// Mutable data that can be directly modified
	// TODO (DB-655): Switch to `Object`.
	Mutable(Value),
	/// Read-only data wrapped in an Arc for shared access
	ReadOnly(Arc<Value>),
}

impl Data {
	/// Returns a reference to the underlying value
	///
	/// This method provides uniform access to the value regardless of whether
	/// it's stored as mutable or read-only data.
	///
	/// # Returns
	///
	/// A reference to the stored value
	pub(crate) fn as_ref(&self) -> &Value {
		match self {
			Data::Mutable(value) => value,
			Data::ReadOnly(arc) => arc,
		}
	}

	/// Returns a mutable reference to the underlying value
	///
	/// If the data is currently read-only, it will be converted to mutable
	/// by cloning the Arc's contents. This ensures that modifications don't
	/// affect other references to the same data.
	///
	/// # Returns
	///
	/// A mutable reference to the stored value
	pub(crate) fn to_mut(&mut self) -> &mut Value {
		match self {
			Data::Mutable(value) => value,
			Data::ReadOnly(arc) => Arc::make_mut(arc),
		}
	}

	/// Converts the data to read-only format and returns an Arc reference
	///
	/// If the data is already read-only, it returns a clone of the existing Arc.
	/// If the data is mutable, it converts it to read-only by wrapping it in an Arc.
	///
	/// # Returns
	///
	/// An Arc reference to the read-only data
	pub(crate) fn read_only(&mut self) -> Arc<Value> {
		match self {
			Data::ReadOnly(arc) => arc.clone(),
			Data::Mutable(value) => {
				let value = mem::take(value);
				let arc = Arc::new(value);
				*self = Data::ReadOnly(arc.clone());
				arc
			}
		}
	}
}

impl Default for Data {
	/// Creates a default Data instance with a default Value
	fn default() -> Self {
		Self::Mutable(Value::default())
	}
}

impl Revisioned for Data {
	/// Serializes the data using the revisioned format
	///
	/// This delegates to the underlying Value's serialization.
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Error> {
		self.as_ref().serialize_revisioned(writer)
	}

	/// Deserializes the data from the revisioned format
	///
	/// This deserializes a Value and wraps it in a Mutable Data variant.
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		Value::deserialize_revisioned(reader).map(Self::Mutable)
	}

	/// Returns the revision number for this type
	fn revision() -> u16 {
		1
	}
}

impl PartialEq for Data {
	/// Compares two Data instances for equality
	///
	/// This compares the underlying values, regardless of whether they're
	/// stored as mutable or read-only.
	fn eq(&self, other: &Self) -> bool {
		self.as_ref() == other.as_ref()
	}
}

impl PartialOrd for Data {
	/// Compares two Data instances for ordering
	///
	/// This compares the underlying values, regardless of whether they're
	/// stored as mutable or read-only.
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.as_ref().partial_cmp(other.as_ref())
	}
}

impl Hash for Data {
	/// Computes the hash of the Data instance
	///
	/// This hashes the underlying value, regardless of whether it's
	/// stored as mutable or read-only.
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.as_ref().hash(state);
	}
}

impl Serialize for Data {
	/// Serializes the Data instance
	///
	/// This delegates to the underlying Value's serialization.
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.as_ref().serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for Data {
	/// Deserializes a Data instance
	///
	/// This deserializes a Value and wraps it in a Mutable Data variant.
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Value::deserialize(deserializer).map(Self::Mutable)
	}
}

impl From<Value> for Data {
	fn from(value: Value) -> Self {
		Self::Mutable(value)
	}
}

impl From<Arc<Value>> for Data {
	fn from(value: Arc<Value>) -> Self {
		Self::ReadOnly(value)
	}
}

/// Metadata associated with a record
///
/// This struct contains optional metadata about a record, such as its type.
/// The metadata is revisioned to ensure compatibility across different versions
/// of the database.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct Metadata {
	/// The type of the record (e.g., Edge for graph edges)
	record_type: Option<RecordType>,
}

/// Types of records that can be stored in the database
///
/// This enum defines the different types of records that can be stored.
/// Currently, only Edge is supported, but this can be extended to support
/// other record types in the future.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) enum RecordType {
	/// Represents an edge in a graph
	Edge,
}
