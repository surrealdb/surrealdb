//! Record module for SurrealDB
//!
//! This module provides the `Record` type which represents a database record with metadata.
//! Records can contain both data and metadata about the record type (e.g., whether it's an edge).

use std::sync::Arc;

use revision::revisioned;

use crate::catalog::aggregation::AggregationStat;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

/// Represents a record stored in the database
///
/// A `Record` contains both the actual data and optional metadata about the record.
/// The metadata can include information such as the record type (e.g., Edge for graph edges).
///
/// # Examples
///
/// ```no_compile
/// use surrealdb_core::val::{record::Record, Value, Object};
///
/// // Create a new record with data
/// let record = Record::new(Value::Object(Object::default()));
///
/// // Check if it's an edge record
/// assert!(!record.is_edge());
/// ```
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Record {
	/// Optional metadata about the record (e.g., record type)
	pub(crate) metadata: Option<Metadata>,
	/// The actual data stored in the record
	// TODO (DB-655): Switch to `Object`.
	pub(crate) data: Value,
}

// Enable revisioned serialization for the Record type
impl_kv_value_revisioned!(Record);

impl Record {
	/// Creates a new record with the given data and no metadata
	pub(crate) fn new(data: Value) -> Self {
		Self {
			metadata: None,
			data,
		}
	}

	/// Checks if this record represents an edge in a graph
	pub const fn is_edge(&self) -> bool {
		matches!(
			&self.metadata,
			Some(Metadata {
				record_type: RecordType::Edge,
				..
			})
		)
	}

	/// Wraps this record in an `Arc` for shared ownership.
	pub(crate) fn into_read_only(self) -> Arc<Self> {
		Arc::new(self)
	}

	/// Sets the record type in the metadata
	pub(crate) fn set_record_type(&mut self, rtype: RecordType) {
		match &mut self.metadata {
			Some(metadata) => {
				metadata.record_type = rtype;
			}
			metadata => {
				*metadata = Some(Metadata {
					record_type: rtype,
					aggregation_stats: Vec::new(),
				});
			}
		}
	}
}

/// Types of records that can be stored in the database
///
/// This enum defines the different types of records that can be stored.
/// Currently, only Edge is supported, but this can be extended to support
/// other record types in the future.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub(crate) enum RecordType {
	/// Represents a normal table record
	#[default]
	Table,
	/// Represents an edge in a graph
	Edge,
}

/// Metadata associated with a record
///
/// This struct contains optional metadata about a record, such as its type and
/// aggregation statistics for materialized view records.
/// The metadata is revisioned to ensure compatibility across different versions
/// of the database.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Metadata {
	/// The type of the record (e.g., Edge for graph edges)
	pub(crate) record_type: RecordType,
	/// Statistics related to running aggregations for this record.
	/// These do not directly correspond to a feild but must be used in conjunction with the table
	/// definition to calculate the final value for this record.
	pub(crate) aggregation_stats: Vec<AggregationStat>,
}
