//! Relation discovery and data structures for GraphQL relation resolution.
//!
//! This module handles:
//! - Auto-discovery of relation tables (`DEFINE TABLE x TYPE RELATION FROM a TO b`)
//! - Data structures for tracking relation connectivity between tables
//!
//! The relation fields themselves are constructed in [`super::tables`] where
//! they have access to the query-building helpers.

use crate::catalog::{TableDefinition, TableType};
use crate::val::TableName;

/// Information about a single relation table and which tables it connects.
#[derive(Debug, Clone)]
pub(crate) struct RelationInfo {
	/// The name of the relation table.
	pub table_name: TableName,
	/// Tables the relation originates from (the `IN` / `FROM` clause).
	/// Empty if no `IN` clause was specified.
	pub from_tables: Vec<String>,
	/// Tables the relation points to (the `OUT` / `TO` clause).
	/// Empty if no `OUT` clause was specified.
	pub to_tables: Vec<String>,
}

/// The direction of a relation from the perspective of the table the field is added to.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RelationDirection {
	/// This table is the source (appears in the `FROM` / `IN` list).
	/// Resolved by filtering: `WHERE in = $current_record`.
	Outgoing,
	/// This table is the target (appears in the `TO` / `OUT` list).
	/// Resolved by filtering: `WHERE out = $current_record`.
	Incoming,
}

/// Scan table definitions and collect information about relation tables.
///
/// Only returns relations where at least one of `FROM` or `TO` tables are
/// specified, so that relation fields can be meaningfully generated on the
/// connected tables.
pub(crate) fn collect_relations(tbs: &[TableDefinition]) -> Vec<RelationInfo> {
	tbs.iter()
		.filter_map(|tb| {
			if let TableType::Relation(ref rel) = tb.table_type {
				if !rel.from.is_empty() || !rel.to.is_empty() {
					Some(RelationInfo {
						table_name: tb.name.clone(),
						from_tables: rel.from.clone(),
						to_tables: rel.to.clone(),
					})
				} else {
					None
				}
			} else {
				None
			}
		})
		.collect()
}
