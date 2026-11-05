//! How a table source's scan is planned for the rules that depend on the rows' schema:
//! the table `SELECT` permission, the computed fields and the field-level `SELECT` permissions.
//! The two cases below differ in where those rules are applied.
//!
//! - A source the analyzer resolved (`LogicalOp::Source(LogicalSource { schema: Some(_), .. })`)
//!   carries the rules in the plan, as a `PermissionFilter` and a stack of projection layers above
//!   the scan. The scan is planned [`EnforcementMode::Planned`] and yields rows as stored. If the
//!   scan also cut rows, the nodes above would receive an already-cut row, and a computed field's
//!   guard, which reads a stored field raw, would be judged against that cut, so the plan alone
//!   applies the rules.
//! - A source the analyzer did not resolve carries none of them. The scan is planned
//!   [`EnforcementMode::Runtime`] and applies all three rules per row.
//!
//! The session-level gates (`should_check_perms`, `validate_record_user_access`)
//! stay with the scan. They are checked at execution time against the authority the
//! executor set up.

// The plan lowering reaches these items; dead-code warnings are suppressed for them.
#![allow(dead_code)]

use std::sync::Arc;

use crate::catalog::{Permission, TableDefinition};
use crate::exec::permission::PhysicalPermission;
use crate::expr::logical::Schema;

/// How a `Source` node asks its scan to be planned.
///
/// Chosen from the node's `schema`. A resolved schema means the analyzer has
/// already inserted, above the source, every stage the read needs. No other
/// stage may apply them again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnforcementMode {
	/// The plan enforces; the scan yields rows as stored.
	Planned,
	/// The scan enforces per row.
	Runtime,
}

impl EnforcementMode {
	/// The mode for a source with the given `schema`.
	pub(crate) fn for_source(schema: Option<&Arc<Schema>>) -> Self {
		if schema.is_some() {
			Self::Planned
		} else {
			Self::Runtime
		}
	}
}

/// The table `SELECT` permission that a scan under
/// [`EnforcementMode::Planned`] carries for readers outside the plan. It is
/// `Deny` for `NONE` and `Allow` otherwise, because the plan judges any
/// conditional guard. It is read from the definition the scan runs with.
pub(crate) fn planned_select_permission(table_def: &TableDefinition) -> PhysicalPermission {
	match table_def.permissions.select {
		Permission::None => PhysicalPermission::Deny,
		Permission::Full | Permission::Specific(_) => PhysicalPermission::Allow,
	}
}

#[cfg(test)]
pub(crate) mod test_support {

	use surrealdb_strand::TableName;

	use crate::exec::ExecutionContext;
	use crate::exec::index::access_path::IndexRef;

	/// The index `name` on `table`, as the access-path chooser hands it to a
	/// scan.
	pub(crate) async fn index_ref(ctx: &ExecutionContext, table: &str, name: &str) -> IndexRef {
		let indexes = ctx
			.database()
			.expect("a database context")
			.get_table_indexes(&TableName::from(table), None)
			.await
			.expect("catalog read");
		let idx =
			indexes.iter().position(|ix| ix.name.as_str() == name).expect("the index is defined");
		IndexRef::new(&indexes, idx)
	}
}
