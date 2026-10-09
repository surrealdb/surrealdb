//! How a table source's scan is planned with respect to the schema-derived
//! behaviours of the rows it reads — the table `SELECT` permission, the
//! computed fields and the field-level `SELECT` permissions:
//!
//! - A source the analyzer resolved (`LogicalOp::Source(LogicalSource { schema: Some(_), .. })`)
//!   carries those declarations in the plan, as a `PermissionFilter` and a stack of projection
//!   layers above the scan. The scan is planned [`EnforcementMode::Planned`] and yields rows as
//!   stored; a scan that reduced them itself would hand the nodes above an already-cut row, and a
//!   computed field's own guard, written to read a stored field raw, would be judged against the
//!   cut.
//! - A source the analyzer did not resolve carries nothing, so the scan is planned
//!   [`EnforcementMode::Runtime`] and applies all three per row, as the scan pipeline does.
//!
//! The session-level gates (`should_check_perms`, `validate_record_user_access`)
//! remain the scan's, evaluated at execution time against the authority the
//! executor set up.

use std::sync::Arc;

use crate::catalog::{Permission, TableDefinition};
use crate::exec::permission::PhysicalPermission;
use crate::expr::logical::Schema;

/// How a `Source` node asks its scan to be planned.
///
/// Decided from the node's `schema`: a resolved schema means the analyzer
/// inserted above the source whatever the read needs, and nothing else may
/// apply it again.
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

/// The table `SELECT` permission a scan carries under
/// [`EnforcementMode::Planned`], for the readers that are not the plan: `Deny`
/// for a permission of `NONE` and `Allow` otherwise, the plan judging a
/// conditional guard. Read off the definition the scan runs with.
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
