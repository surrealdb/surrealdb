//! Plan-time resolved table context.
//!
//! When the table name is known at planning time and a transaction is available,
//! the planner resolves table definitions and field state eagerly. The result is
//! bundled into [`ResolvedTableContext`] and passed directly to scan operators,
//! eliminating all per-query runtime KV lookups from the hot path.
//!
//! Permission checks (`should_check_perms`, `validate_record_user_access`) are
//! still done at execution time because they depend on auth context that is set
//! up by the executor, but these are pure CPU operations with zero KV overhead.

use std::sync::Arc;

use super::pipeline::{FieldState, build_field_state_raw, filter_field_state_for_projection};
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::err::Error;
use crate::exec::permission::{PhysicalPermission, convert_permission_to_physical};
use crate::exec::planner::Planner;
use crate::val::TableName;

/// Plan-time resolved table metadata that replaces runtime KV lookups.
///
/// Contains the table definition, pre-compiled SELECT permission, and
/// pre-built field state. When an operator has a `ResolvedTableContext`,
/// its `execute()` performs zero async work for metadata resolution --
/// only the synchronous `should_check_perms` call remains.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedTableContext {
	/// The resolved table definition (used for ns_id/db_id/table_id).
	pub table_def: Arc<TableDefinition>,
	/// Pre-compiled SELECT permission (Full/None/Conditional with PhysicalExpr).
	/// Compiled at plan time so operators don't need to call
	/// `convert_permission_to_physical` at execute time.
	pub select_permission: PhysicalPermission,
	/// Pre-built field state with all computed fields and field-level permissions.
	/// Stored as Arc for cheap cloning; operators filter by `needed_fields` at use time.
	pub field_state: Arc<FieldState>,
}

impl ResolvedTableContext {
	/// Get a field state filtered for a specific projection.
	pub fn field_state_for_projection(
		&self,
		needed_fields: Option<&std::collections::HashSet<String>>,
	) -> FieldState {
		filter_field_state_for_projection(&self.field_state, needed_fields)
	}

	/// Get the SELECT permission, respecting the `check_perms` flag.
	/// When `check_perms` is false, returns `Allow` regardless of the
	/// pre-compiled permission.
	pub fn select_permission(&self, check_perms: bool) -> PhysicalPermission {
		if check_perms {
			self.select_permission.clone()
		} else {
			PhysicalPermission::Allow
		}
	}
}

/// Resolve table context at plan time.
///
/// Performs the expensive KV lookups (table definition, field definitions) and
/// PhysicalExpr compilation eagerly during planning. Returns `None` if the
/// table does not exist.
///
/// The `check_perms` flag controls whether field-level permissions are compiled
/// into the field state. Since the planner doesn't have auth context, this
/// should be `true` (conservative) -- the operator will skip permission
/// evaluation at runtime if it determines checks aren't needed.
///
/// `planner` provides the transaction, ns/db context, and the
/// [`crate::exec::planner::CycleGuard`] used to break compile-time cycles
/// for self-referential permissions / computed fields. The caller (the
/// only legitimate one is `try_resolve_table_ctx`) is responsible for
/// pushing `(ns_id, db_id, table_name)` onto the guard *before* invoking
/// this function.
pub(crate) async fn resolve_table_context(
	planner: &Planner<'_>,
	ns: &str,
	db: &str,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table_name: &TableName,
) -> Result<Option<ResolvedTableContext>, Error> {
	use crate::catalog::providers::TableProvider;

	let txn = planner.txn().ok_or_else(|| {
		Error::Internal("resolve_table_context requires a planner with txn".into())
	})?;

	// Look up table definition
	let table_def = match txn
		.get_tb_by_name(ns, db, table_name, None)
		.await
		.map_err(|e| Error::Internal(e.to_string()))?
	{
		Some(def) => def,
		None => return Ok(None),
	};

	// Pre-compile SELECT permission and field state at plan time. Inner
	// subqueries inherit the planner's `CycleGuard` via
	// `planner.physical_expr(...)` → SELECT planning →
	// `try_resolve_table_ctx` → guard check. A self-referential permission
	// like `WHERE (SELECT FROM same_table) != NONE` will fall back to
	// runtime resolution for the inner subtree only; the outer `table_def`
	// here is fully resolved.
	let select_permission =
		convert_permission_to_physical(&table_def.permissions.select, planner).await?;

	let field_state = build_field_state_raw(planner, ns_id, db_id, table_name, true, None)
		.await
		.map_err(|cf| match cf {
			crate::expr::ControlFlow::Err(e) => Error::Internal(e.to_string()),
			_ => Error::Internal("Unexpected control flow in field state resolution".into()),
		})?;

	Ok(Some(ResolvedTableContext {
		table_def,
		select_permission,
		field_state: Arc::new(field_state),
	}))
}
