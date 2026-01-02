//! Permission resolution utilities for the stream executor.
//!
//! This module provides utilities for resolving and checking table/field permissions
//! at execution time. Since SurrealQL allows DDL and DML interleaving within transactions,
//! permissions must be resolved from the current transaction's schema view rather than
//! at planning time.

use std::sync::Arc;

use crate::catalog::{Permission, TableDefinition};
use crate::err::Error;
use crate::exec::{DatabaseContext, EvalContext, PhysicalExpr};
use crate::iam::Action;
use crate::val::Value;

/// Result of a permission check.
#[derive(Debug, Clone)]
pub enum PhysicalPermission {
	/// Permission allows access unconditionally
	Allow,
	/// Permission denies access unconditionally
	Deny,
	/// Permission requires per-record evaluation
	Conditional(Arc<dyn PhysicalExpr>),
}

/// Convert a catalog Permission to a PhysicalPermission for execution.
pub fn convert_permission_to_physical(
	permission: &Permission,
) -> Result<PhysicalPermission, Error> {
	match permission {
		Permission::None => Ok(PhysicalPermission::Deny),
		Permission::Full => Ok(PhysicalPermission::Allow),
		Permission::Specific(expr) => {
			// Convert Expr to PhysicalExpr using the planner's conversion
			let physical_expr = crate::exec::planner::expr_to_physical_expr(expr.clone())?;
			Ok(PhysicalPermission::Conditional(physical_expr))
		}
	}
}

/// Resolve the SELECT permission for a table.
///
/// If the table doesn't exist (schemaless mode), returns `Permission::None`
/// which will deny access for record users.
pub fn resolve_select_permission(table_def: Option<&TableDefinition>) -> &Permission {
	match table_def {
		Some(def) => &def.permissions.select,
		None => &Permission::None,
	}
}

/// Resolve the CREATE permission for a table.
pub fn resolve_create_permission(table_def: Option<&TableDefinition>) -> &Permission {
	match table_def {
		Some(def) => &def.permissions.create,
		None => &Permission::None,
	}
}

/// Resolve the UPDATE permission for a table.
pub fn resolve_update_permission(table_def: Option<&TableDefinition>) -> &Permission {
	match table_def {
		Some(def) => &def.permissions.update,
		None => &Permission::None,
	}
}

/// Resolve the DELETE permission for a table.
pub fn resolve_delete_permission(table_def: Option<&TableDefinition>) -> &Permission {
	match table_def {
		Some(def) => &def.permissions.delete,
		None => &Permission::None,
	}
}

/// Check if permission should be checked for the given action.
///
/// Returns `true` if permission checks should be performed, `false` if they
/// should be bypassed (e.g., for root/owner users or when auth is disabled).
pub fn should_check_perms(db_ctx: &DatabaseContext, action: Action) -> Result<bool, Error> {
	let root = &db_ctx.ns_ctx.root;

	// Check if server auth is disabled
	if !root.auth_enabled && root.auth.is_anon() {
		return Ok(false);
	}

	let ns = db_ctx.ns_name();
	let db = db_ctx.db_name();

	match action {
		Action::Edit => {
			let allowed = root.auth.has_editor_role();
			let db_in_actor_level =
				root.auth.is_root() || root.auth.is_ns_check(ns) || root.auth.is_db_check(ns, db);
			Ok(!allowed || !db_in_actor_level)
		}
		Action::View => {
			let allowed = root.auth.has_viewer_role();
			let db_in_actor_level =
				root.auth.is_root() || root.auth.is_ns_check(ns) || root.auth.is_db_check(ns, db);
			Ok(!allowed || !db_in_actor_level)
		}
	}
}

/// Check a physical permission against a specific record value.
///
/// Returns `true` if access is allowed, `false` if denied.
pub async fn check_permission_for_value(
	permission: &PhysicalPermission,
	value: &Value,
	db_ctx: &DatabaseContext,
) -> Result<bool, Error> {
	match permission {
		PhysicalPermission::Deny => Ok(false),
		PhysicalPermission::Allow => Ok(true),
		PhysicalPermission::Conditional(physical_expr) => {
			// Evaluate physical expression directly (no spawn_blocking needed)
			let eval_ctx = EvalContext::scalar(
				&db_ctx.ns_ctx.root.params,
				Some(db_ctx.ns_name()),
				Some(db_ctx.db_name()),
				Some(db_ctx.txn()),
			)
			.with_value(value);

			let result = physical_expr
				.evaluate(eval_ctx)
				.await
				.map_err(|e| Error::Internal(e.to_string()))?;
			Ok(result.is_truthy())
		}
	}
}
