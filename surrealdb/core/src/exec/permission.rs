//! Permission resolution utilities for the stream executor.
//!
//! This module provides utilities for resolving and checking table/field permissions
//! at execution time. Since SurrealQL allows DDL and DML interleaving within transactions,
//! permissions must be resolved from the current transaction's schema view rather than
//! at planning time.

use std::sync::Arc;

use reblessive::tree::Stk;

use crate::catalog::{Permission, Record};
use crate::err::{EngineError, Error};
use crate::exec::planner::Planner;
use crate::exec::{
	DatabaseContext, Error as ExecError, EvalContext, ExecutionContext, PhysicalExpr,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::idx::trees::gate::{BoxGateFut, TableSelectGate};
use crate::val::{RecordId, Value};

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

/// Convert a catalog [`Permission`] to a PhysicalPermission via the
/// given planner. Inner subqueries inherit the planner's `CycleGuard`, so a
/// self-referential permission (`WHERE (SELECT FROM same_table) != NONE`)
/// falls back to runtime resolution for that subtree instead of recursing.
pub(crate) async fn convert_permission_to_physical(
	permission: &Permission,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	match permission {
		Permission::None => Ok(PhysicalPermission::Deny),
		Permission::Full => Ok(PhysicalPermission::Allow),
		Permission::Specific(expr) => {
			let physical_expr = planner.physical_expr(expr.clone()).await?;
			Ok(PhysicalPermission::Conditional(physical_expr))
		}
	}
}

/// Runtime convenience wrapper: build a txn-less planner from `ctx` and
/// convert. Equivalent to today's per-scan permission resolution path —
/// no plan-time index resolution, no cycle guard interaction.
///
/// Cycle safety note: this path is intentionally *txn-less*. The txn-less
/// shim in `expr_to_physical_expr` short-circuits `try_resolve_table_ctx`,
/// so a self-referential permission compiled here (e.g. a cache-miss
/// runtime build for a permission that contains `SELECT FROM same_table`)
/// can't recurse into table-context resolution. Do **not** switch this
/// helper to [`Planner::with_txn`] without re-deriving cycle safety —
/// in particular, runtime callers don't share a parent [`CycleGuard`]
/// the way plan-time nested planners do, so the inner subtree would
/// either need its own guard or a different cycle-break mechanism.
#[inline]
pub(crate) async fn convert_permission_to_physical_runtime(
	permission: &Permission,
	ctx: &ExecutionContext,
) -> Result<PhysicalPermission, Error> {
	convert_permission_to_physical(permission, &Planner::new(ctx.ctx(), ctx.function_registry()))
		.await
}

/// Check if permission should be checked for the given action.
///
/// Returns `true` if permission checks should be performed, `false` if they
/// should be bypassed (e.g., for root/owner users or when auth is disabled).
pub(crate) fn should_check_perms(db_ctx: &DatabaseContext, action: Action) -> Result<bool, Error> {
	let root = &db_ctx.ns_ctx.root;

	// Inside a permission predicate (`skip_fetch_perms`), enforcement is
	// bypassed so the definer-authored predicate can read freely — matching the
	// legacy `Options::new_with_perms(false)` path. This is the single gate
	// every scan, graph and reference operator consults, so exempting it here
	// disables the whole-scan `Deny` short-circuits, per-row table/field
	// permission filtering, and edge/target enforcement in one place.
	if root.skip_fetch_perms {
		return Ok(false);
	}

	// Check if server auth is disabled
	if !root.ctx.auth_enabled() && root.auth.is_anon() {
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

/// Validate that a record user has access to the current namespace and database.
///
/// Record users (tokens scoped to a specific record) should only be able to access
/// data within their authenticated namespace and database. This check ensures that
/// a record user cannot access data in other namespaces or databases.
///
/// Returns `Ok(())` if access is allowed, `Err` with an appropriate error if denied.
pub(crate) fn validate_record_user_access(db_ctx: &DatabaseContext) -> Result<(), Error> {
	let root = &db_ctx.ns_ctx.root;

	// Only check for record users
	if !root.auth.is_record() {
		return Ok(());
	}

	let ns = db_ctx.ns_name();
	let db = db_ctx.db_name();

	// Verify namespace matches
	if root.auth.level().ns() != Some(ns) {
		return Err(ExecError::NsNotAllowed {
			ns: ns.into(),
		}
		.into());
	}

	// Verify database matches
	if root.auth.level().db() != Some(db) {
		return Err(ExecError::DbNotAllowed {
			db: db.into(),
		}
		.into());
	}

	Ok(())
}

/// Check a physical permission against a specific record value.
///
/// Returns `true` if access is allowed, `false` if denied.
///
/// `value_param` lets field-level callers bind the `$value` parameter to
/// the field's picked value (matching legacy `pluck.rs` semantics). Pass
/// `None` for table-level checks, where `$value` has no meaning.
pub(crate) async fn check_permission_for_value(
	permission: &PhysicalPermission,
	value: &Value,
	value_param: Option<&Value>,
	ctx: &ExecutionContext,
) -> anyhow::Result<bool> {
	match permission {
		PhysicalPermission::Deny => Ok(false),
		PhysicalPermission::Allow => Ok(true),
		PhysicalPermission::Conditional(physical_expr) => {
			// Inside a permission predicate evaluation (propagated via
			// skip_fetch_perms), allow unconditionally so cyclic links
			// don't recurse forever.
			if ctx.root().skip_fetch_perms {
				return Ok(true);
			}

			let bound_ctx;
			let exec_ctx = match value_param {
				Some(v) => {
					bound_ctx = ctx.with_param("value", v.clone());
					&bound_ctx
				}
				None => ctx,
			};
			// Bind the record as both the current value and the document root.
			// The legacy path passes it as the `CursorDoc`
			// (`doc/check.rs::process_permissions`, `doc/reduce.rs`), which is
			// what lets `$parent` inside an idiom-level filter — e.g.
			// `PERMISSIONS FOR select WHERE acl[WHERE $parent.owner = $auth.id]`
			// — resolve to the row under test. `$parent` is the one reader that
			// does not fall back to `current_value`, so binding only the value
			// left it unresolved and the whole predicate falsy, denying every row.
			let mut eval_ctx = EvalContext::from_exec_ctx(exec_ctx).with_value_and_doc(value);
			eval_ctx.skip_fetch_perms = true;

			// The concrete error is carried through rather than collapsed to
			// `Internal`: this runs per permission-checked row, and a write
			// conflict raised inside a predicate has to stay downcastable for
			// the transactor to retry it, while a cancelled query has to keep
			// reporting as cancelled rather than as an internal failure.
			//
			// A predicate that signals control flow has nowhere to send it —
			// there is no surrounding block — so those stay an error, as
			// before.
			let result = physical_expr.evaluate(eval_ctx).await.map_err(|ctrl| match ctrl {
				ControlFlow::Err(e) => e,
				other => anyhow::Error::new(EngineError::Internal(format!(
					"unexpected control flow in a permission predicate: {other}"
				))),
			})?;
			Ok(result.is_truthy())
		}
	}
}

/// The streaming executor's SELECT-permission gate for ANN truthy-document
/// filters.
///
/// Pairs a permission already resolved for the surrounding scan with the
/// context it is evaluated against, so the ANN search gates each candidate on
/// exactly the permission that filters the fetched batch after the search and
/// the two checks cannot disagree.
pub(crate) struct PhysicalTableSelect {
	permission: PhysicalPermission,
	ctx: ExecutionContext,
}

impl PhysicalTableSelect {
	pub(crate) fn new(permission: PhysicalPermission, ctx: ExecutionContext) -> Self {
		Self {
			permission,
			ctx,
		}
	}
}

impl TableSelectGate for PhysicalTableSelect {
	fn allows_every_doc(&self) -> Option<bool> {
		match self.permission {
			PhysicalPermission::Allow => Some(true),
			PhysicalPermission::Deny => Some(false),
			PhysicalPermission::Conditional(_) => None,
		}
	}

	fn allows_doc<'a>(
		&'a self,
		_stk: &'a mut Stk,
		_rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a> {
		Box::pin(async move {
			check_permission_for_value(&self.permission, &record.data, None, &self.ctx).await
		})
	}
}
