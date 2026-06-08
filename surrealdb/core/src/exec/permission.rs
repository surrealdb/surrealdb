//! Permission resolution utilities for the stream executor.
//!
//! This module provides utilities for resolving and checking table/field permissions
//! at execution time. Since SurrealQL allows DDL and DML interleaving within transactions,
//! permissions must be resolved from the current transaction's schema view rather than
//! at planning time.

use std::sync::Arc;

use reblessive::tree::Stk;

use crate::catalog::{Permission, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exec::planner::Planner;
use crate::exec::{DatabaseContext, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::FlowResultExt as _;
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

/// Convert a catalog Permission to a PhysicalPermission via the given
/// planner. Inner subqueries inherit the planner's `CycleGuard`, so a
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
	ctx: &FrozenContext,
) -> Result<PhysicalPermission, Error> {
	convert_permission_to_physical(permission, &Planner::new(ctx)).await
}

/// Resolve the SELECT permission for a table.
///
/// If the table doesn't exist (schemaless mode), returns `Permission::None`
/// which will deny access for record users.
pub(crate) fn resolve_select_permission(table_def: Option<&TableDefinition>) -> &Permission {
	match table_def {
		Some(def) => &def.permissions.select,
		None => &Permission::None,
	}
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
		return Err(Error::NsNotAllowed {
			ns: ns.into(),
		});
	}

	// Verify database matches
	if root.auth.level().db() != Some(db) {
		return Err(Error::DbNotAllowed {
			db: db.into(),
		});
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
) -> Result<bool, Error> {
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
			let mut eval_ctx = EvalContext::from_exec_ctx(exec_ctx).with_value(value);
			eval_ctx.skip_fetch_perms = true;

			let result = physical_expr
				.evaluate(eval_ctx)
				.await
				.map_err(|e| Error::Internal(e.to_string()))?;
			Ok(result.is_truthy())
		}
	}
}

/// Evaluate a catalog SELECT [`Permission`] against a [`CursorDoc`] using the
/// legacy compute path. Returns `true` when access is allowed.
///
/// Used by KNN truthy-document filters (HNSW, DiskANN) where the table's
/// SELECT permission must be checked per candidate before the
/// caller-supplied WHERE condition runs. Without this gate, a caller can
/// probe restricted fields by crafting a WHERE on them and observing the
/// resulting count / order / timing.
///
/// `Specific` expressions are evaluated with `opt.new_with_perms(false)` so
/// the permission expression itself doesn't recurse into permission checks
/// against its own table.
pub(crate) async fn evaluate_table_select_for_doc(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	permission: &Permission,
	cursor_doc: &CursorDoc,
) -> anyhow::Result<bool> {
	match permission {
		Permission::None => Ok(false),
		Permission::Full => Ok(true),
		Permission::Specific(e) => {
			let opt_no_perms = opt.new_with_perms(false);
			Ok(stk
				.run(|stk| e.compute(stk, ctx, &opt_no_perms, Some(cursor_doc)))
				.await
				.catch_return()?
				.is_truthy())
		}
	}
}

/// Cached resolution of a table's SELECT permission check, for callers that
/// need to evaluate the permission per candidate row (e.g. KNN truthy-doc
/// filters). Resolve once per filter via [`resolve_cached_table_select`],
/// then check each candidate via [`check_cached_table_select_for_doc`].
#[derive(Clone)]
pub(crate) enum CachedTableSelect {
	/// Permission checks are bypassed (auth disabled / privileged session).
	Skip,
	/// Permission must be evaluated against each candidate document.
	Apply(Permission),
}

/// Resolve a table's SELECT permission for caching across per-row checks in
/// an ANN truthy-doc filter. Returns `Skip` when [`crate::ctx::Context::check_perms`]
/// reports `false`; otherwise returns `Apply(p)` with the table's SELECT
/// permission (or `Permission::None` if the table is missing — which denies
/// access by design).
pub(crate) async fn resolve_cached_table_select(
	ctx: &FrozenContext,
	opt: &Options,
	table_def: Option<&TableDefinition>,
) -> anyhow::Result<CachedTableSelect> {
	if !ctx.check_perms(opt, Action::View)? {
		return Ok(CachedTableSelect::Skip);
	}
	Ok(CachedTableSelect::Apply(resolve_select_permission(table_def).clone()))
}

/// Check a previously-resolved [`CachedTableSelect`] against a [`CursorDoc`].
/// Companion to [`resolve_cached_table_select`].
pub(crate) async fn check_cached_table_select_for_doc(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	cached: &CachedTableSelect,
	cursor_doc: &CursorDoc,
) -> anyhow::Result<bool> {
	match cached {
		CachedTableSelect::Skip => Ok(true),
		CachedTableSelect::Apply(p) => {
			evaluate_table_select_for_doc(stk, ctx, opt, p, cursor_doc).await
		}
	}
}

/// Populate `slot` with the table's cached SELECT permission on first call,
/// then return a reference to it. Subsequent calls reuse the cached value
/// without re-fetching the table definition. Shared between the HNSW and
/// DiskANN truthy-doc filters, both of which resolve the permission once per
/// filter and reuse it for every candidate.
pub(crate) async fn ensure_cached_table_select<'a>(
	ctx: &FrozenContext,
	opt: &Options,
	txn: &crate::kvs::Transaction,
	ikb: &crate::idx::IndexKeyBase,
	slot: &'a mut Option<CachedTableSelect>,
) -> anyhow::Result<&'a CachedTableSelect> {
	use crate::catalog::providers::TableProvider;
	if slot.is_none() {
		let table = txn.get_tb(ikb.ns(), ikb.db(), ikb.table(), None).await?;
		*slot = Some(resolve_cached_table_select(ctx, opt, table.as_deref()).await?);
	}
	Ok(slot.as_ref().expect("just populated above"))
}
