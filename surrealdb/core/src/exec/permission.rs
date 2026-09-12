//! Permission resolution utilities for the stream executor.
//!
//! This module provides utilities for resolving and checking table/field permissions
//! at execution time. Since SurrealQL allows DDL and DML interleaving within transactions,
//! permissions must be resolved from the current transaction's schema view rather than
//! at planning time.
//!
//! A stored clause is compiled from its text into the streaming engine's
//! scalar. The one clause that cannot be — a guard the lowering has no form
//! for — is carried as a [`LegacyClause`], which evaluates the stored
//! expression through the engine's legacy bridge in the frame its caller is
//! in, so a scan enforcing permissions at runtime can evaluate every clause the
//! catalog holds.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{Permission, Record, TableDefinition};
use crate::doc::CursorDoc;
use crate::err::{EngineError, Error};
use crate::exec::operators::legacy_bridge::{legacy_compute, legacy_options_for_frame};
use crate::exec::planner::{Planner, compile_expr};
use crate::exec::{
	AccessMode, BoxFut, ContextLevel, DatabaseContext, Error as ExecError, EvalContext,
	ExecutionContext, FlowResult, PhysicalExpr,
};
use crate::expr::logical::{LogicalPermission, LogicalPermissionRef};
use crate::expr::{ControlFlow, Expr};
use crate::iam::{Action, Auth};
use crate::idx::trees::gate::{BoxGateFut, TableSelectGate};
use crate::kvs::Transaction;
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

/// The `SELECT` permission of a table as the streaming engine applies it,
/// compiled through `planner`.
///
/// The guard is compiled from the definition's logical body — the clause's
/// stored text, lowered once for the life of the definition and shared with
/// the analyzer — so a statement that plans the table does not lower the
/// clause again. A guard whose text has no logical form is carried as a
/// [`LegacyClause`] over the definition's expression form of the same clause.
/// A table with no definition denies, as
/// [`table_select_permission`](crate::catalog::table_select_permission)
/// resolves it. Inner subqueries inherit the planner's `CycleGuard`, so a
/// self-referential permission (`WHERE (SELECT FROM same_table) != NONE`)
/// falls back to runtime resolution for that subtree instead of recursing.
pub(crate) async fn table_select_permission_physical(
	table_def: Option<&TableDefinition>,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	let Some(def) = table_def else {
		return Ok(PhysicalPermission::Deny);
	};
	permission_to_physical(&def.permissions.select, def.select_permission_logical(), planner).await
}

/// A stored `SELECT` clause as the streaming engine applies it: `logical`,
/// the clause compiled from its stored text by the definition that holds it,
/// or — when the text has no logical form — a [`LegacyClause`] over
/// `permission`, the legacy engine's expression form of the same clause. The
/// two are the same clause of the same definition; a caller passes both halves
/// of one definition, never a body of its own making.
pub(crate) async fn permission_to_physical(
	permission: &Permission,
	logical: anyhow::Result<LogicalPermission>,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	match logical {
		Ok(LogicalPermission::None) => Ok(PhysicalPermission::Deny),
		Ok(LogicalPermission::Full) => Ok(PhysicalPermission::Allow),
		Ok(LogicalPermission::Specific(guard)) => {
			Ok(PhysicalPermission::Conditional(planner.physical_expr(guard).await?))
		}
		Err(_) => Ok(match permission {
			Permission::None => PhysicalPermission::Deny,
			Permission::Full => PhysicalPermission::Allow,
			Permission::Specific(guard) => {
				PhysicalPermission::Conditional(LegacyClause::permission(guard.clone()))
			}
		}),
	}
}

/// A stored clause the lowering cannot express, evaluated by the legacy
/// engine.
///
/// The streaming engine compiles a catalog clause — a table or field
/// `PERMISSIONS FOR select` guard, a `COMPUTED` body — from its stored text into
/// a [`PhysicalExpr`]. A clause whose text has no logical form (a mock record
/// set in scalar position, a write statement) cannot be compiled that way and
/// is carried as this expression instead, so the runtime enforcement path can
/// evaluate every clause the catalog holds rather than declining the read.
/// The clause is the legacy engine's own input; nothing here converts it.
///
/// Evaluation goes through the engine's one legacy bridge
/// (`operators/legacy_bridge.rs`), in the frame the caller is in: a guard is
/// evaluated under `skip_fetch_perms`, which the bridge maps to the
/// write-refusing permission frame, and a computed body under the computed
/// frame the field-state kernel sets on the context, with the definer's `AUTH
/// LIMIT` already applied to the context's options. The row is bound as the
/// cursor document, as the legacy reducer binds it, so `$this`, `$parent` and a
/// `$value` bound on the context resolve as they do there.
#[derive(Debug)]
pub(crate) struct LegacyClause {
	expr: Expr,
	kind: LegacyClauseKind,
}

/// Which clause a [`LegacyClause`] carries; names the expression in a plan.
#[derive(Debug, Clone, Copy)]
enum LegacyClauseKind {
	Permission,
	Computed,
}

impl LegacyClause {
	/// A `PERMISSIONS FOR select` guard with no logical form.
	pub(crate) fn permission(expr: Expr) -> Arc<dyn PhysicalExpr> {
		Arc::new(Self {
			expr,
			kind: LegacyClauseKind::Permission,
		})
	}

	/// A `COMPUTED` body with no logical form.
	pub(crate) fn computed(expr: Expr) -> Arc<dyn PhysicalExpr> {
		Arc::new(Self {
			expr,
			kind: LegacyClauseKind::Computed,
		})
	}
}

impl PhysicalExpr for LegacyClause {
	fn name(&self) -> &'static str {
		match self.kind {
			LegacyClauseKind::Permission => "LegacyPermissionClause",
			LegacyClauseKind::Computed => "LegacyComputedClause",
		}
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// A clause belongs to a table, so it is only ever evaluated with a
		// database selected.
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let Some(options) = ctx.exec_ctx.options() else {
				return Err(ExecError::Thrown(
					"Options not available to evaluate a stored clause through the legacy evaluator"
						.into(),
				)
				.into());
			};
			let options = legacy_options_for_frame(
				options,
				ctx.skip_fetch_perms,
				ctx.exec_ctx.root().computing_field,
				ctx.exec_ctx.version_stamp(),
			)
			.with_dive_consumed(ctx.plan_depth + 1);
			let doc = ctx
				.document_root
				.or(ctx.current_value)
				.map(|v| CursorDoc::new(None, None, v.clone()));
			legacy_compute(&self.expr, ctx.exec_ctx.ctx(), &options, doc.as_ref()).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Both frames the clause is evaluated in refuse writes, so a write in
		// the clause is an error at evaluation, never a mutation.
		AccessMode::ReadOnly
	}
}

impl ToSql for LegacyClause {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
	}
}

/// [`table_select_permission_physical`] for a scan that resolves its table
/// when it runs: a txn-less planner is built from `ctx` — no plan-time index
/// resolution, no cycle guard interaction.
///
/// Cycle safety note: this path is intentionally *txn-less*. A planner
/// without a transaction resolves no table context, so a self-referential
/// permission compiled here (e.g. a cache-miss runtime build for a permission
/// that contains `SELECT FROM same_table`) cannot recurse into table-context
/// resolution. Do **not** switch this helper to [`Planner::with_txn`] without
/// re-deriving cycle safety — in particular, runtime callers don't share a
/// parent [`CycleGuard`] the way plan-time nested planners do, so the inner
/// subtree would either need its own guard or a different cycle-break
/// mechanism.
#[inline]
pub(crate) async fn table_select_permission_physical_runtime(
	table_def: Option<&TableDefinition>,
	ctx: &ExecutionContext,
) -> Result<PhysicalPermission, Error> {
	table_select_permission_physical(table_def, &Planner::new(ctx.ctx(), ctx.function_registry()))
		.await
}

/// Whether `action` by `auth` is permission-checked, by role and actor level
/// alone.
///
/// Never, while server auth is disabled and the session is anonymous.
/// Otherwise a principal is exempt only when it holds the role the action
/// needs — Viewer for `View`, Editor for `Edit` — and its actor level covers
/// the target: a root user covers everything, a namespace user the namespace
/// named in `ns`, a database user the database named in `ns`/`db`. With no
/// namespace named only a root user can be exempt.
///
/// This is the one rule behind every gate, at run time and at plan time:
/// [`ExecutionContext::should_check_perms`], which adds the permission-frame
/// bypass in front of it, `Planner::should_check_perms_for_view` and the
/// analyzer's `Caller`. The legacy engine's `Context::check_perms` applies
/// the same rule to its `Options`.
pub(crate) fn action_is_checked(
	auth_enabled: bool,
	auth: &Auth,
	action: Action,
	ns: Option<&str>,
	db: Option<&str>,
) -> bool {
	if !auth_enabled && auth.is_anon() {
		return false;
	}
	let allowed = match action {
		Action::Edit => auth.has_editor_role(),
		Action::View => auth.has_viewer_role(),
	};
	let in_level = auth.is_root()
		|| match (ns, db) {
			(Some(ns), Some(db)) => auth.is_ns_check(ns) || auth.is_db_check(ns, db),
			(Some(ns), None) => auth.is_ns_check(ns),
			(None, _) => false,
		};
	!allowed || !in_level
}

/// [`ExecutionContext::should_check_perms`] for a database-level context: the
/// gate every scan, graph, reference and join operator consults before it
/// enforces anything. `true` when permission checks apply, `false` when they
/// are bypassed — inside a permission predicate, for an exempt principal, or
/// for an anonymous session on a datastore with auth disabled.
pub(crate) fn should_check_perms(db_ctx: &DatabaseContext, action: Action) -> Result<bool, Error> {
	db_ctx.should_check_perms(action)
}

/// Whether every table in `tables` carries `Permission::Full` for `SELECT`.
///
/// `Permission::Full` is the only declaration under which a fast path may
/// resolve a value embedded in an index or adjacency key — a doc-ID, a
/// target vertex — without fetching and permission-checking the record
/// itself: with no per-row predicate to evaluate, skipping the fetch
/// changes nothing observable. A missing table or catalog error is
/// conservatively `false`.
///
/// This only ever gates an optimization, never a correctness requirement,
/// so callers always have a slower, per-record-checked path to fall back
/// to. Callers are responsible for their own `should_check_perms` /
/// `should_check_perms_for_view` gate: when permission evaluation would be
/// skipped for the session regardless, the fast path is safe independent
/// of the declared permission, and this function should not be reached.
pub(crate) async fn tables_have_full_select_permission(
	txn: &crate::kvs::Transaction,
	ns: &str,
	db: &str,
	tables: &[crate::val::TableName],
) -> bool {
	use crate::catalog::providers::TableProvider;
	for table in tables {
		let Ok(Some(td)) = txn.get_tb_by_name(ns, db, table, None).await else {
			return false;
		};
		if !matches!(td.permissions.select, Permission::Full) {
			return false;
		}
	}
	true
}

/// Validate that the authenticated principal may read in the current namespace
/// and database (GHSA-2v9j tenant-boundary gate for the read path).
///
/// The session's *selected* ns/db can be steered with `USE` - including the RPC
/// `use` method, which sets the context without rejecting a cross-tenant switch -
/// so it must not be trusted on its own. The authenticated auth level is the
/// source of truth: root and anonymous principals are unconfined, a namespace
/// principal may read any database within its namespace, and database and record
/// principals are pinned to their exact namespace/database. Enforcing this here,
/// before table/field `PERMISSIONS` are consulted, stops a principal that has
/// pivoted into a foreign tenant from reading a table there whose `PERMISSIONS`
/// happen to be satisfiable.
///
/// Returns `Ok(())` if access is allowed, `Err` with an appropriate error if denied.
pub(crate) fn validate_record_user_access(db_ctx: &DatabaseContext) -> Result<(), Error> {
	let root = &db_ctx.ns_ctx.root;

	let ns = db_ctx.ns_name();
	let db = db_ctx.db_name();

	if root.auth.can_access_ns_db(ns, db) {
		return Ok(());
	}

	// A namespace match with a database mismatch is a database-scope violation;
	// otherwise the namespace itself is out of the principal's reach.
	if root.auth.level().ns() == Some(ns) {
		return Err(ExecError::DbNotAllowed {
			db: db.into(),
		}
		.into());
	}
	Err(ExecError::NsNotAllowed {
		ns: ns.into(),
	}
	.into())
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

/// A stored `PERMISSIONS` clause, compiled once for the definition it was read
/// from rather than once per evaluation.
///
/// A clause of this kind is judged per *evaluation*, not per statement: a
/// `DEFINE PARAM` guard against the row the parameter is read on, a function's
/// against the frame of the call. Compiling it there puts plan-time work
/// inside a row loop, and a parameter or a call in a projection is evaluated
/// once per row.
///
/// Only the compilation is kept. The definition is still read afresh, and the
/// compiled predicate is still evaluated, on every evaluation — what the
/// predicate answers belongs to that evaluation, so a guard that admits one
/// row still refuses the next.
///
/// Reuse is keyed on the definition and the depth. The transaction cache hands
/// out one `Arc` per definition, so an evaluation that gets this very
/// allocation back has the clause the compilation came from without comparing
/// it, and a `DEFINE` that replaces the definition matches nothing. The depth
/// is part of the key because a guard is compiled one re-entry deeper than the
/// expression reading it, and compilation is bounded by that depth.
///
/// What the key pins is the clause, not everything compiling it resolved: a
/// subquery inside a predicate is bound against the catalog the first
/// evaluation saw, and a later evaluation of the same node reads that binding.
/// It is the rule the rest of the plan already follows — a statement binds
/// once, before anything it goes on to define — and it is why the guard is
/// compiled for the definition rather than for the statement.
///
/// The caller's authority is *not* in the key, and the reuse rests on it being
/// fixed: compiling a scalar consults the principal — a read the caller
/// bypasses is planned without the permission the check would apply
/// ([`Planner::should_check_perms_for_view`]) — so a guard compiled for one
/// principal states another's plan wrongly. What holds it is that a physical
/// node belongs to one plan, one statement and one principal, and the authority
/// at the node is a function of that principal and the definition, both of which
/// the key already pins: an `AUTH LIMIT` derives a fresh `Arc` per call, but
/// from the same caller and the same definition, so its value does not move.
/// A cache that let two principals reach one compiled node would break that,
/// and would have to key this memo on the principal as well.
pub(crate) struct CompiledGuard<D> {
	memo: ArcSwapOption<Memo<D>>,
}

/// A compiled guard and the definition and depth it was compiled for.
struct Memo<D> {
	definition: Arc<D>,
	plan_depth: u32,
	compiled: Arc<dyn PhysicalExpr>,
}

/// A cloned node carries the compilation the original made. Reuse is keyed on
/// the definition, which every evaluation checks, so the copy is used only
/// where the original's would have been.
impl<D> Clone for CompiledGuard<D> {
	fn clone(&self) -> Self {
		Self {
			memo: ArcSwapOption::new(self.memo.load_full()),
		}
	}
}

impl<D> Default for CompiledGuard<D> {
	fn default() -> Self {
		Self {
			memo: ArcSwapOption::empty(),
		}
	}
}

/// The memo is an evaluation-time cache and says nothing about the node, so it
/// is not printed.
impl<D> std::fmt::Debug for CompiledGuard<D> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledGuard").finish_non_exhaustive()
	}
}

impl<D> CompiledGuard<D> {
	/// Judge `permission`, the clause `definition` stores, for this
	/// evaluation, and answer whether it admits the caller.
	///
	/// The guard is evaluated in the permission frame — reads inside it are
	/// not themselves permission-checked, which is what stops a predicate
	/// following a record link from recursing into itself — one re-entry
	/// deeper than the expression reading it. `denied` builds the error a
	/// refusal raises; a guard that fails to compile denies, and so does one
	/// that raises anything but an error, while an error keeps its type so a
	/// write conflict raised inside a predicate stays retryable.
	pub(crate) async fn check(
		&self,
		permission: LogicalPermissionRef<'_>,
		definition: &Arc<D>,
		ctx: &EvalContext<'_>,
		txn: Option<Arc<Transaction>>,
		denied: impl Fn() -> ExecError,
	) -> FlowResult<()> {
		let guard = match permission {
			LogicalPermissionRef::Full => return Ok(()),
			LogicalPermissionRef::None => return Err(denied().into()),
			LogicalPermissionRef::Specific(guard) => guard,
		};
		let compiled = match self.cached(definition, ctx.plan_depth) {
			Some(compiled) => compiled,
			None => {
				let Ok(compiled) = compile_expr(
					guard.clone(),
					ctx.exec_ctx.ctx(),
					ctx.exec_ctx.function_registry(),
					txn,
					Some(Arc::clone(&ctx.exec_ctx.root().auth)),
					ctx.plan_depth + 1,
				)
				.await
				else {
					return Err(denied().into());
				};
				self.memo.store(Some(Arc::new(Memo {
					definition: Arc::clone(definition),
					plan_depth: ctx.plan_depth,
					compiled: Arc::clone(&compiled),
				})));
				compiled
			}
		};
		let mut frame = ctx.clone();
		frame.skip_fetch_perms = true;
		match compiled.evaluate(frame).await {
			Ok(result) if result.is_truthy() => Ok(()),
			Ok(_) => Err(denied().into()),
			Err(ControlFlow::Err(e)) => Err(ControlFlow::Err(e)),
			Err(_) => Err(denied().into()),
		}
	}

	/// The kept compilation, if it was made for this definition at this depth.
	fn cached(&self, definition: &Arc<D>, plan_depth: u32) -> Option<Arc<dyn PhysicalExpr>> {
		let memo = self.memo.load();
		let memo = memo.as_ref()?;
		(Arc::ptr_eq(&memo.definition, definition) && memo.plan_depth == plan_depth)
			.then(|| Arc::clone(&memo.compiled))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{
		TestDb, parse_expr, parse_sql_expr, physical_expr, root_ctx, val,
	};
	use crate::iam::{Level, Role};
	use crate::kvs::TransactionType;

	// =========================================================================
	// table_select_permission_physical
	// =========================================================================

	/// A table definition whose `SELECT` clause is `select`, both halves set
	/// from it as a `DEFINE` statement sets them.
	fn table_with_select(select: Permission) -> TableDefinition {
		let mut def = table_with_disagreeing_halves(select.clone(), &select);
		def.permissions.select = select;
		def
	}

	/// A table definition whose expression half of the `SELECT` clause is
	/// `permission` and whose logical body is compiled from `body`. Only a
	/// test builds one: the two halves of a real definition name the same
	/// clause.
	fn table_with_disagreeing_halves(permission: Permission, body: &Permission) -> TableDefinition {
		use crate::catalog::{
			DatabaseId, FromStored, LogicalPermissionBody, NamespaceId, StoredTableDefinition,
			TableId,
		};
		let mut def = TableDefinition::from_stored(&StoredTableDefinition::new(
			NamespaceId(0),
			DatabaseId(0),
			TableId(0),
			"t".into(),
		))
		.expect("a table definition");
		def.permissions.select = permission;
		def.select_body = LogicalPermissionBody::of(body);
		def
	}

	#[tokio::test]
	async fn catalog_permission_maps_onto_the_three_physical_arms() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());

		let none = table_with_select(Permission::None);
		let none = table_select_permission_physical(Some(&none), &planner).await.unwrap();
		assert!(matches!(none, PhysicalPermission::Deny));

		let full = table_with_select(Permission::Full);
		let full = table_select_permission_physical(Some(&full), &planner).await.unwrap();
		assert!(matches!(full, PhysicalPermission::Allow));

		let specific =
			table_with_select(Permission::Specific(parse_sql_expr("owner = $auth.id").into()));
		let conditional =
			table_select_permission_physical(Some(&specific), &planner).await.unwrap();
		assert!(matches!(conditional, PhysicalPermission::Conditional(_)));

		// A table with no definition denies.
		let absent = table_select_permission_physical(None, &planner).await.unwrap();
		assert!(matches!(absent, PhysicalPermission::Deny));
	}

	/// The guard is compiled from the definition's logical body, compiled
	/// once for the life of the definition, and not lowered again from the
	/// expression form: a body that disagrees with the expression form is
	/// what the predicate evaluates.
	#[tokio::test]
	async fn the_guard_is_compiled_from_the_definitions_logical_body() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());
		let def = table_with_disagreeing_halves(
			Permission::Specific(parse_sql_expr("false").into()),
			&Permission::Specific(parse_sql_expr("true").into()),
		);
		let permission = table_select_permission_physical(Some(&def), &planner).await.unwrap();
		let allowed =
			check_permission_for_value(&permission, &val("{ }").await, None, &ctx).await.unwrap();
		assert!(allowed, "the logical body (`true`) decides, not the expression form (`false`)");
	}

	#[tokio::test]
	async fn runtime_conversion_matches_the_planner_conversion() {
		let ctx = root_ctx();
		let def = table_with_select(Permission::Specific(parse_sql_expr("age > 18").into()));

		let resolved = table_select_permission_physical_runtime(Some(&def), &ctx).await.unwrap();
		let PhysicalPermission::Conditional(expr) = resolved else {
			panic!("a Specific permission resolves to Conditional");
		};

		// The txn-less runtime path must still produce an evaluable predicate,
		// not a deferred/unsupported placeholder.
		let allowed = check_permission_for_value(
			&PhysicalPermission::Conditional(expr),
			&val("{ age: 21 }").await,
			None,
			&ctx,
		)
		.await
		.unwrap();
		assert!(allowed);
	}

	// =========================================================================
	// should_check_perms
	// =========================================================================

	/// Build a database context for `session` and ask whether `action` needs
	/// permission enforcement.
	async fn checks_perms(db: &TestDb, session: &Session, action: Action) -> bool {
		let ctx = db.exec_ctx_as(session, TransactionType::Read).await;
		should_check_perms(ctx.database().unwrap(), action).unwrap()
	}

	#[tokio::test]
	async fn root_owner_is_exempt_from_both_actions() {
		let db = TestDb::new("").await;
		let owner = TestDb::owner();
		assert!(!checks_perms(&db, &owner, Action::View).await);
		assert!(!checks_perms(&db, &owner, Action::Edit).await);
	}

	#[tokio::test]
	async fn viewer_is_exempt_from_view_but_not_from_edit() {
		let db = TestDb::new("").await;
		let viewer =
			Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), Role::Viewer);
		assert!(!checks_perms(&db, &viewer, Action::View).await);
		// A Viewer has no editor role, so writes stay permission-checked.
		assert!(checks_perms(&db, &viewer, Action::Edit).await);
	}

	#[tokio::test]
	async fn a_role_outside_the_target_database_is_not_exempt() {
		let db = TestDb::new("").await;
		// Owner of a *different* database: has the role, but the database under
		// query is not inside its actor level, so checks stay on.
		let elsewhere =
			Session::for_level(Level::Database("test".to_owned(), "other".to_owned()), Role::Owner);
		assert!(checks_perms(&db, &elsewhere, Action::View).await);
		assert!(checks_perms(&db, &elsewhere, Action::Edit).await);
	}

	#[tokio::test]
	async fn record_users_are_always_permission_checked() {
		let db = TestDb::new("").await;
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		assert!(checks_perms(&db, &record, Action::View).await);
		assert!(checks_perms(&db, &record, Action::Edit).await);
	}

	#[tokio::test]
	async fn anonymous_is_exempt_only_while_server_auth_is_disabled() {
		let anon = Session::default().with_ns("test").with_db("test");

		let open = TestDb::new("").await;
		assert!(!checks_perms(&open, &anon, Action::View).await);

		let secured = TestDb::new_with_auth("").await;
		assert!(checks_perms(&secured, &anon, Action::View).await);
	}

	#[tokio::test]
	async fn skip_fetch_perms_disables_every_check() {
		let db = TestDb::new_with_auth("").await;
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		let ctx = db.exec_ctx_as(&record, TransactionType::Read).await;

		// Sanity: this identity is checked before the bypass is set.
		assert!(should_check_perms(ctx.database().unwrap(), Action::View).unwrap());

		let ExecutionContext::Database(mut inner) = ctx else {
			panic!("exec_ctx_as builds a Database context");
		};
		inner.ns_ctx.root.skip_fetch_perms = true;
		assert!(!should_check_perms(&inner, Action::View).unwrap());
		assert!(!should_check_perms(&inner, Action::Edit).unwrap());
	}

	// =========================================================================
	// validate_record_user_access
	// =========================================================================

	#[tokio::test]
	async fn root_and_anonymous_identities_are_unconfined() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());
	}

	#[tokio::test]
	async fn a_database_user_is_confined_to_its_own_database() {
		let db = TestDb::new("").await;
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());

		// A database Viewer of test/test reading its own database is allowed.
		let own = Session {
			au: Arc::new(crate::iam::Auth::for_db(crate::iam::Role::Viewer, "test", "test")),
			..Session::for_record("test", "test", "user", rid.clone())
		};
		let ctx = db.exec_ctx_as(&own, TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());

		// The same principal whose authenticated level names a different database
		// than the one it is operating in (the state left by an RPC `use` pivot)
		// is denied at the read gate, before any table PERMISSIONS are consulted.
		let pivoted = Session {
			au: Arc::new(crate::iam::Auth::for_db(crate::iam::Role::Viewer, "test", "other")),
			..Session::for_record("test", "test", "user", rid)
		};
		let ctx = db.exec_ctx_as(&pivoted, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::DbNotAllowed { .. })),
			"expected DbNotAllowed, got {err:?}"
		);
	}

	#[tokio::test]
	async fn a_namespace_user_is_confined_to_its_own_namespace() {
		let db = TestDb::new("").await;
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());

		// A namespace Viewer of `test` may read within its own namespace
		// (a namespace level authorizes any database inside it).
		let own_ns = Session {
			au: Arc::new(crate::iam::Auth::for_ns(crate::iam::Role::Viewer, "test")),
			..Session::for_record("test", "test", "user", rid.clone())
		};
		let ctx = db.exec_ctx_as(&own_ns, TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());

		// A namespace principal operating in a different namespace is refused.
		let other_ns = Session {
			au: Arc::new(crate::iam::Auth::for_ns(crate::iam::Role::Viewer, "other")),
			..Session::for_record("test", "test", "user", rid)
		};
		let ctx = db.exec_ctx_as(&other_ns, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::NsNotAllowed { .. })),
			"expected NsNotAllowed, got {err:?}"
		);
	}

	#[tokio::test]
	async fn a_record_user_is_confined_to_its_own_namespace_and_database() {
		let db = TestDb::new("").await;
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());

		let matching = Session::for_record("test", "test", "user", rid.clone());
		let ctx = db.exec_ctx_as(&matching, TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());

		// The session names test/test (so the context resolves), but the token's
		// own level names a different namespace / database.
		let wrong_ns = Session {
			au: Arc::new(crate::iam::Auth::for_record(
				"user:tobie".to_owned(),
				"other",
				"test",
				"user",
			)),
			..Session::for_record("test", "test", "user", rid.clone())
		};
		let ctx = db.exec_ctx_as(&wrong_ns, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::NsNotAllowed { .. })),
			"expected NsNotAllowed, got {err:?}"
		);

		let wrong_db = Session {
			au: Arc::new(crate::iam::Auth::for_record(
				"user:tobie".to_owned(),
				"test",
				"other",
				"user",
			)),
			..Session::for_record("test", "test", "user", rid)
		};
		let ctx = db.exec_ctx_as(&wrong_db, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::DbNotAllowed { .. })),
			"expected DbNotAllowed, got {err:?}"
		);
	}

	// =========================================================================
	// CompiledGuard
	// =========================================================================

	/// Judge `clause`, stored by `definition`, against `row`, and report
	/// whether it admitted.
	async fn guard_admits<D>(
		guard: &CompiledGuard<D>,
		clause: &crate::expr::logical::LogicalExpr,
		definition: &Arc<D>,
		row: &Value,
		ctx: &ExecutionContext,
	) -> bool {
		let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value_and_doc(row);
		guard
			.check(LogicalPermissionRef::Specific(clause), definition, &eval_ctx, None, || {
				ExecError::Thrown("denied".to_owned())
			})
			.await
			.is_ok()
	}

	/// The memo keeps a compilation, never an answer: the kept predicate is
	/// evaluated afresh against every row, so a guard that admits one row
	/// still refuses the next.
	#[tokio::test]
	async fn a_kept_compilation_is_judged_against_each_row() {
		let ctx = root_ctx();
		let guard: CompiledGuard<String> = CompiledGuard::default();
		// To the memo a definition is an identity and nothing more, so a
		// `String` stands in for the catalog struct a caller passes.
		let definition = Arc::new("v1".to_owned());
		let clause = parse_expr("public = true");

		assert!(
			guard_admits(&guard, &clause, &definition, &val("{ public: true }").await, &ctx).await
		);
		assert!(
			!guard_admits(&guard, &clause, &definition, &val("{ public: false }").await, &ctx)
				.await
		);
	}

	/// A definition the caller re-read is a different `Arc`, and the clause it
	/// carries is compiled afresh. This is what stops a redefinition from
	/// being judged by the clause it replaced: every caller re-reads the
	/// definition per evaluation, so the memo cannot outlive the one it was
	/// made for.
	#[tokio::test]
	async fn a_redefinition_is_judged_by_its_own_clause() {
		let ctx = root_ctx();
		let guard: CompiledGuard<String> = CompiledGuard::default();
		let admits = parse_expr("public = true");
		let refuses = parse_expr("public = false");

		let first = Arc::new("v1".to_owned());
		assert!(guard_admits(&guard, &admits, &first, &val("{ public: true }").await, &ctx).await);

		// The same row, the same guard node, a definition read afresh.
		let second = Arc::new("v2".to_owned());
		assert!(
			!guard_admits(&guard, &refuses, &second, &val("{ public: true }").await, &ctx).await
		);

		// And back: the memo now holds the second definition's compilation,
		// so the first's clause is compiled again rather than answered by it.
		assert!(guard_admits(&guard, &admits, &first, &val("{ public: true }").await, &ctx).await);
	}

	/// The re-entry depth is the other half of the key. A guard reached one
	/// level deeper is compiled with a depth budget of its own, so a
	/// compilation made at one depth is not served at another.
	#[tokio::test]
	async fn a_kept_compilation_belongs_to_the_depth_it_was_made_at() {
		let ctx = root_ctx();
		let guard: CompiledGuard<String> = CompiledGuard::default();
		let definition = Arc::new("v1".to_owned());
		let clause = parse_expr("public = true");
		let row = val("{ public: true }").await;

		let shallow = EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&row);
		assert!(
			guard
				.check(LogicalPermissionRef::Specific(&clause), &definition, &shallow, None, || {
					ExecError::Thrown("denied".to_owned())
				})
				.await
				.is_ok()
		);
		assert!(guard.cached(&definition, 0).is_some());
		assert!(guard.cached(&definition, 1).is_none());
	}

	// =========================================================================
	// check_permission_for_value
	// =========================================================================

	#[tokio::test]
	async fn unconditional_arms_short_circuit_without_evaluating() {
		let ctx = root_ctx();
		let row = val("{ id: person:tobie }").await;

		assert!(
			check_permission_for_value(&PhysicalPermission::Allow, &row, None, &ctx).await.unwrap()
		);
		assert!(
			!check_permission_for_value(&PhysicalPermission::Deny, &row, None, &ctx).await.unwrap()
		);
	}

	#[tokio::test]
	async fn a_conditional_permission_is_evaluated_against_the_record() {
		let ctx = root_ctx();
		let perm = PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await);

		let allowed = check_permission_for_value(&perm, &val("{ public: true }").await, None, &ctx)
			.await
			.unwrap();
		assert!(allowed);

		let denied = check_permission_for_value(&perm, &val("{ public: false }").await, None, &ctx)
			.await
			.unwrap();
		assert!(!denied);

		// A missing field is NONE, which is not truthy — deny, not error.
		let absent = check_permission_for_value(&perm, &val("{ other: 1 }").await, None, &ctx)
			.await
			.unwrap();
		assert!(!absent);
	}

	#[tokio::test]
	async fn value_param_binds_the_picked_field_for_field_level_checks() {
		let ctx = root_ctx();
		let perm = PhysicalPermission::Conditional(physical_expr("$value > 10", &ctx).await);
		let row = val("{ score: 42 }").await;

		let allowed =
			check_permission_for_value(&perm, &row, Some(&Value::from(42)), &ctx).await.unwrap();
		assert!(allowed);

		let denied =
			check_permission_for_value(&perm, &row, Some(&Value::from(3)), &ctx).await.unwrap();
		assert!(!denied);
	}

	#[tokio::test]
	async fn the_record_is_bound_as_the_document_root_so_parent_resolves() {
		// `$parent` is the one reader that does not fall back to the current
		// value, so binding only the value would leave this predicate falsy and
		// deny every row.
		let ctx = root_ctx();
		let perm = PhysicalPermission::Conditional(
			physical_expr("acl[WHERE $parent.owner = 'tobie'] != []", &ctx).await,
		);

		let row = val("{ owner: 'tobie', acl: [{ read: true }] }").await;
		assert!(check_permission_for_value(&perm, &row, None, &ctx).await.unwrap());

		let other = val("{ owner: 'jaime', acl: [{ read: true }] }").await;
		assert!(!check_permission_for_value(&perm, &other, None, &ctx).await.unwrap());
	}

	#[tokio::test]
	async fn skip_fetch_perms_allows_a_conditional_permission_unconditionally() {
		let ctx = root_ctx();
		// A predicate that would otherwise deny.
		let perm = PhysicalPermission::Conditional(physical_expr("false", &ctx).await);
		let row = val("{ id: person:tobie }").await;
		assert!(!check_permission_for_value(&perm, &row, None, &ctx).await.unwrap());

		let ExecutionContext::Root(mut root) = ctx else {
			panic!("root_ctx builds a Root context");
		};
		root.skip_fetch_perms = true;
		let inner = ExecutionContext::Root(root);
		assert!(check_permission_for_value(&perm, &row, None, &inner).await.unwrap());
	}

	#[tokio::test]
	async fn control_flow_out_of_a_predicate_becomes_an_error_not_a_panic() {
		let ctx = root_ctx();
		// BREAK has no surrounding loop inside a permission predicate, so it has
		// nowhere to go and must surface as an error.
		let perm = PhysicalPermission::Conditional(physical_expr("BREAK", &ctx).await);
		let row = val("{ id: person:tobie }").await;

		let err = check_permission_for_value(&perm, &row, None, &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("unexpected control flow"),
			"expected the control-flow message, got {err}"
		);
	}

	#[tokio::test]
	async fn an_error_inside_a_predicate_is_carried_through_unwrapped() {
		let ctx = root_ctx();
		// A thrown error must stay downcastable rather than collapse to
		// `Internal`, so a write conflict raised per row can still be retried.
		let perm = PhysicalPermission::Conditional(physical_expr("THROW 'boom'", &ctx).await);
		let row = val("{ id: person:tobie }").await;

		let err = check_permission_for_value(&perm, &row, None, &ctx).await.unwrap_err();
		assert!(err.to_string().contains("boom"), "expected the thrown message, got {err}");
		assert!(
			!err.to_string().contains("unexpected control flow"),
			"a THROW is an error, not a stray control-flow signal: {err}"
		);
	}

	#[tokio::test]
	async fn a_conditional_permission_reads_through_a_record_link() {
		// A table permission that dereferences a link needs a transaction, so
		// this is the path that `root_ctx` cannot serve.
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 DEFINE TABLE doc SCHEMALESS;
			 CREATE org:acme SET public = true;
			 CREATE org:secret SET public = false;
			 CREATE doc:1 SET org = org:acme;
			 CREATE doc:2 SET org = org:secret;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let perm = PhysicalPermission::Conditional(physical_expr("org.public = true", &ctx).await);

		let doc1 = val("{ id: doc:1, org: org:acme }").await;
		let doc2 = val("{ id: doc:2, org: org:secret }").await;
		assert!(check_permission_for_value(&perm, &doc1, None, &ctx).await.unwrap());
		assert!(!check_permission_for_value(&perm, &doc2, None, &ctx).await.unwrap());
	}

	// =========================================================================
	// PhysicalTableSelect
	// =========================================================================

	#[tokio::test]
	async fn the_ann_gate_reports_whole_index_decisions_up_front() {
		let ctx = root_ctx();

		let allow = PhysicalTableSelect::new(PhysicalPermission::Allow, ctx.clone());
		assert_eq!(allow.allows_every_doc(), Some(true));

		let deny = PhysicalTableSelect::new(PhysicalPermission::Deny, ctx.clone());
		assert_eq!(deny.allows_every_doc(), Some(false));

		// Conditional cannot be decided for the whole index, so the search must
		// gate each candidate individually.
		let conditional = PhysicalTableSelect::new(
			PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await),
			ctx.clone(),
		);
		assert_eq!(conditional.allows_every_doc(), None);
	}

	#[tokio::test]
	async fn the_ann_gate_decides_a_candidate_with_the_same_predicate() {
		let ctx = root_ctx();
		let gate = PhysicalTableSelect::new(
			PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await),
			ctx.clone(),
		);

		let rid = Arc::new(RecordId {
			table: "doc".into(),
			key: crate::val::RecordIdKey::Number(1),
		});
		let allowed = Arc::new(Record::new(val("{ public: true }").await));
		let denied = Arc::new(Record::new(val("{ public: false }").await));

		let mut stack = reblessive::tree::TreeStack::new();
		let (yes, no) = stack
			.enter(|stk| async {
				let yes = gate.allows_doc(stk, &rid, &allowed).await.unwrap();
				let no = gate.allows_doc(stk, &rid, &denied).await.unwrap();
				(yes, no)
			})
			.finish()
			.await;
		assert!(yes);
		assert!(!no);
	}
}
