//! Query planner for the engine.
//!
//! The planner binds a lowered plan ([`LogicalOp`]) for one transaction and one principal, hands
//! the bound plan to the IR planning and the VM ([`crate::exec::vm::plan_checked`]), and supplies
//! what planning reads: a table source's access path and the pushdowns it admits, the clauses a
//! pipeline collects, the analysis of an aggregation, the targets of a write. A logical scalar
//! ([`LogicalExpr`]) is described by [`Planner::scalar_metadata`] and evaluated as the VM unit of
//! the [`Clause`] built from that description. The planner's input is the logical plan and nothing
//! else: the surface statement is lowered by `surrealdb_expr::expr::logical::lower`, bound by
//! [`crate::exec::logical::bind`], and only then planned here. No expression-layer value
//! (`crate::expr::Expr`) enters the planner; a GQL `MATCH` plan's scalars are `LogicalExpr` and are
//! described like every other scalar.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────┐  lower  ┌───────────┐  bind  ┌───────────┐  vm::plan  ┌─────────┐
//! │ expr::Expr │ ──────► │ LogicalOp │ ─────► │ LogicalOp │ ─────────► │ Program │
//! │ (surface)  │         │ (unbound) │        │  (bound)  │            │  (VM)   │
//! └────────────┘         └───────────┘        └───────────┘            └─────────┘
//! ```
//!
//! # Entry points
//!
//! - [`Planner::scalar_metadata`] describes a scalar; the description is inert, and the VM unit of
//!   the [`Clause`] built from it evaluates it.
//! - [`plan_statement_inner`] binds a lowered statement and plans it as a VM program for the
//!   executor.
//! - [`plan_logical_inner`] plans a plan a front end produced, with no statement behind it.
//! - [`compile_unit_expr`] compiles one scalar to a [`Clause`] from inside a running query — a
//!   closure body, a dynamic source, a `DEFINE PARAM` value.

mod aggregate;
mod cycle_guard;
pub(crate) mod fold;
mod path;
pub(crate) mod placement;
mod scalar_metadata;
pub(crate) use path::contains_repeat_recurse;
// GQL `MATCH` planning (`plan_match`).
pub(crate) mod match_backend;
pub(crate) mod match_plan;
pub(crate) mod name_scope;
pub(crate) mod row_scope;
mod select;
mod source;
pub(crate) mod util;
pub(crate) mod write;

use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use aggregate::analyse_aggregation;
pub(crate) use cycle_guard::{CycleGuard, UnitKey};
pub(crate) use select::{
	RestrictedPrefixes, adjust_direction_for_order, decide_union_merge,
	detect_order_for_composite_union, drop_restricted_indexes, output_field_path,
};

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::timers::{RequestTiming, compile_phase, compile_recorded_catalog_reads};
use crate::exec::{Clause, Error as ExecError, ExecOperator};
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalOp, LogicalRecordIdKey};
use crate::val::Value;

/// Query planner that converts logical plans to physical execution plans.
///
/// The `Planner` holds shared resources (context, function registry) to avoid
/// passing them through every function call. Methods on `Planner` are spread
/// across submodules:
///
/// - `fold` — the bottom-up compilation of a [`LogicalOp`] tree
/// - [`select`] — SELECT source and pipeline planning
/// - [`aggregate`] — GROUP BY and aggregate extraction
/// - [`path`] — path-to-physical-step compilation
/// - [`source`] — lookup, index function, and source planning
/// - [`match_plan`] — GQL `MATCH` planning
/// - [`util`] — pure utility functions
///
/// # Auth-aware planning
///
/// `Planner::auth` carries the calling statement's `Arc<Auth>` when the
/// caller has one in scope. Plan-time decisions that depend on whether
/// permission evaluation will actually run at execute time use the
/// `should_check_perms_for_*` helpers on `Planner` rather than re-deriving
/// the logic from `auth_enabled` + the `Auth` API directly. The helpers
/// mirror `crate::exec::permission::should_check_perms`, default to
/// "permissions apply" when the auth principal isn't wired through (deep
/// nested compilation, txn-less paths), and keep the auth check
/// consistent across the planner.
/// The version a planner's catalog reads are made at.
///
/// A source the analyzer resolved is resolved as of the `VERSION` its
/// statement names, when the statement's expression denotes a stamp at bind
/// time; the physical planner reads the same table at the same stamp when it
/// resolves the scan's context, so the plan's nodes and the scan agree on the
/// definition they enforce. An expression only the runtime can evaluate has
/// no stamp at plan time: the analyzer leaves the sources beneath it
/// unresolved, and a plan-time catalog read for them is declined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlanTimeVersion {
	/// No `VERSION`: the transaction's current state.
	Current,
	/// A `VERSION` denoting this stamp.
	At(u64),
}

/// What a call of a user-defined function does, resolved through its call
/// graph at plan time ([`Planner::resolve_custom_function`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedCustomFunction {
	/// What the reachable bodies may do to the transaction.
	pub(crate) access_mode: crate::exec::AccessMode,
	/// Whether calls for several rows must run one after another, in row
	/// order.
	pub(crate) keeps_row_order: bool,
}

#[cfg(test)]
thread_local! {
	/// How many binds, plans and expression compiles the planner has made
	/// on this thread, for tests that must know a run compiled nothing.
	pub(crate) static COMPILES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct Planner<'ctx> {
	/// The frozen context containing query parameters, capabilities, and session info.
	///
	/// Its value chain is what a parameter read binds against at plan time.
	/// A user-defined function body is planned against a context of its own
	/// ([`Self::body_scope`]) that holds the request's parameters and the row
	/// names and none of the caller's `LET` or `FOR` bindings; a body binds
	/// only the session's own values ([`UNIT_RUNTIME_PARAMS`]) at plan time,
	/// and reads every other name when it runs. A privileged frame
	/// ([`Self::privileged`]) plans against an isolated context.
	ctx: FrozenContext,
	/// Cached reference to the function registry for aggregate/projection detection.
	function_registry: &'ctx FunctionRegistry,
	/// Optional transaction for plan-time index resolution.
	///
	/// When present, the planner can resolve table definitions and indexes
	/// at plan time, which access-path choice and optimizations like sort
	/// elimination via [`OutputOrdering`] read.
	///
	/// When `None`, the access path of a source is resolved when the program
	/// runs.
	pub(crate) txn: Option<Arc<crate::kvs::Transaction>>,
	/// Optional namespace name for plan-time catalog lookups.
	pub(crate) ns: Option<String>,
	/// Optional database name for plan-time catalog lookups.
	pub(crate) db: Option<String>,
	/// Optional VERSION expression from the enclosing SELECT statement.
	///
	/// Propagated to the graph traversals planned beneath the statement so
	/// that they respect the VERSION clause.
	pub(crate) version: Option<Arc<Clause>>,
	/// The version the catalog reads planning makes are made at — the stamp
	/// the analyzer resolved the pipeline's sources under. Set by the SELECT
	/// pipeline from its `VERSION` clause, and inherited by a nested pipeline
	/// without one, so a source resolved under an enclosing `VERSION` is
	/// read at that stamp wherever it is planned.
	pub(crate) plan_time_version: PlanTimeVersion,
	/// Optional auth principal for the planning statement.
	///
	/// Forwarded by the executor when the calling `ExecutionContext` (or
	/// `Options`) is in scope. The planner only consults this for plan-time
	/// decisions that depend on whether permission evaluation will actually
	/// run at execute time — chiefly the `Permission::Full` eligibility check
	/// in [`try_fast_path_pair`]. `None` means "assume permissions apply",
	/// which keeps every nested / sub-statement planner conservative without
	/// needing every call site to thread the principal through.
	pub(crate) auth: Option<Arc<crate::iam::Auth>>,
	/// Whether the frame this planner compiles for suppresses permission
	/// enforcement, as
	/// [`RootContext::perms_suppressed`](crate::exec::context::RootContext)
	/// suppresses it at runtime.
	///
	/// Carried into binding, where it decides that no permission filter and
	/// no field guard is planted over the sources the plan reads: the frame
	/// reads every row whatever its principal may select, and a plan that
	/// filtered them would compute a different answer rather than a
	/// differently-authorised one. `false` — enforcement assumed — for every
	/// planner whose caller does not say otherwise.
	pub(crate) perms_suppressed: bool,
	/// Plan-time cycle detector for permission and computed-field
	/// compilation. Default empty for fresh planners (`new` / `with_txn`);
	/// nested planners spawned to compile a permission or computed-field
	/// body inherit the parent's guard via [`Planner::with_cycle_guard`].
	///
	/// See [`cycle_guard`] for the design rationale.
	pub(crate) cycle_guard: CycleGuard,
	/// Cached `(NamespaceId, DatabaseId)` lookup keyed by `(ns, db)`.
	///
	/// Resolved on first use by [`Planner::ns_db_ids`]. `(ns, db, txn)` are
	/// immutable for the planner's lifetime, so the result is stable.
	/// `None` after init means the lookup failed (txn unavailable or
	/// namespace/db not yet created) — callers fall back to runtime
	/// resolution.
	ns_db_ids_cache:
		tokio::sync::OnceCell<Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>>,
	/// MATCHES clauses of the WHERE condition being planned, keyed by match
	/// reference, resolved by index functions such as `search::highlight` into
	/// the entry they report against. Written only by
	/// [`Self::set_planning_scopes`], which owns the inherit-then-override rule.
	/// `None` where no MATCHES is in scope, which is what makes those functions
	/// report a missing MATCHES clause.
	matches_context: Option<Arc<crate::exec::function::MatchesContext>>,
	/// Shared KNN distance slot for the WHERE condition being planned, through
	/// which the ANN scan publishes each candidate's distance and
	/// `vector::distance::knn()` reads it back. Written only by
	/// [`Self::set_planning_scopes`], and `None` when no KNN operator is in
	/// scope.
	knn_context: Option<Arc<crate::exec::function::KnnContext>>,
	/// The user-defined functions whose bodies are being compiled into this
	/// plan, outermost first: the planner compiling a body called from the
	/// statement holds `[f]`, the one compiling a body `f` calls holds
	/// `[f, g]`. A call to a function on the path is a recursive edge: it is
	/// given the frame's shared slot, which the body's plan is published into
	/// once compiled, so every level of the recursion evaluates that one plan
	/// and the runtime's depth budget bounds it
	/// ([`Self::plan_function_body`]). Empty for a statement's planner.
	function_path: Vec<FunctionFrame>,
	/// What the stored code being planned spends of the computation-depth
	/// budget where it is evaluated one expression at a time
	/// ([`crate::exec::vm::depth`]), for code that is not the body on top of
	/// [`Self::function_path`]: an event clause. `None` elsewhere.
	depth_sites: Option<Arc<crate::exec::vm::depth::ExprDepth>>,
	/// Where the statement being planned stands below the statement the
	/// session runs, as the depth a write reached from there resumes the
	/// computation-depth count from ([`crate::exec::vm::depth`]): two for each
	/// `IF` branch it is in — the branch's re-entry and the branch's block —
	/// and one more where it is in a block, a loop, an expression or `FROM` at
	/// all.
	statement_nesting: StatementNesting,
	/// Re-entry nesting depth for this planner, the counterpart of
	/// `Options::dive` (and bounded by the same `max_computation_depth`).
	///
	/// This is an **immutable** construction parameter — set once via
	/// [`Planner::with_depth`] and only ever read. A top-level planner starts at
	/// 0. Whenever execution re-enters the planner to compile a *new* query at
	/// runtime — an `eval` string, a user-defined-function body — the fresh
	/// planner is seeded at `parent_depth + 1`, so the count continues across the
	/// boundary rather than resetting. The within-plan expression/statement tree
	/// is already bounded by the parser's recursion limits, so this only needs to
	/// bound the unbounded axis: the chain of runtime re-entries. Rejecting once
	/// it exceeds `max_computation_depth` is what stops `eval`/UDF recursion from
	/// growing the native stack without limit.
	depth: u32,
	/// Parameter names an enclosing `FOR`, closure, user-defined function or
	/// block `LET` binds around the expression being compiled.
	///
	/// Those constructs bind a user-chosen name unchecked, so a read of one
	/// inside their body is the value they bind and not the ambient planning
	/// context's. The planner reads a parameter's plan-time value only where
	/// the name is absent here, as for `$session`
	/// ([`Self::constant_root_value`](super::planner::path)) and a KNN query
	/// vector.
	shadowed_params: Arc<[String]>,
	/// The session's options and the catalog ids the statement's planning
	/// holds, on the executor's planner for a statement it is about to run.
	/// With them a block is planned against what its own schema statements
	/// write, which it learns by running each one speculatively
	/// ([`crate::exec::vm`]); without them a block learns it by compiling
	/// again once they have run.
	pub(crate) speculation: Option<Arc<crate::exec::vm::speculate::Speculating>>,
	/// Whether a block plans what follows a statement that may change the
	/// catalog, or the names the session resolves in, late-bound
	/// ([`Self::late_binding`]), rather than declining it.
	late_after_schema: bool,
	/// Whether the statement being planned runs after a change to the catalog
	/// its plan cannot know: binding resolves nothing of the catalog, so each
	/// table, parameter and function the statement names is loaded when it
	/// runs, as the catalog then stands ([`Self::late_binding`]).
	late_bound: bool,
	/// Whether the statement being planned is one a block compiles when it
	/// reaches it, after the block's first schema statement, rather than when
	/// the block is planned ([`Self::compiled_when_reached`]).
	when_reached: bool,
	/// How the statement being planned reaches what code it runs may redefine
	/// while it runs ([`CatalogWithin`]).
	within: CatalogWithin,
	/// Whether what this planner plans is a stored clause — an event's `WHEN`
	/// or `THEN`, a field's `VALUE`, `ASSERT`, `DEFAULT` or `COMPUTED` — or a
	/// statement nested in one. Such a clause is evaluated an expression at a
	/// time, so a `LET` written where its value is read (`RETURN (LET …)`,
	/// `LET $a = (LET …)`) raises when it is evaluated rather than binding
	/// ([`Self::in_stored_clause`]).
	stored_clause: bool,
	/// Whether access-path selection may trust the value shapes the indexes
	/// have recorded ([`surrealdb_datastore::index_shapes`]): set only for the
	/// planner of a program that cannot write ([`Self::trusting_index_shapes`]).
	trusts_index_shapes: bool,
	/// Whether the program this planner plans is a statement of a request, run in the frame the
	/// request's own transaction opens: no caller's `VERSION` frame is inherited, so a read the
	/// plan states at the current version is read at it ([`Self::at_request_root`]). A unit
	/// compiled on its own, and every planner derived to compile one, is not.
	request_root: bool,
	/// Whether the statement planned is the value a top-level `LET` binds, whose rows the
	/// statements after the `LET` read in the order they were bound
	/// ([`Self::binding_rows`]).
	binds_rows: bool,
	/// The table of the records each request variable names that a statement of a request reads
	/// as the one target of its write, by the variable's name: the class the statement cache keeps
	/// the statement under ([`Self::naming_targets`]). Read only by the bind of a request's own
	/// statement.
	named_targets: Arc<[(String, surrealdb_strand::TableName)]>,
	/// Whether a write of a request's statement binds its table's stages as ops of its own rather
	/// than running the bundle the compile links for the table ([`Self::inlining_stages`]).
	stages_inline: bool,
	/// Whether this planner plans a statement the statement cache keeps, whose write may link
	/// its table's stages ([`Self::linking_stages`]).
	stages_linked: bool,
}

/// How a statement reaches the catalog objects that code it runs may redefine
/// while it runs, in its own transaction: an embedded script, a closure
/// invoked through a value, a function body that reaches either, or a schema
/// statement held in one of its expressions. The statement's own targets are
/// loaded once, when it starts, whatever that code does; what it reaches
/// through calls and the statements written in its expressions is loaded as
/// the catalog stands when each runs, as follows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CatalogWithin {
	/// No catalog definition is inlined into the statement — no function
	/// body, no `DEFINE PARAM` value — and each call of a user-defined
	/// function runs the definition the transaction reads when the call is
	/// made, through the function's call unit, which the compile cache serves
	/// by the version of that definition and of every definition it inlined.
	pub(crate) calls_current: bool,
	/// The statements written in the statement's expressions are planned
	/// late-bound, each loading the catalog objects it names when it runs:
	/// the statement writes records, or holds a write or a schema statement
	/// in an expression.
	pub(crate) nested_late: bool,
}

/// The names a unit reads when it runs rather than folding the planning
/// context's value: the session's own values, which differ between the
/// principals a unit serves ([`Planner::unit_root`]).
pub(crate) const UNIT_RUNTIME_PARAMS: &[&str] = surrealdb_cnf::PROTECTED_PARAM_NAMES;

/// Where a statement stands below the statement the session runs
/// ([`Planner::statement_nesting`]).
#[derive(Debug, Clone, Copy, Default)]
struct StatementNesting {}

/// One function body under compilation on a planner's [`Planner::function_path`].
#[derive(Clone, Debug)]
pub(crate) struct FunctionFrame {}

impl<'ctx> Planner<'ctx> {
	/// Create a new planner with the given context (no transaction).
	///
	/// Table sources will use the generic `Scan` operator that resolves
	/// indexes at execution time. This is used by callers that compile a
	/// scalar without transaction access.
	///
	/// `registry` is the function registry every function name in the planned
	/// expression resolves against; callers holding an `ExecutionContext` pass
	/// its `function_registry()`.
	pub fn new(ctx: &FrozenContext, registry: &'ctx FunctionRegistry) -> Self {
		crate::exec::compiles::assert_may_plan();
		Self {
			ctx: Arc::clone(ctx),
			function_registry: registry,
			txn: None,
			ns: None,
			db: None,
			version: None,
			plan_time_version: PlanTimeVersion::Current,
			auth: None,
			perms_suppressed: false,
			cycle_guard: CycleGuard::default(),
			ns_db_ids_cache: tokio::sync::OnceCell::new(),
			matches_context: None,
			knn_context: None,
			function_path: Vec::new(),
			depth_sites: None,
			statement_nesting: StatementNesting::default(),
			depth: 0,
			shadowed_params: Arc::from(Vec::new()),
			speculation: None,
			late_after_schema: false,
			late_bound: false,
			when_reached: false,
			within: CatalogWithin::default(),
			stored_clause: false,
			trusts_index_shapes: false,
			request_root: false,
			binds_rows: false,
			named_targets: Arc::from(Vec::new()),
			stages_inline: false,
			stages_linked: false,
		}
	}

	/// Create a new planner with the given context and transaction.
	///
	/// When a transaction is provided, the planner can resolve table
	/// definitions and indexes at plan time, choosing access paths and
	/// enabling optimizations like sort elimination.
	///
	/// `registry` is as in [`Planner::new`].
	pub fn with_txn(
		ctx: &FrozenContext,
		registry: &'ctx FunctionRegistry,
		txn: Arc<crate::kvs::Transaction>,
		ns: Option<String>,
		db: Option<String>,
	) -> Self {
		crate::exec::compiles::assert_may_plan();
		Self {
			ctx: Arc::clone(ctx),
			function_registry: registry,
			txn: Some(txn),
			ns,
			db,
			version: None,
			plan_time_version: PlanTimeVersion::Current,
			auth: None,
			perms_suppressed: false,
			cycle_guard: CycleGuard::default(),
			ns_db_ids_cache: tokio::sync::OnceCell::new(),
			matches_context: None,
			knn_context: None,
			function_path: Vec::new(),
			depth_sites: None,
			statement_nesting: StatementNesting::default(),
			depth: 0,
			shadowed_params: Arc::from(Vec::new()),
			speculation: None,
			late_after_schema: false,
			late_bound: false,
			when_reached: false,
			within: CatalogWithin::default(),
			stored_clause: false,
			trusts_index_shapes: false,
			request_root: false,
			binds_rows: false,
			named_targets: Arc::from(Vec::new()),
			stages_inline: false,
			stages_linked: false,
		}
	}

	/// Attach the calling statement's auth principal.
	///
	/// See the `auth` field for what the planner uses this for. Callers
	/// that have an `Options` or `ExecutionContext` in scope should clone
	/// the `Arc<Auth>` and pass it through; callers without it (deeply
	/// nested permission / computed-field compilation, txn-less paths)
	/// can leave it unset and the planner will fall back to the
	/// conservative defaults.
	#[must_use]
	pub(crate) fn with_auth(mut self, auth: Arc<crate::iam::Auth>) -> Self {
		self.auth = Some(auth);
		self
	}

	/// Compile for a frame whose permission enforcement is suppressed (see the
	/// `perms_suppressed` field).
	///
	/// Passed by a caller that holds the frame the plan will run in, so that
	/// the plan is compiled for the enforcement the runtime will actually
	/// apply. A caller that leaves it unset gets enforcement assumed.
	#[must_use]
	pub(crate) fn with_perms_suppressed(mut self, suppressed: bool) -> Self {
		self.perms_suppressed = suppressed;
		self
	}

	/// The runtime's `should_check_perms(View)` answer for a read of `ns`/`db`
	/// by this planner's principal, decided at plan time.
	///
	/// Returns `true` when permissions will be evaluated at execute time —
	/// the conservative default, and the answer whenever no auth principal
	/// is attached. Returns `false` only when the runtime would bypass
	/// permission evaluation by [`crate::exec::permission::action_is_checked`]:
	/// a principal with the viewer role whose actor level covers `ns`/`db`,
	/// or an anonymous session on a datastore with auth disabled.
	pub(crate) fn should_check_perms_for_view(&self, ns: &str, db: &str) -> bool {
		let Some(ref auth) = self.auth else {
			// No auth attached -- can't reason about runtime behaviour.
			return true;
		};
		crate::exec::permission::action_is_checked(
			self.ctx.auth_enabled(),
			auth,
			crate::iam::Action::View,
			Some(ns),
			Some(db),
		)
	}

	/// The digest of the class of `auth` in this planner's namespace and
	/// database ([`crate::exec::vm::program::PrincipalClass`]): what a unit
	/// lowered here for `auth` admits.
	pub(crate) fn class_digest(&self, auth: Option<&crate::iam::Auth>) -> u64 {
		crate::exec::vm::program::UnitFrame::digest(
			self.ctx.auth_enabled(),
			auth,
			self.ns.as_deref(),
			self.db.as_deref(),
		)
	}

	/// Whether this planner compiles for a caller whose reads are
	/// permission-checked: [`Self::should_check_perms_for_view`] for the
	/// planner's own namespace and database, and `true` (enforcement assumed)
	/// when it has no names to check against.
	pub(crate) fn compiles_for_checked_caller(&self) -> bool {
		match (self.ns.as_deref(), self.db.as_deref()) {
			(Some(ns), Some(db)) => self.should_check_perms_for_view(ns, db),
			_ => true,
		}
	}

	/// Whether the statement being planned reads under the permissions of a
	/// caller they are enforced on: [`Self::compiles_for_checked_caller`],
	/// unless permission checks are suppressed for the plan.
	pub(crate) fn reads_are_checked(&self) -> bool {
		!self.perms_suppressed && self.compiles_for_checked_caller()
	}

	/// Inherit a parent planner's cycle guard.
	///
	/// Used when a nested planner is constructed (during permission /
	/// computed-field body compilation, scalar subquery planning, etc.)
	/// so the cycle detector sees the parent's in-progress tables. Without
	/// this, a self-referential permission like
	/// `WHERE (SELECT FROM same_table) != NONE` would re-enter
	/// table-context resolution on the inner subquery and recurse.
	#[must_use]
	pub(crate) fn with_cycle_guard(mut self, guard: CycleGuard) -> Self {
		self.cycle_guard = guard;
		self
	}

	pub(crate) fn with_depth(mut self, depth: u32) -> Self {
		self.depth = depth;
		self
	}

	/// The re-entry nesting depth recorded onto nodes planned here (see the
	/// `depth` field). Constant for the planner's lifetime.
	#[inline]
	pub(crate) fn current_depth(&self) -> u32 {
		self.depth
	}

	/// Reject planning once this planner's re-entry depth has exceeded
	/// `max_computation_depth`. Checked at the entry of [`Planner::scalar_metadata`]
	/// and when a plan is planned — the points at which a runtime re-entry begins
	/// compiling a fresh query — so a runaway `eval`/UDF chain stops here
	/// instead of growing the native stack.
	#[inline]
	fn check_depth(&self) -> Result<(), Error> {
		if self.depth > self.ctx.config.exec.max_computation_depth {
			return Err(ExecError::ComputationDepthExceeded.into());
		}
		Ok(())
	}

	/// Plan-time transaction, when available. Returns `None` for txn-less
	/// planners (constructed via `Planner::new`); the caller must fall back
	/// to runtime resolution in that case.
	#[inline]
	/// The frozen context the statement is planned under.
	pub(crate) fn frozen(&self) -> &FrozenContext {
		&self.ctx
	}

	pub(crate) fn txn(&self) -> Option<&Arc<crate::kvs::Transaction>> {
		self.txn.as_ref()
	}

	/// Namespace name for plan-time catalog lookups, when set.
	#[inline]
	pub(crate) fn ns(&self) -> Option<&str> {
		self.ns.as_deref()
	}

	/// Database name for plan-time catalog lookups, when set.
	#[inline]
	pub(crate) fn db(&self) -> Option<&str> {
		self.db.as_deref()
	}

	/// Get the function registry.
	#[inline]
	pub fn function_registry(&self) -> &'ctx FunctionRegistry {
		self.function_registry
	}

	/// Look up (NamespaceId, DatabaseId) from the planner's transaction.
	///
	/// Cached on first hit — `(self.ns, self.db, self.txn)` are immutable
	/// for the planner's lifetime, so callers (binding, parameter resolution,
	/// SELECT planning, etc.) can hit this many times per plan without
	/// re-reading the catalog.
	///
	/// Returns `None` when the transaction or namespace/database is
	/// unavailable; callers fall back to runtime resolution.
	///
	/// Logging policy on the *first* lookup:
	/// - **`Ok(None)`** ("ns/db doesn't exist yet"): `debug!` — common during schema-creation
	///   flows; not actionable.
	/// - **`Err(e)`** (storage error, lock contention, etc.): `warn!` — exceptional. Plan-time
	///   index resolution / sort elimination silently degrades, so it should be visible in
	///   production monitoring.
	pub(crate) async fn ns_db_ids(
		&self,
	) -> Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)> {
		use crate::catalog::providers::DatabaseProvider;

		self.ns_db_ids_cache
			.get_or_init(|| async {
				let (txn, ns, db) = match (&self.txn, &self.ns, &self.db) {
					(Some(txn), Some(ns), Some(db)) => (txn, ns, db),
					_ => return None,
				};
				match txn.get_db_by_name(ns, db, None).await {
					Ok(Some(db_def)) => Some((db_def.namespace_id, db_def.database_id)),
					Ok(None) => {
						tracing::debug!(
							ns = %ns,
							db = %db,
							"plan-time db lookup returned None; falling back to runtime resolution",
						);
						None
					}
					Err(e) => {
						tracing::warn!(
							ns = %ns,
							db = %db,
							error = %e,
							"plan-time db lookup failed; falling back to runtime resolution \
							 (plan-time optimisations may be unavailable)",
						);
						None
					}
				}
			})
			.await
			.as_ref()
			.copied()
	}

	/// Bind an unbound plan for this planner's transaction and principal.
	///
	/// The binding passes need the statement's context, the transaction's
	/// catalog snapshot and the caller's authority — exactly what this planner
	/// was constructed with — so callers bind through the planner rather than
	/// assembling a [`BindContext`](crate::exec::logical::BindContext) of their
	/// own.
	pub(crate) async fn bind(&self, op: LogicalOp) -> Result<LogicalOp, Error> {
		#[cfg(test)]
		COMPILES.with(|compiles| compiles.set(compiles.get() + 1));
		let row_scoped = self.row_scoped();
		self.bind_scoped(op, &row_scoped).await
	}

	/// The names this planner never resolves a value for at plan time: the
	/// row parameters, and every name bound around what it plans
	/// ([`Self::param_is_shadowed`]), whose value is the one the binding
	/// gives it when the plan runs, not the ambient context's.
	pub(crate) fn row_scoped(&self) -> Vec<&str> {
		let mut names: Vec<&str> = util::SELECT_ITERATION_PARAMS.to_vec();
		names.extend(self.shadowed_params.iter().map(String::as_str));
		names
	}

	/// [`Self::bind`] with `row_scoped` as the names the plan binds per row
	/// or per call, which binding never substitutes a value for. A statement
	/// binds the row parameters; a function body binds its arguments too.
	/// The session's own values this planner shadows
	/// ([`Self::reading_session_values`]) are never substituted either,
	/// whatever `row_scoped` names: what a unit planner binds is served to
	/// every principal of its frame's class.
	pub(crate) async fn bind_scoped(
		&self,
		mut op: LogicalOp,
		row_scoped: &[&str],
	) -> Result<LogicalOp, Error> {
		crate::exec::compiles::note();
		// Before the binding passes, so a read of a `LET` of a literal reaches the
		// rules and the access paths as that literal; an `IF` condition is left as
		// written. Every statement, nested statement and function body is bound
		// here, so none is planned without it.
		crate::exec::logical::constants::propagate(&mut op);
		let session_values: Vec<&str> = UNIT_RUNTIME_PARAMS
			.iter()
			.copied()
			.filter(|name| !row_scoped.contains(name) && self.param_is_shadowed(name))
			.collect();
		let widened: Vec<&str>;
		let row_scoped = if session_values.is_empty() {
			row_scoped
		} else {
			widened = row_scoped.iter().copied().chain(session_values).collect();
			widened.as_slice()
		};
		let ns_db = crate::kvs::catalog_reads::sealed(false, self.ns_db_ids()).await;
		// A late-bound statement resolves nothing of the catalog here.
		let (txn, ns_db) = match self.late_bound {
			true => (None, None),
			false => (self.txn.clone(), ns_db),
		};
		let bind_ctx = crate::exec::logical::BindContext {
			ctx: &self.ctx,
			txn,
			ns_db,
			auth: self.auth.clone(),
			perms_suppressed: self.perms_suppressed,
			row_scoped,
			registry: self.function_registry,
			inlines_definitions: !self.within.calls_current,
			binds_nested: !self.within.nested_late,
		};
		// Binding reads the catalog, so it runs unsealed wherever it is
		// reached from ([`crate::kvs::catalog_reads::sealed`]).
		crate::kvs::catalog_reads::sealed(false, crate::exec::logical::bind(op, &bind_ctx)).await
	}

	/// The node's compiled units and the transaction a body of a function
	/// compiled by this planner may be kept under; `None` when such a body is
	/// compiled for this plan alone: one reached from inside another body's
	/// compilation, one read under `VERSION`, one planned over a speculation's
	/// overlay, or a planner with no transaction or database.
	pub(crate) fn unit_cache(
		&self,
	) -> Option<(Arc<crate::exec::vm::units::CompiledUnits>, Arc<crate::kvs::Transaction>)> {
		// A unit lowered under a `VERSION` is lowered against the definitions
		// stored at its stamp, which no key of the node's units holds.
		if !self.function_path.is_empty()
			|| self.version.is_some()
			|| !matches!(self.plan_time_version, PlanTimeVersion::Current)
		{
			return None;
		}
		#[cfg(test)]
		if crate::exec::vm::units::OFF.with(std::cell::Cell::get) {
			return None;
		}
		let txn = self.txn.clone()?;
		if txn.speculating() {
			return None;
		}
		let units = Arc::clone(self.ctx.compiled_units()?);
		Some((units, txn))
	}

	/// What the compile cache keeps the unit of `fn::name` under, as this
	/// planner's transaction reads the function: its catalog version, or the
	/// content hash of a definition no write has stamped; `None` for a planner
	/// with no transaction or database, or a function that is neither stamped
	/// nor defined.
	pub(crate) async fn function_unit_key(
		&self,
		name: &str,
	) -> Option<crate::exec::vm::compile_cache::UnitKey> {
		use surrealdb_datastore::values::catalog_version::CatalogObject;

		use crate::exec::vm::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
		let ids = self.ns_db_ids().await?;
		let txn = self.txn.as_ref()?;
		let version =
			match txn.catalog_object_version(ids.0, ids.1, CatalogObject::Function(name)).await {
				Ok(Some(version)) => ObjectVersion::Stamped(version),
				Ok(None) => {
					let cache = self.ctx.get_cache();
					let snapshot = crate::kvs::cache::ds::FunctionSnapshot::for_function(
						cache.as_deref(),
						txn,
						ids.0,
						ids.1,
						name,
					)
					.await
					.ok()??;
					ObjectVersion::Content(snapshot.content_hash())
				}
				Err(_) => return None,
			};
		Some(UnitKey {
			ns: self.ns.clone()?,
			db: self.db.clone()?,
			ids,
			object: UnitObject::Function(name.to_owned()),
			version,
			build: BuildId::current(self.ctx.get_capabilities()),
		})
	}

	/// What the compile cache keeps the read unit of `table` under, as this
	/// planner's transaction reads the table: its catalog version; `None` for
	/// a planner with no transaction or database, or a table no catalog write
	/// of this build has stamped, whose read is compiled for the plan alone.
	pub(crate) async fn table_unit_key(
		&self,
		table: &crate::val::TableName,
	) -> Option<crate::exec::vm::compile_cache::UnitKey> {
		use crate::exec::vm::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
		let ids = self.ns_db_ids().await?;
		let txn = self.txn.as_ref()?;
		let version = txn.catalog_version(ids.0, ids.1, table).await.ok()??;
		Some(UnitKey {
			ns: self.ns.clone()?,
			db: self.db.clone()?,
			ids,
			object: UnitObject::TableRead(table.clone()),
			version: ObjectVersion::Stamped(version),
			build: BuildId::current(self.ctx.get_capabilities()),
		})
	}

	/// Whether a catalog unit this planner lowers may be kept: one lowered
	/// with nothing the key of a unit does not hold — a `VERSION`, a planning
	/// scope of a `MATCHES` or KNN predicate, or a table or event compilation
	/// open on its cycle guard — is lowered for this plan alone.
	pub(crate) fn keeps_catalog_units(&self) -> bool {
		matches!(self.plan_time_version, PlanTimeVersion::Current)
			&& self.matches_context.is_none()
			&& self.knn_context.is_none()
			&& self.cycle_guard.is_idle()
	}

	/// This planner reading the catalog at `stamp`, a `VERSION` stamp, where
	/// it is one.
	pub(crate) fn at_stamp(mut self, stamp: Option<u64>) -> Planner<'ctx> {
		if let Some(stamp) = stamp {
			self.plan_time_version = PlanTimeVersion::At(stamp);
		}
		self
	}

	/// The planner a unit is compiled with: this planner's transaction,
	/// namespace, database, principal and permission contract, at the depth
	/// of a statement, binding no name of the scope around it and none of the
	/// planning scope of the statement being planned. A unit is served to any
	/// caller of its frame, at any depth and in any scope: a body binds only
	/// its own arguments, and the run counts the re-entry depth
	/// ([`crate::exec::vm::compile_cache`]).
	///
	/// The session's own values — `$auth`, `$access`, `$token`, `$session` —
	/// are read when the unit runs, never folded into its code
	/// ([`UNIT_RUNTIME_PARAMS`]), so a permission that reads `$auth` serves
	/// every principal of the unit's class with the caller's own. So is the
	/// clock: a `time::now()` is never folded, so a unit holds no instant.
	///
	/// A unit compiled where reads are made at a `VERSION` stamp is compiled
	/// at that stamp, from the definitions the stamp holds: the planner keeps
	/// this one's stamp, and the compile cache serves no unit to such a planner
	/// ([`Self::unit_cache`]), so a unit of the current definitions never
	/// stands for a stamped read.
	pub(crate) fn unit_root(&self) -> Planner<'ctx> {
		let mut planner = self.own_scope();
		planner.shadowed_params =
			UNIT_RUNTIME_PARAMS.iter().map(|name| (*name).to_owned()).collect();
		planner.depth = 0;
		planner.version.clone_from(&self.version);
		planner.plan_time_version = self.plan_time_version;
		planner
	}

	/// Whether the statement this planner plans inlines no catalog definition
	/// ([`CatalogWithin::calls_current`]).
	pub(crate) fn calls_current(&self) -> bool {
		self.within.calls_current
	}

	/// This planner with the session's own values ([`UNIT_RUNTIME_PARAMS`])
	/// shadowed, so what it compiles reads them when it runs and never folds
	/// the planning context's. Every planner whose compilation the compile
	/// cache keeps and serves to other principals of the frame's class plans
	/// with them shadowed: the class is not the principal, and a folded
	/// `$auth` would answer one principal's rows to another.
	pub(crate) fn reading_session_values(&self) -> Planner<'ctx> {
		let missing: Vec<String> = UNIT_RUNTIME_PARAMS
			.iter()
			.filter(|name| !self.param_is_shadowed(name))
			.map(|name| (*name).to_owned())
			.collect();
		self.shadowing(missing)
	}

	/// Whether the caller may call `time::now()`, so that the index analyzer may
	/// offer a comparison against it as a bound that reads the statement clock.
	/// The scan reads the clock without a function-name check of its own, so
	/// the check is made here, where the plan is chosen: a caller denied the
	/// function keeps the comparison as a filter, which raises the denial.
	pub(crate) fn allows_clock_bound(&self) -> bool {
		self.ctx.get_capabilities().allows_function_name("time::now")
	}

	/// The compiled definition of `fn::name` as this planner's transaction
	/// reads it, when a call planned here may be bound from it.
	pub(crate) async fn function_snapshot(&self, name: &str) -> FunctionLookup {
		use crate::kvs::cache::ds::FunctionSnapshot;

		// A call needs a selected namespace and database; entering one without
		// them raises the missing-selection error, whichever function it names.
		if self.ns.is_none() || self.db.is_none() {
			return FunctionLookup::Absent;
		}
		if self.version.is_some() {
			return FunctionLookup::Unavailable;
		}
		// A call under a `VERSION` runs the definition stored at its stamp; a
		// stamp only the run can evaluate names no definition to read.
		let version = match self.plan_time_version {
			PlanTimeVersion::Current => None,
			PlanTimeVersion::At(stamp) => Some(stamp),
		};
		let (Some(txn), Some((ns, db))) = (&self.txn, self.ns_db_ids().await) else {
			return FunctionLookup::Unavailable;
		};
		if !self.ctx.get_capabilities().allows_function_name(&format!("fn::{name}")) {
			return FunctionLookup::Absent;
		}
		let cache = self.ctx.get_cache();
		match FunctionSnapshot::for_function_at(cache.as_deref(), txn, ns, db, name, version).await
		{
			Ok(Some(snapshot)) => FunctionLookup::Found(snapshot),
			Ok(None) => FunctionLookup::Absent,
			Err(e) => {
				tracing::debug!("fn::{name} could not be read at plan time: {e:#}");
				FunctionLookup::Unavailable
			}
		}
	}

	/// The principal a body of `definition` is bound for when called from
	/// here: this planner's, capped by the definition's `AUTH LIMIT`. `None`
	/// for a limit that does not parse, and `Some(None)` for a planner with no
	/// principal.
	pub(crate) fn function_principal(
		&self,
		definition: &crate::catalog::FunctionDefinition,
	) -> Option<Option<Arc<crate::iam::Auth>>> {
		let limit = crate::iam::AuthLimit::try_from(&definition.auth_limit).ok()?;
		Some(self.auth.as_ref().map(|auth| Arc::new(auth.new_limited(&limit))))
	}

	/// This planner, its principal capped by `limit`: the planner for a
	/// clause that runs under the `AUTH LIMIT` of the definition holding it.
	pub(crate) fn limited(&self, limit: &crate::iam::AuthLimit) -> Planner<'ctx> {
		let mut planner = self.shadowing([]);
		planner.auth = self.auth.as_ref().map(|auth| Arc::new(auth.new_limited(limit)));
		planner
	}

	/// Whether a body bound for `principal`, as [`Self::function_principal`]
	/// answers it, is bound for a caller whose reads are permission-checked:
	/// it has a principal, is not under suppressed permissions, and the
	/// principal's reads are enforced in this planner's database.
	pub(crate) fn body_is_checked(&self, principal: Option<&Arc<crate::iam::Auth>>) -> bool {
		let Some(auth) = principal else {
			return false;
		};
		if self.perms_suppressed {
			return false;
		}
		match (self.ns.as_deref(), self.db.as_deref()) {
			(Some(ns), Some(db)) => crate::exec::permission::action_is_checked(
				self.ctx.auth_enabled(),
				auth,
				crate::iam::Action::View,
				Some(ns),
				Some(db),
			),
			_ => true,
		}
	}

	/// The body of the function `snapshot` reads, bound once for every call
	/// from code this planner plans, including one on the path being
	/// compiled, whose recursive edge the caller finds. The arguments, the
	/// row names and every name the body reads that the planner does not bind
	/// are read when the body runs, by the lexical rule, so the body is one
	/// generic unit whatever values a call passes. Fails with what the
	/// binding raised, or where the planner has no transaction or database.
	pub(crate) async fn prepare_snapshot(
		&self,
		name: &str,
		snapshot: &Arc<crate::kvs::cache::ds::FunctionSnapshot>,
	) -> Result<PreparedBody<'ctx>, PrepareError> {
		let (Some(txn), Some((ns, db))) = (&self.txn, self.ns_db_ids().await) else {
			return Err(PrepareError::NoDatabase);
		};
		let definition = snapshot.definition();
		let body = snapshot.body().map_err(PrepareError::Bind)?.clone();

		// The names the body binds itself, and the names it reads that are
		// bound nowhere the planner can see.
		let mut referenced: HashSet<String> = HashSet::new();
		let Ok(()) = body.visit_scalars_deep(&mut |scalar| {
			let _ = scalar.visit_deep(&mut |e| {
				if let LogicalExpr::Param(param) = e {
					referenced.insert(param.as_str().to_owned());
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			Ok::<_, std::convert::Infallible>(())
		});
		let (planner, _) = self.body_scope(snapshot).ok_or(PrepareError::AuthLimit)?;
		let mut row_scoped: Vec<&str> = util::SELECT_ITERATION_PARAMS.to_vec();
		row_scoped.extend(definition.args.iter().map(|(arg, _)| arg.as_str()));
		let mut unbound = Vec::new();
		for param in &referenced {
			if row_scoped.contains(&param.as_str()) {
				continue;
			}
			// Only the session's own values are read here. Any other name is
			// left to the runtime, which binds it in the frame the call runs in
			// by the lexical rule ([`crate::ctx::Context::lexical_value`]): the
			// body's arguments and bindings, the row names, the request's
			// parameters, and otherwise the database's `DEFINE PARAM`, never a
			// binding the caller made with `LET` or `FOR`. So no unit folds one
			// caller's binding for another.
			if !UNIT_RUNTIME_PARAMS.contains(&param.as_str()) {
				unbound.push(param.as_str());
				continue;
			}
			if let util::ParamBinding::Unbound =
				util::bind_param(param, &planner.ctx, Some(txn), Some((ns, db)), &row_scoped).await
			{
				unbound.push(param.as_str());
			}
		}
		row_scoped.extend(unbound);

		// A `LET` no statement may hold is refused from the stored text,
		// before binding folds away an arm that never runs. `DEFINE FUNCTION`
		// refuses such a body; a definition an earlier release stored may
		// still hold one.
		if let Some(error) = crate::exec::logical::invalid_let(&body) {
			return Err(PrepareError::Bind(error.into()));
		}
		let bound = planner.bind_scoped(body, &row_scoped).await.map_err(|e| {
			tracing::debug!("fn::{name} body did not bind: {e:#}");
			PrepareError::Bind(e.into())
		})?;
		Ok(PreparedBody {
			planner,
			bound,
		})
	}

	/// The planner a body of `snapshot` is compiled with, and the frame it
	/// compiles in: this planner's principal capped by the definition's
	/// `AUTH LIMIT`, one re-entry deeper, with the arguments shadowed and the
	/// calling statement's shadowed names kept shadowed, so that a name the
	/// caller binds is a run-time lookup in the body and never a plan-time
	/// value. The stamp of the statement's `VERSION` carries over: a body runs
	/// in the versioned frame of the call that enters it, so its reads are
	/// planned as of the same stamp.
	///
	/// The body is planned in the context a body reads
	/// ([`crate::ctx::Context::new_call_scope`]): this planner's request
	/// parameters, row names and protected names, and none of the bindings its
	/// statement's caller made with `LET` or `FOR`, which a body never reads.
	/// A privileged frame's planner holds an isolated context, which has no
	/// request parameter to pass on ([`ExecutionContext::privileged_frame`]).
	fn body_scope(
		&self,
		snapshot: &Arc<crate::kvs::cache::ds::FunctionSnapshot>,
	) -> Option<(Planner<'ctx>, FunctionFrame)> {
		let definition = snapshot.definition();
		let mut planner =
			self.shadowing(definition.args.iter().map(|(arg, _)| arg.as_str().to_owned()));
		planner.ctx = crate::ctx::Context::new_call_scope(&self.ctx).freeze();
		planner.request_root = false;
		planner.version = None;
		planner.matches_context = None;
		planner.knn_context = None;
		planner.within = CatalogWithin::default();
		planner.depth = self.depth + 1;
		planner.auth = self.function_principal(definition)?;
		let frame = FunctionFrame {};
		planner.function_path.push(frame.clone());
		planner.depth_sites = None;
		Some((planner, frame))
	}

	/// A planner for a body compiled in its own scope: a closure body.
	///
	/// Shares the context, transaction, principal, cycle guard and re-entry
	/// depth, and carries none of the enclosing statement's planning scope — no
	/// MATCHES registrations, no KNN slot, no `VERSION`, no `FOR UPDATE`. A
	/// closure body is evaluated wherever the closure is finally called, which
	/// is not necessarily the statement that wrote it, so it must not inherit
	/// what that statement registered.
	pub(crate) fn own_scope(&self) -> Planner<'ctx> {
		Planner {
			ctx: Arc::clone(&self.ctx),
			function_registry: self.function_registry,
			txn: self.txn.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			version: None,
			plan_time_version: PlanTimeVersion::Current,
			auth: self.auth.clone(),
			perms_suppressed: self.perms_suppressed,
			cycle_guard: self.cycle_guard.clone(),
			ns_db_ids_cache: self.ns_db_ids_cache.clone(),
			matches_context: None,
			knn_context: None,
			function_path: self.function_path.clone(),
			depth_sites: self.depth_sites.clone(),
			statement_nesting: self.statement_nesting,
			depth: self.depth,
			shadowed_params: Arc::clone(&self.shadowed_params),
			speculation: None,
			late_after_schema: self.late_after_schema,
			late_bound: false,
			when_reached: false,
			within: CatalogWithin::default(),
			stored_clause: false,
			trusts_index_shapes: false,
			request_root: false,
			binds_rows: false,
			named_targets: Arc::from(Vec::new()),
			stages_inline: false,
			stages_linked: false,
		}
	}

	/// This planner over an isolated view of its context: the planner of a
	/// statement held in definer code, which runs in a privileged frame that
	/// reads the session's own values and the database's parameters and none
	/// of the caller's bindings
	/// ([`crate::exec::ExecutionContext::privileged_frame`]).
	pub(crate) fn privileged(&self) -> Planner<'ctx> {
		let mut planner = self.shadowing(std::iter::empty());
		planner.ctx = crate::ctx::Context::new_isolated(&self.ctx).freeze();
		planner.request_root = false;
		planner
	}

	/// This planner with `names` added to [`Self::shadowed_params`]: the
	/// planner for the body of a `FOR`, a closure or a user-defined function,
	/// or for a block whose `LET`s bind those names.
	///
	/// Every other part of the planning scope is carried through unchanged —
	/// a `FOR` inside a `VERSION` is still a versioned read — except the
	/// catalog id cache, which is a cache and is re-filled on demand.
	pub(crate) fn shadowing(&self, names: impl IntoIterator<Item = String>) -> Planner<'ctx> {
		let mut shadowed: Vec<String> = self.shadowed_params.to_vec();
		shadowed.extend(names);
		Planner {
			ctx: Arc::clone(&self.ctx),
			function_registry: self.function_registry,
			txn: self.txn.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			version: self.version.clone(),
			plan_time_version: self.plan_time_version,
			auth: self.auth.clone(),
			perms_suppressed: self.perms_suppressed,
			cycle_guard: self.cycle_guard.clone(),
			ns_db_ids_cache: tokio::sync::OnceCell::new(),
			matches_context: self.matches_context.clone(),
			knn_context: self.knn_context.clone(),
			function_path: self.function_path.clone(),
			depth_sites: self.depth_sites.clone(),
			statement_nesting: self.statement_nesting,
			depth: self.depth,
			shadowed_params: Arc::from(shadowed),
			speculation: self.speculation.clone(),
			late_after_schema: self.late_after_schema,
			late_bound: self.late_bound,
			when_reached: self.when_reached,
			within: self.within,
			stored_clause: self.stored_clause,
			trusts_index_shapes: self.trusts_index_shapes,
			request_root: self.request_root,
			binds_rows: self.binds_rows,
			named_targets: Arc::clone(&self.named_targets),
			stages_inline: self.stages_inline,
			stages_linked: self.stages_linked,
		}
	}

	/// Let this planner speculate with the session's `options`, as the
	/// executor's planner for a statement it is about to run does, holding
	/// the catalog ids of the statement's speculative runs afresh; `None`
	/// stops it speculating.
	pub(crate) fn speculate_with(&mut self, options: Option<Arc<crate::dbs::Options>>) {
		self.speculation = options.map(|options| {
			Arc::new(crate::exec::vm::speculate::Speculating::new(options, &self.ctx))
		});
	}

	/// This planner planning a block whose statements after one that may
	/// change the catalog or the names it resolves in are planned late-bound
	/// ([`Self::late_binding`]).
	pub(crate) fn binding_late_after_schema(&self) -> Planner<'ctx> {
		let mut late = self.shadowing(std::iter::empty());
		late.late_after_schema = true;
		late
	}

	/// This planner planning a statement that runs after a change to the
	/// catalog its plan cannot know: binding reads nothing of the catalog, so
	/// every table, parameter and function the statement names is resolved
	/// when it runs.
	pub(crate) fn late_binding(&self) -> Planner<'ctx> {
		let mut late = self.shadowing(std::iter::empty());
		late.late_bound = true;
		// A late-bound statement reaches every catalog object it names as the
		// catalog stands when it runs.
		late.within = CatalogWithin::default();
		late
	}

	/// This planner planning a statement that reaches what code it runs may
	/// redefine as `within` says ([`CatalogWithin`]).
	pub(crate) fn reaching(mut self, within: CatalogWithin) -> Self {
		self.within = within;
		self
	}

	/// Whether this planner plans a statement whose catalog is loaded when it
	/// runs ([`Self::late_binding`]).
	pub(crate) fn is_late_bound(&self) -> bool {
		self.late_bound
	}

	/// This planner naming, for each `(name, table)` of `targets`, `table` as the table the write
	/// of a request's statement writes where its one target is the variable `name`: the variable
	/// holds a record of the table, or records of it alone, or the table itself, in every request
	/// the plan serves.
	pub(crate) fn naming_targets(
		mut self,
		targets: Vec<(String, surrealdb_strand::TableName)>,
	) -> Self {
		self.named_targets = Arc::from(targets);
		self
	}

	/// This planner binding the stages of every write as ops of the write's own, as a statement
	/// whose linked stages did not compile is planned again.
	pub(crate) fn inlining_stages(&self) -> Planner<'ctx> {
		let mut inline = self.shadowing(std::iter::empty());
		inline.stages_inline = true;
		inline
	}

	/// This planner planning a statement the statement cache keeps: its write may run the bundle
	/// of its table's stages the compile links, which serves every statement of its shape for as
	/// long as the table's definitions hold. A statement planned for one request alone binds its
	/// stages as its own ops, compiling no bundle it would not reuse.
	pub(crate) fn linking_stages(mut self) -> Planner<'ctx> {
		self.stages_linked = true;
		self
	}

	/// Whether a write of a request's statement this planner plans may run the bundle of its
	/// table's stages the compile links ([`Self::linking_stages`], [`Self::inlining_stages`]).
	pub(crate) fn links_stages(&self) -> bool {
		#[cfg(test)]
		if crate::exec::vm::units::INLINE_STAGES.with(std::cell::Cell::get) {
			return false;
		}
		self.stages_linked && !self.stages_inline
	}

	/// The tables [`Self::naming_targets`] named for the targets of a request's statement; none
	/// for a planner that plans no statement of a request.
	pub(crate) fn named_targets(&self) -> &[(String, surrealdb_strand::TableName)] {
		match self.request_root {
			true => &self.named_targets,
			false => &[],
		}
	}

	/// This planner, trusting the value shapes the indexes have recorded when
	/// `reads_only` says the program it plans cannot write.
	///
	/// A program is compiled in full before it runs, and is not compiled again
	/// while it runs. A program that writes may record a shape an index had
	/// not stored after its later statements were planned on the index lacking
	/// it, and those statements would read the index by an order and a range
	/// its entries no longer keep. A program that cannot write sees every
	/// shape its transaction recorded before it was planned, and records none.
	pub(crate) fn trusting_index_shapes(mut self, reads_only: bool) -> Self {
		self.trusts_index_shapes = reads_only;
		self
	}

	/// This planner planning the value of a top-level `LET`: the statements after the `LET`
	/// read its rows in the order they were bound, so no pipeline of it may hand its rows on in
	/// another order than the serial run's.
	pub(crate) fn binding_rows(mut self) -> Self {
		self.binds_rows = true;
		self
	}

	/// Whether this planner plans the value of a top-level `LET`
	/// ([`Self::binding_rows`]).
	pub(crate) fn binds_rows(&self) -> bool {
		self.binds_rows
	}

	/// Whether access-path selection may trust the value shapes the indexes
	/// have recorded ([`Self::trusting_index_shapes`]).
	pub(crate) fn trusts_index_shapes(&self) -> bool {
		self.trusts_index_shapes
	}

	/// This planner, planning a statement of a request: the program it plans runs in the
	/// request's own frame, which holds no `VERSION` the statement does not state itself.
	/// Only the executor's planner of a top-level statement says so. A planner of a unit that
	/// runs in a caller's frame (a function call, a table or field read, a closure, a served
	/// statement) never does, because that frame may read at a `VERSION` the unit's plan does
	/// not name.
	pub(crate) fn at_request_root(mut self) -> Self {
		self.request_root = true;
		self
	}

	/// Whether the program this planner plans is a statement of a request
	/// ([`Self::at_request_root`]).
	pub(crate) fn is_request_root(&self) -> bool {
		self.request_root
	}

	/// Whether this planner plans what follows a statement of a block that
	/// may change what the rest of it resolves against late-bound
	/// ([`Self::binding_late_after_schema`]).
	pub(crate) fn binds_late_after_schema(&self) -> bool {
		self.late_after_schema
	}

	/// This planner compiling for a frame that waives permission checks, for
	/// the same principal: binding resolves every source, its computed fields
	/// derived and no permission or field guard planted, as a frame that
	/// skips permissions reads it. What a call evaluated in a permission frame
	/// is planned with.
	pub(crate) fn waiving_permissions(&self) -> Planner<'ctx> {
		self.shadowing(std::iter::empty()).with_perms_suppressed(true)
	}

	/// Whether an enclosing `FOR`, closure, user-defined function or block
	/// `LET` binds `name`, so that a read of it is not the ambient context's
	/// value.
	pub(crate) fn param_is_shadowed(&self, name: &str) -> bool {
		self.shadowed_params.iter().any(|shadowed| shadowed == name)
	}

	/// [`Self::resolve_custom_access_mode`], with whether the body keeps row
	/// order: whether some body the call reaches calls a builtin or method the
	/// registry declares effects for that a second call answers differently
	/// or leaves behind ([`FunctionRegistry::keeps_row_order`]). Calls of such
	/// a function run one row after another. A body that may write keeps row
	/// order too, and so does one this planner cannot resolve.
	pub(crate) async fn resolve_custom_function(&self, name: &str) -> ResolvedCustomFunction {
		use crate::catalog::providers::DatabaseProvider;
		use crate::exec::AccessMode;

		const UNRESOLVED: ResolvedCustomFunction = ResolvedCustomFunction {
			access_mode: AccessMode::ReadWrite,
			keeps_row_order: true,
		};
		let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return UNRESOLVED;
		};
		let version = match self.plan_time_version {
			PlanTimeVersion::Current => None,
			PlanTimeVersion::At(stamp) => Some(stamp),
		};
		// A UDF call reaches its callee's body through the call graph; the call
		// itself contributes exactly that one edge.
		let mut facts = crate::expr::function_facts::FunctionFacts::default();
		facts.calls.insert(name.to_owned());

		let resolved = async {
			let db_def = txn.get_db_by_name(ns, db, version).await?.ok_or_else(|| {
				anyhow::anyhow!("database `{ns}/{db}` not found while resolving `fn::{name}`")
			})?;
			let registry = self.function_registry();
			let keeps_row_order = |facts: &crate::expr::function_facts::FunctionFacts| {
				facts.unseen_closures
					|| facts.builtins.iter().any(|name| registry.keeps_row_order(name))
					|| facts.methods.iter().any(|name| registry.method_keeps_row_order(name))
			};
			crate::fnc::mutability::resolve_mutability(
				txn,
				db_def.namespace_id,
				db_def.database_id,
				version,
				&facts,
				&keeps_row_order,
				Some(registry),
			)
			.await
		}
		.await;
		match resolved {
			Ok(r) if !r.possibly_writes() => ResolvedCustomFunction {
				access_mode: AccessMode::ReadOnly,
				keeps_row_order: r.keeps_row_order,
			},
			// A write is possible, or the snapshot could not be read: keep the
			// safe over-approximation.
			Ok(_) | Err(_) => UNRESOLVED,
		}
	}

	/// Resolve the `writeable` flag for a Surrealism module function from
	/// the cached runtime's exports manifest.
	///
	/// Returns `Ok(true)` — the safe over-approximation — when the planner
	/// lacks the transaction or namespace/database context the lookup needs.
	/// `ReadWrite` costs serialised evaluation, whereas a wrong `ReadOnly`
	/// would license a writing module onto the partitioned and parallel
	/// fan-out paths, which are sound only for readers. Neither answer moves
	/// the transaction: that is taken from the surface statement before
	/// planning, and a module call is never read-only there. In all other
	/// cases the module is loaded (blocking on first use if necessary) and the
	/// signature is read so the flag is always consistent with the module's
	/// declaration.
	#[cfg(feature = "surrealism")]
	async fn resolve_module_writeable(
		&self,
		module: &str,
		sub: Option<&str>,
	) -> Result<bool, Error> {
		use crate::catalog::Error as CatalogError;
		use crate::catalog::providers::DatabaseProvider;
		use crate::ctx::Context;
		use crate::err::EngineError;
		use crate::expr::module::ModuleExecutable;

		let Some(txn) = &self.txn else {
			return Ok(true);
		};
		let (Some(ns), Some(db)) = (&self.ns, &self.db) else {
			return Ok(true);
		};
		let Some(db_def) = txn
			.get_db_by_name(ns, db, None)
			.await
			.map_err(|e| EngineError::Internal(e.to_string()))?
		else {
			return Ok(true);
		};
		let mod_name = format!("mod::{module}");
		let val =
			match txn.get_db_module(db_def.namespace_id, db_def.database_id, &mod_name, None).await
			{
				Ok(v) => v,
				Err(e) => {
					if let Some(CatalogError::MdNotFound {
						..
					}) = e.downcast_ref::<CatalogError>()
					{
						// A missing module is a resolved absence, not an unknown: the
						// invocation repeats this lookup and fails on it, so the
						// call cannot reach a module body and cannot write.
						return Ok(false);
					}
					return Err(EngineError::Internal(e.to_string()).into());
				}
			};
		// A definition with no DIGEST has no archive this plan may load: one a
		// release before revision 3 stored, or one a speculative run of a
		// `DEFINE` in the same block wrote before its DIGEST was pinned. The
		// call resolves the definition again when it runs.
		if val.digest.is_none() {
			return Ok(true);
		}
		let executable: ModuleExecutable = val.executable.clone().into();
		// The planner's self.ctx does not carry a transaction (the executor
		// sets it after planning). Derive a context with the planner's txn
		// so that a cache-miss in get_surrealism_module can access the
		// bucket store without panicking.
		let mut plan_ctx = Context::new_child(&self.ctx);
		plan_ctx.set_transaction(Arc::clone(txn));
		let frozen = plan_ctx.freeze();
		let sig = crate::fnc::module::module_executable_signature(
			&executable,
			(&*val).into(),
			&frozen,
			&db_def.namespace_id,
			&db_def.database_id,
			sub,
		)
		.await
		.map_err(|e| EngineError::Internal(e.to_string()))?;
		Ok(sig.writeable)
	}

	#[cfg(not(feature = "surrealism"))]
	async fn resolve_module_writeable(
		&self,
		_module: &str,
		_sub: Option<&str>,
	) -> Result<bool, Error> {
		Ok(false)
	}

	/// Resolve the `writeable` flag for a Silo package function from
	/// the cached runtime's exports manifest.
	///
	/// A silo runtime is cached per namespace/database, so without the
	/// planner's transaction and those ids there is no key to resolve the
	/// signature under. Those cases return `Ok(true)`, the safe
	/// over-approximation: `ReadWrite` costs serialised evaluation, whereas a
	/// wrong `ReadOnly` would license a writing module onto the partitioned and
	/// parallel fan-out paths, which are sound only for readers. Neither answer
	/// moves the transaction: that is taken from the surface statement before
	/// planning, and a silo call is never read-only there.
	#[cfg(feature = "surrealism")]
	async fn resolve_silo_writeable(
		&self,
		org: &str,
		pkg: &str,
		major: u32,
		minor: u32,
		patch: u32,
		sub: Option<&str>,
	) -> Result<bool, Error> {
		use crate::catalog::providers::DatabaseProvider;
		use crate::ctx::Context;
		use crate::err::EngineError;
		use crate::expr::module::SiloExecutable;

		let Some(txn) = &self.txn else {
			return Ok(true);
		};
		let (Some(ns), Some(db)) = (&self.ns, &self.db) else {
			return Ok(true);
		};
		let Some(db_def) = txn
			.get_db_by_name(ns, db, None)
			.await
			.map_err(|e| EngineError::Internal(e.to_string()))?
		else {
			return Ok(true);
		};
		let executable = SiloExecutable {
			organisation: org.to_string(),
			package: pkg.to_string(),
			major,
			minor,
			patch,
		};
		// The archive is checked against the definition the call resolves to,
		// stored under the package's coordinates. Without one, the invocation
		// repeats this lookup and fails on it, as in resolve_module_writeable.
		let storage_name = format!("silo::{org}::{pkg}::<{major}.{minor}.{patch}>");
		let definition = match txn
			.get_db_module(db_def.namespace_id, db_def.database_id, &storage_name, None)
			.await
		{
			Ok(definition) => definition,
			Err(e) => {
				if let Some(crate::catalog::Error::MdNotFound {
					..
				}) = e.downcast_ref::<crate::catalog::Error>()
				{
					return Ok(false);
				}
				return Err(EngineError::Internal(e.to_string()).into());
			}
		};
		// As in resolve_module_writeable: no DIGEST, no archive to load here.
		if definition.digest.is_none() {
			return Ok(true);
		}
		// Same as resolve_module_writeable: derive a context with the
		// planner's transaction so signature resolution can access stores.
		let mut plan_ctx = Context::new_child(&self.ctx);
		plan_ctx.set_transaction(Arc::clone(txn));
		let frozen = plan_ctx.freeze();
		let sig = crate::fnc::module::silo_executable_signature(
			&executable,
			(&*definition).into(),
			&frozen,
			&db_def.namespace_id,
			&db_def.database_id,
			sub,
		)
		.await
		.map_err(|e| EngineError::Internal(e.to_string()))?;
		Ok(sig.writeable)
	}

	#[cfg(not(feature = "surrealism"))]
	async fn resolve_silo_writeable(
		&self,
		_org: &str,
		_pkg: &str,
		_major: u32,
		_minor: u32,
		_patch: u32,
		_sub: Option<&str>,
	) -> Result<bool, Error> {
		Ok(false)
	}

	// ========================================================================
	// Top-Level Planning
	// ========================================================================

	// ========================================================================
	// Scalar compilation
	// ========================================================================

	// ========================================================================
	// Record ID keys
	// ========================================================================
}

// ============================================================================
// Public API Wrappers
// ============================================================================

/// Plan an already-lowered logical plan into an executable operator.
///
/// The entry for a front end that emits a plan rather than a statement, so
/// there is no lowering step: the plan is compiled or the query fails. The
/// operator is a VM program ([`crate::exec::vm::plan_front_end`]).
///
/// `depth` seeds the expression-nesting counter, and is non-zero only when a
/// plan is compiled from inside a running query (`eval::gql`) so the count
/// continues toward `max_computation_depth` rather than resetting.
pub(crate) async fn plan_logical_inner(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_logical_scoped(op, ctx, registry, txn, auth, depth, false, &[]).await
}

/// [`plan_logical_inner`] for a plan compiled on behalf of a frame whose
/// permissions are `perms_suppressed`, with the parameters `shadowed` names
/// looked up when the program runs, never substituted into the plan.
#[expect(
	clippy::too_many_arguments,
	reason = "the arguments of plan_logical_inner, the frame's suppression and the names the plan leaves to the run"
)]
pub(crate) async fn plan_logical_scoped(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	perms_suppressed: bool,
	shadowed: &[String],
) -> Result<Arc<dyn ExecOperator>, Error> {
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, txn, ns, db)
		.with_depth(depth)
		.with_perms_suppressed(perms_suppressed)
		.trusting_index_shapes(op.read_only());
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	if !shadowed.is_empty() {
		planner = planner.shadowing(shadowed.iter().cloned());
	}
	// Boxed so the large planner state machine is allocated once and is not
	// copied through every frame between the statement loop and here.
	crate::exec::vm::out_of_line(|| crate::exec::vm::plan_front_end(&planner, op)).await
}

/// Bind a lowered statement for `txn` and plan it to run on the VM.
///
/// The executor lowers the surface statement; this binds the result for the
/// transaction and the caller's principal, then plans the bound plan as a VM
/// program. A statement the VM does not run fails with the reason; nothing
/// else executes it. It is also the runtime re-entry for a statement planned
/// from inside a running query — a user-defined function body — which seeds
/// `depth` from the node that called it so the count continues toward
/// `max_computation_depth`.
pub(crate) async fn plan_statement_inner(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	speculation: Option<Arc<crate::dbs::Options>>,
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_statement_on(op, ctx, registry, txn, auth, depth, speculation, false).await
}

/// [`plan_statement_inner`], with `perms_suppressed` set for a statement
/// planned on behalf of a frame whose permissions are suppressed.
#[expect(
	clippy::too_many_arguments,
	reason = "the arguments of plan_statement_inner and the frame's suppression"
)]
pub(crate) async fn plan_statement_on(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	speculation: Option<Arc<crate::dbs::Options>>,
	perms_suppressed: bool,
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_statement_with(
		op,
		ctx,
		registry,
		txn,
		auth,
		depth,
		speculation,
		perms_suppressed,
		&[],
		false,
		false,
		Vec::new(),
	)
	.await
}

/// [`plan_statement_inner`] for the value a top-level `LET` binds: the statements after the
/// `LET` read its rows in the order they were bound.
pub(crate) async fn plan_binding_value(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	speculation: Option<Arc<crate::dbs::Options>>,
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_statement_with(
		op,
		ctx,
		registry,
		txn,
		auth,
		0,
		speculation,
		false,
		&[],
		false,
		true,
		Vec::new(),
	)
	.await
}

/// [`plan_statement_on`] for a statement whose parameters `shadowed` names
/// are looked up when the program runs, never substituted into the plan: the
/// plan then serves every evaluation that binds them differently. `binds_rows` is
/// whether the statement is the value of a top-level `LET`
/// ([`Planner::binding_rows`]).
#[expect(
	clippy::too_many_arguments,
	reason = "the arguments of plan_statement_on, the names the plan leaves to the run and how its rows are read"
)]
pub(crate) async fn plan_statement_scoped(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	perms_suppressed: bool,
	shadowed: &[String],
	binds_rows: bool,
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_statement_with(
		op,
		ctx,
		registry,
		txn,
		auth,
		depth,
		None,
		perms_suppressed,
		shadowed,
		false,
		binds_rows,
		Vec::new(),
	)
	.await
}

/// [`plan_statement_scoped`] for a top-level statement of a request, planned
/// as the statement of a request it is ([`Planner::at_request_root`]): the
/// request's own frame runs it. `binds_rows` is as [`plan_statement_scoped`]
/// takes it, and `named_targets` names the table of each variable among
/// `shadowed` that the statement's write reads as its one target
/// ([`Planner::naming_targets`]).
#[expect(
	clippy::too_many_arguments,
	reason = "the arguments of plan_statement_scoped without the depth a request's statement starts at"
)]
pub(crate) async fn plan_request_statement_scoped(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	perms_suppressed: bool,
	shadowed: &[String],
	binds_rows: bool,
	named_targets: Vec<(String, surrealdb_strand::TableName)>,
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_statement_with(
		op,
		ctx,
		registry,
		txn,
		auth,
		0,
		None,
		perms_suppressed,
		shadowed,
		true,
		binds_rows,
		named_targets,
	)
	.await
}

/// Plan a top-level statement of a request when the request arrives, in the
/// transaction of the request's first planned statement, lowering it into the
/// request's program as `request` builds it.
///
/// `ctx` holds the session and the parameters the statements before it leave.
/// The executor plans here only a statement that runs in this transaction with
/// no statement before it that may have changed the catalog, so the plan
/// meets the catalog it was planned against.
#[expect(
	clippy::too_many_arguments,
	reason = "the statement, its planning context, how its rows are read and the request it joins"
)]
pub(crate) async fn plan_request_statement(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	speculation: Option<Arc<crate::dbs::Options>>,
	binds_rows: bool,
	request: &mut crate::exec::vm::RequestBuilder,
) -> Result<crate::exec::vm::RequestEntry, Error> {
	if let Some(error) = crate::exec::logical::invalid_let(&op) {
		return Err(error);
	}
	if let Some(why) = crate::exec::logical::unplannable_write(&op) {
		return Err(Error::Exec(ExecError::PlannerUnsupported {
			reason: why,
		}));
	}
	let planner = compile_phase!("bind", {
		let (ns, db) = session_ns_db(ctx);
		let mut planner = Planner::with_txn(ctx, registry, Arc::clone(&txn), ns, db)
			.trusting_index_shapes(op.read_only())
			.at_request_root();
		if binds_rows {
			planner = planner.binding_rows();
		}
		if let Some(auth) = auth {
			planner = planner.with_auth(auth);
		}
		planner.speculate_with(speculation);
		let within =
			crate::exec::vm::out_of_line(|| crate::exec::vm::catalog_within(&planner, &op, &[]))
				.await;
		planner.reaching(within)
	});
	let unbound = matches!(
		op,
		LogicalOp::Sequence(_) | LogicalOp::Foreach(_) | LogicalOp::Let(_) | LogicalOp::Eval(_)
	)
	.then(|| op.clone());
	let bound = compile_phase!(
		"bind",
		compile_recorded_catalog_reads!(txn, Box::pin(planner.bind(op)).await)
	);
	match bound {
		Ok(bound) => Box::pin(request.plan(&planner, &bound, unbound.as_ref())).await,
		Err(e) => Err(e),
	}
}

/// The refusal of the region-constraint rejection of `op`, the statement at `position` of its
/// request, bound in `txn` as [`plan_request_statement`] binds it; `None` when every region
/// admits what it holds, and when the statement does not bind against the catalog `txn` holds.
pub(crate) async fn request_statement_rejection(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	position: usize,
) -> Option<crate::exec::vm::payload::Refusal> {
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, txn, ns, db);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	let bound = Box::pin(planner.bind(op)).await.ok()?;
	Box::pin(crate::exec::vm::rejection(&planner, &bound, position)).await
}

/// The program of a request whose statements `request` compiled, taking
/// `steps`.
pub(crate) fn finish_request(
	request: crate::exec::vm::RequestBuilder,
	steps: Vec<crate::exec::vm::program::RequestStep>,
) -> Result<crate::exec::vm::request::CompiledRequest, Error> {
	request.finish(steps)
}

#[expect(
	clippy::too_many_arguments,
	reason = "the arguments of plan_statement_on and the names the plan leaves to the run"
)]
async fn plan_statement_with(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	speculation: Option<Arc<crate::dbs::Options>>,
	perms_suppressed: bool,
	shadowed: &[String],
	request_root: bool,
	binds_rows: bool,
	named_targets: Vec<(String, surrealdb_strand::TableName)>,
) -> Result<Arc<dyn ExecOperator>, Error> {
	// A `LET` no statement may hold is refused from the text written, before
	// binding folds any of it away and before a write is declined, so the
	// refusal never depends on what the planner can compile. See
	// `exec::logical::invalid_let`.
	if let Some(error) = crate::exec::logical::invalid_let(&op) {
		return Err(error);
	}
	// Before anything is bound or planned, so a write the planner cannot
	// compile is refused as `PlannerUnsupported` rather than after a sibling
	// branch has raised a planning error. Every caller reaches the VM through
	// here — the executor's statements and `LET` values, `eval::surql`, and a
	// user-defined function body. `plan_logical_inner` deliberately does not
	// have it: that path takes a front end's plan, where the specific error is
	// worth more than a uniform one. See `exec::logical::unplannable_write`.
	if let Some(why) = crate::exec::logical::unplannable_write(&op) {
		return Err(Error::Exec(ExecError::PlannerUnsupported {
			reason: why,
		}));
	}
	let _compiling = RequestTiming::begin();
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, txn, ns, db)
		.with_depth(depth)
		.with_perms_suppressed(perms_suppressed)
		.trusting_index_shapes(op.read_only());
	if binds_rows {
		planner = planner.binding_rows();
	}
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	if request_root {
		planner = planner.at_request_root().linking_stages();
	}
	if !shadowed.is_empty() {
		planner = planner.shadowing(shadowed.iter().cloned());
	}
	if !named_targets.is_empty() {
		planner = planner.naming_targets(named_targets);
	}
	planner.speculate_with(speculation);
	let within = compile_phase!(
		"bind",
		crate::exec::vm::out_of_line(|| crate::exec::vm::catalog_within(&planner, &op, shadowed))
			.await
	);
	let planner = planner.reaching(within);
	// A block the VM plans again late-bound after its schema statements is
	// planned from the statement as written.
	let unbound = matches!(
		op,
		LogicalOp::Sequence(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Let(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
	)
	.then(|| op.clone());
	// Boxed for the same reason as `plan_logical_inner`: the bind passes and the
	// planner together are the bulk of a statement's future, and every enclosing
	// async frame would otherwise carry and copy them.
	let bound = compile_phase!("bind", crate::exec::vm::out_of_line(|| planner.bind(op)).await?);
	crate::exec::vm::out_of_line(|| {
		crate::exec::vm::plan_checked(&planner, &bound, unbound.as_ref())
	})
	.await
}

/// What a planner finds of the function a call names
/// ([`Planner::function_snapshot`]).
#[derive(Clone, Debug)]
pub(crate) enum FunctionLookup {
	/// The function, defined and allowed.
	Found(Arc<crate::kvs::cache::ds::FunctionSnapshot>),
	/// A function the call cannot enter: undefined, refused by the
	/// capabilities, or called with no namespace or database selected.
	/// Entering it raises the error the catalog, the capability or the missing
	/// selection names.
	Absent,
	/// The planner cannot say: it has no transaction or database, plans
	/// under a `VERSION`, or could not read the definition.
	Unavailable,
}

/// Why [`Planner::prepare_snapshot`] bound no body.
pub(crate) enum PrepareError {
	/// The planner has no transaction or database.
	NoDatabase,
	/// The definition's `AUTH LIMIT` does not parse.
	AuthLimit,
	/// The body has no logical form, or binding it raised this error.
	Bind(anyhow::Error),
}

/// A function body bound for a call and the planner that compiles it
/// ([`Planner::prepare_snapshot`]).
pub(crate) struct PreparedBody<'ctx> {
	/// The planner the body is compiled with: the call's principal capped by
	/// the definition's `AUTH LIMIT`, one re-entry deeper, with the arguments
	/// shadowed.
	pub(crate) planner: Planner<'ctx>,
	/// The body, bound by [`Self::planner`]; as written, when
	/// [`Self::specialised`].
	pub(crate) bound: LogicalOp,
}

/// Bind `op`, a statement nested in another, with `planner`, refusing the
/// forms `plan_statement_inner` declines.
pub(crate) async fn bind_statement(
	planner: &Planner<'_>,
	op: LogicalOp,
) -> Result<LogicalOp, Error> {
	// As `plan_statement_inner` refuses and declines them, and for the same
	// reasons.
	if let Some(error) = crate::exec::logical::invalid_let(&op) {
		return Err(error);
	}
	if let Some(why) = crate::exec::logical::unplannable_write(&op) {
		return Err(Error::Exec(ExecError::PlannerUnsupported {
			reason: why,
		}));
	}
	// A planner that plans what follows a block's schema statements
	// late-bound resolves the catalog and the session's names those
	// statements leave when each runs; any other binds the statement against
	// the catalog it finds, so a schema statement nested in it is one whose
	// effect the compilation cannot see.
	if !planner.binds_late_after_schema()
		&& let Some(why) = crate::exec::logical::unplannable_context_change(&op)
	{
		return Err(Error::Exec(ExecError::PlannerUnsupported {
			reason: why,
		}));
	}
	crate::exec::vm::out_of_line(|| planner.bind(op)).await
}

/// Compiles `expr` to a [`Clause`] for code the compile cache keeps and serves to every
/// principal of the frame's class: the session's own values are read when it
/// runs, never folded ([`Planner::reading_session_values`]). It is compiled
/// over `exec`'s context, registry and transaction, with permission
/// enforcement suppressed where `exec`'s frame suppresses it
/// ([`RootContext::perms_suppressed`](crate::exec::context::RootContext)): a
/// frame that reads with permissions off compiles no permission filter or
/// field guard over what it reads. The names `bound`, which the evaluation
/// binds around the code, are read when it runs.
pub(crate) async fn compile_unit_expr(
	expr: LogicalExpr,
	exec: &crate::exec::ExecutionContext,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	bound: &[String],
) -> Result<Clause, Error> {
	let compiled = expr_planner(exec.ctx(), exec.function_registry(), exec.try_txn(), auth, depth)
		.with_perms_suppressed(exec.root().perms_suppressed)
		.reading_session_values()
		.shadowing(bound.iter().cloned())
		.scalar_metadata(expr.clone())
		.await?;
	Ok(Clause::of(expr, compiled))
}

/// The planner [`compile_unit_expr`] compiles with.
fn expr_planner<'ctx>(
	ctx: &'ctx FrozenContext,
	registry: &'ctx FunctionRegistry,
	txn: Option<Arc<crate::kvs::Transaction>>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
) -> Planner<'ctx> {
	let mut planner = match txn {
		Some(txn) => {
			let (ns, db) = session_ns_db(ctx);
			Planner::with_txn(ctx, registry, txn, ns, db)
		}
		None => Planner::new(ctx, registry),
	}
	.with_depth(depth);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	planner
}

/// The namespace and database the session is using, when it named them.
fn session_ns_db(ctx: &FrozenContext) -> (Option<String>, Option<String>) {
	let read = |key: &str| {
		ctx.value("session").and_then(|v| v.as_object()).and_then(|o| o.get(key)).and_then(|v| {
			if let Value::String(s) = v {
				Some(s.as_str().to_owned())
			} else {
				None
			}
		})
	};
	(read("ns"), read("db"))
}

/// The value a leaf literal denotes.
fn literal_value(lit: LogicalLiteral) -> Value {
	use crate::val::Number;
	match lit {
		LogicalLiteral::None => Value::None,
		LogicalLiteral::Null => Value::Null,
		LogicalLiteral::UnboundedRange => Value::Range(Box::new(crate::val::Range::unbounded())),
		LogicalLiteral::Bool(x) => Value::Bool(x),
		LogicalLiteral::Float(x) => Value::Number(Number::Float(x)),
		LogicalLiteral::F16(x) => Value::Number(Number::F16(x)),
		LogicalLiteral::F32(x) => Value::Number(Number::F32(x)),
		LogicalLiteral::Integer(x) => Value::Number(Number::Int(x)),
		LogicalLiteral::I8(x) => Value::Number(Number::I8(x)),
		LogicalLiteral::U8(x) => Value::Number(Number::U8(x)),
		LogicalLiteral::I16(x) => Value::Number(Number::I16(x)),
		LogicalLiteral::U16(x) => Value::Number(Number::U16(x)),
		LogicalLiteral::I32(x) => Value::Number(Number::I32(x)),
		LogicalLiteral::U32(x) => Value::Number(Number::U32(x)),
		LogicalLiteral::U64(x) => Value::Number(Number::U64(x)),
		LogicalLiteral::Decimal(x) => Value::Number(Number::Decimal(x)),
		LogicalLiteral::BigDecimal(x) => Value::Number(Number::BigDecimal(x)),
		LogicalLiteral::String(x) => Value::String(x),
		LogicalLiteral::Bytes(x) => Value::Bytes(x),
		LogicalLiteral::Regex(x) => Value::Regex(x),
		LogicalLiteral::Duration(x) => Value::Duration(x),
		LogicalLiteral::Datetime(x) => Value::Datetime(x),
		LogicalLiteral::Date(x) => Value::Date(x),
		LogicalLiteral::Time(x) => Value::Time(x),
		LogicalLiteral::Timestamp(x) => Value::Timestamp(x),
		LogicalLiteral::Interval(x) => Value::Interval(x),
		LogicalLiteral::Uuid(x) => Value::Uuid(x),
		LogicalLiteral::Geometry(x) => Value::Geometry(x),
		LogicalLiteral::File(x) => Value::File(x),
	}
}

/// The value a scalar denotes without evaluation, when it denotes one.
///
/// A leaf literal is one; a collection is one when every element is; a record
/// id is one when its key is a plain number, string or uuid — a generated key
/// is allocated during evaluation, and the compound keys hold expressions.
/// Everything else is `None`.
///
/// An object is built through a sorted map, exactly as evaluation builds it,
/// so key ordering and duplicate-key resolution cannot drift apart.
pub(crate) fn static_value(expr: &LogicalExpr) -> Option<Value> {
	match expr {
		LogicalExpr::Literal(lit) => Some(literal_value(lit.clone())),
		LogicalExpr::Array(items) => {
			let mut values = Vec::with_capacity(items.len());
			for item in items {
				values.push(static_value(item)?);
			}
			Some(Value::Array(crate::val::Array(values)))
		}
		LogicalExpr::Set(items) => {
			let mut set = crate::val::Set::new();
			for item in items {
				set.insert(static_value(item)?);
			}
			Some(Value::Set(set))
		}
		LogicalExpr::Object(entries) => {
			let mut map = std::collections::BTreeMap::new();
			for (key, value) in entries {
				map.insert(key.clone(), static_value(value)?);
			}
			Some(Value::Object(crate::val::Object::from(map)))
		}
		LogicalExpr::RecordId(rid) => {
			let key = match &rid.key {
				LogicalRecordIdKey::Number(x) => crate::val::RecordIdKey::Number(*x),
				LogicalRecordIdKey::String(x) => crate::val::RecordIdKey::String(x.clone()),
				LogicalRecordIdKey::Uuid(x) => crate::val::RecordIdKey::Uuid(*x),
				LogicalRecordIdKey::Array(_)
				| LogicalRecordIdKey::Object(_)
				| LogicalRecordIdKey::Generate(_)
				| LogicalRecordIdKey::Range(_) => return None,
			};
			Some(Value::RecordId(crate::val::RecordId::new(rid.table.clone(), key)))
		}
		LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

// ============================================================================
// Tests
// ============================================================================
