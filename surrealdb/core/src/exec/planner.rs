//! Query planner for the engine.
//!
//! The planner compiles a bound logical plan ([`LogicalOp`]) into a tree of
//! physical operators (`Arc<dyn ExecOperator>`), and a logical scalar
//! ([`LogicalExpr`]) into a physical expression (`Arc<dyn PhysicalExpr>`). Its
//! input is the logical plan and nothing else: the surface statement is lowered
//! by `surrealdb_expr::expr::logical::lower`, bound by
//! [`crate::exec::logical::bind`], and only then compiled here. No
//! expression-layer value (`crate::expr::Expr`) enters the planner; a GQL
//! `MATCH` plan's scalars are `LogicalExpr` and compile through
//! [`Planner::physical_expr`] like every other scalar.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────┐  lower  ┌───────────┐  bind  ┌───────────┐  fold  ┌────────────────┐
//! │  sql::Expr │ ──────► │ LogicalOp │ ─────► │ LogicalOp │ ─────► │  ExecOperator  │
//! │ (surface)  │         │ (unbound) │        │  (bound)  │        │      tree      │
//! └────────────┘         └───────────┘        └───────────┘        └────────────────┘
//! ```
//!
//! # Entry points
//!
//! - [`Planner::plan_logical`] compiles an operator tree. A GQL `MATCH` plan is compiled whole by
//!   `plan_match`; every other tree is a bottom-up fold over its operators (`fold.rs`).
//! - [`Planner::physical_expr`] compiles a scalar. A [`LogicalExpr::Subquery`] is bound and planned
//!   as a bytecode program ([`crate::exec::vm::plan_checked`]) wrapped in a [`ScalarSubquery`], or,
//!   for a planner that holds no transaction, which nothing evaluates ([`UnplannedSubquery`]).
//! - [`plan_statement_inner`] binds and compiles a lowered statement for the executor.
//! - [`plan_logical_inner`] compiles a plan a front end produced, with no statement behind it.
//! - [`compile_expr`] compiles one scalar from inside a running query — a closure body, a dynamic
//!   source, a `DEFINE PARAM` value.
//!
//! # SELECT pipeline
//!
//! A `SELECT` lowers to the operator order below, read bottom-up, and the fold
//! compiles each operator from its already-compiled input:
//!
//! ```text
//! Source / Values / UnionAll (FROM)
//!     │
//!     ▼
//! Filter (WHERE)
//!     │
//!     ▼
//! Split (SPLIT ON)
//!     │
//!     ▼
//! Aggregate (GROUP BY)
//!     │
//!     ▼
//! Sort (ORDER BY)
//!     │
//!     ▼
//! Limit (LIMIT / START)
//!     │
//!     ▼
//! Project (SELECT fields, OMIT)
//!     │
//!     ▼
//! Fetch (FETCH)
//!     │
//!     ▼
//! Timeout / VersionScope / Only / Explain
//! ```

mod aggregate;
mod cycle_guard;
pub(crate) mod fold;
mod path;
pub(crate) use path::contains_repeat_recurse;
// GQL `MATCH` planning (`plan_match`).
pub(crate) mod match_backend;
pub(crate) mod match_plan;
pub(crate) mod row_scope;
mod select;
mod source;
pub(crate) use source::{TableDefs, TargetVertexPlan};
pub(crate) mod util;
pub(crate) mod write;

use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use aggregate::{AggregationAnalysis, analyse_aggregation};
pub(crate) use cycle_guard::{CycleGuard, EventsEntry};
pub(crate) use fold::AccessOffer;
pub(crate) use select::{
	CountDecision, bitmap_plan_to_node, check_locked_read, decide_union_merge, output_field_path,
};
use surrealdb_strand::Strand;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::record_id::PhysicalRecordIdKey;
use crate::exec::physical_expr::{
	ArrayLiteral, BinaryOp, BuiltinFunctionExec, ClosureCallExec, ClosureExec, ControlFlowExpr,
	ControlFlowKind, FunctionCall, IfElseExpr, JsFunctionExec, Literal as PhysicalLiteral,
	MockExpr, ModelFunctionExec, ObjectLiteral, Param, PostfixOp, ProjectionFunctionExec,
	RecordIdExpr, ScalarSubquery, SetFieldExpr, SetLiteral, SiloModuleExec, SurrealismModuleExec,
	UnaryOp, UnplannedSubquery, UserDefinedFunctionExec,
};
use crate::exec::planner::util::try_value_to_logical_expr;
use crate::exec::{Error as ExecError, ExecOperator, PhysicalExpr};
use crate::expr::Function;
use crate::expr::logical::{
	LogicalCall, LogicalClosure, LogicalExpr, LogicalIf, LogicalLiteral, LogicalOp,
	LogicalPostfixOperator, LogicalRecordIdKey,
};
use crate::expr::operator::{BinaryOperator, PrefixOperator};
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
	/// A `VERSION` only the runtime can evaluate.
	Unknown,
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
	/// A user-defined function body is planned against this context
	/// ([`Self::body_scope`]), which is isolated in a privileged frame
	/// ([`Self::privileged`]); a body binds only the session's own values
	/// ([`UNIT_RUNTIME_PARAMS`]) at plan time, and reads every other name when
	/// it runs.
	ctx: FrozenContext,
	/// Cached reference to the function registry for aggregate/projection detection.
	function_registry: &'ctx FunctionRegistry,
	/// Optional transaction for plan-time index resolution.
	///
	/// When present, the planner can resolve table definitions and indexes
	/// at plan time, enabling concrete scan operators (IndexScan, TableScan)
	/// instead of the generic Scan operator. This in turn enables
	/// optimizations like sort elimination via [`OutputOrdering`].
	///
	/// When `None`, the planner creates Scan operators that resolve their
	/// access path at execution time.
	pub(crate) txn: Option<Arc<crate::kvs::Transaction>>,
	/// Optional namespace name for plan-time catalog lookups.
	pub(crate) ns: Option<String>,
	/// Optional database name for plan-time catalog lookups.
	pub(crate) db: Option<String>,
	/// Optional VERSION expression from the enclosing SELECT statement.
	///
	/// Propagated to `GraphEdgeScan` operators created during idiom
	/// conversion so that graph edge traversals respect the VERSION clause.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
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
	/// Per-SELECT MATCHES registration scope, set by the fold on the inner
	/// planner it spawns for a `SELECT`. Names which MATCHES expressions of the
	/// statement's WHERE condition are registered against a table's full-text
	/// index, so [`MatchesOp`](crate::exec::physical_expr::MatchesOp) can decide
	/// per row between evaluating, erroring and answering `false`. `None`
	/// outside SELECT planning (RETURN, LET, the literal record-id fast path,
	/// …), where no index is registered and MATCHES evaluates to `false`.
	pub(crate) matches_scope: Option<Arc<crate::exec::physical_expr::MatchesScope>>,
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
	/// Parameter names an enclosing `FOR`, closure or user-defined function
	/// binds around the expression being compiled.
	///
	/// Those constructs bind a user-chosen name unchecked, so a read of one
	/// inside their body is the value they bind and not the ambient planning
	/// context's. The planner folds a parameter only where the name is absent
	/// here — today that is `$session` alone
	/// ([`Self::constant_root_value`](super::planner::path)).
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
	/// Whether what this planner compiles is kept by the compile cache and
	/// served to later statements: a unit ([`Self::unit_root`],
	/// [`Self::reading_session_values`]). Such code outlives the statement, so
	/// a `time::now()` in a condition is evaluated when it runs rather than
	/// folded to the time of planning ([`Self::folds_clock`]).
	kept: bool,
	/// How the statement being planned reaches what code it runs may redefine
	/// while it runs ([`CatalogWithin`]).
	within: CatalogWithin,
	/// Whether what this planner plans is a stored clause — an event's `WHEN`
	/// or `THEN`, a field's `VALUE`, `ASSERT`, `DEFAULT` or `COMPUTED` — or a
	/// statement nested in one. The operator tree evaluates such a clause an
	/// expression at a time, so a `LET` written where its value is read
	/// (`RETURN (LET …)`, `LET $a = (LET …)`) raises when it is evaluated
	/// rather than binding ([`Self::in_stored_clause`]).
	stored_clause: bool,
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
struct StatementNesting {
	/// The `IF` branches it is in.
	ifs: u32,
	/// Whether it is in a block, a loop, an expression or `FROM`.
	nested: bool,
}

/// What a planner plans with, as [`Planner::frame_key`] answers it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PlannerFrameKey {
	ns: Option<String>,
	db: Option<String>,
	auth: Option<crate::iam::Auth>,
	perms_suppressed: bool,
	plan_time_version: PlanTimeVersion,
	depth: u32,
	/// The shadowed names, sorted and each once.
	shadowed: Vec<String>,
	/// The function bodies under compilation, outermost first.
	functions: Vec<(String, u64, bool)>,
	/// The identities of the transaction, the `VERSION` expression, the
	/// `MATCHES` scope and context, the KNN context and the speculation.
	shared: [usize; 6],
	late_bound: bool,
	when_reached: bool,
	within: CatalogWithin,
	stored_clause: bool,
}

impl PlannerFrameKey {
	/// Whether code planned with `other` reads and checks as code planned
	/// with this does: the same database, principal, permission contract,
	/// stamp, names bound when it runs and shared scopes, at whatever re-entry
	/// depth and under whatever function bodies.
	pub(crate) fn plans_alike(&self, other: &Self) -> bool {
		self.ns == other.ns
			&& self.db == other.db
			&& self.auth == other.auth
			&& self.perms_suppressed == other.perms_suppressed
			&& self.plan_time_version == other.plan_time_version
			&& self.shadowed == other.shadowed
			&& self.shared == other.shared
			&& self.late_bound == other.late_bound
			&& self.stored_clause == other.stored_clause
	}
}

/// One function body under compilation on a planner's [`Planner::function_path`].
#[derive(Clone, Debug)]
pub(crate) struct FunctionFrame {
	/// The function's name, without its `fn::` prefix.
	name: String,
	/// The content hash of the definition being compiled
	/// ([`FunctionSnapshot`](crate::kvs::cache::ds::FunctionSnapshot)).
	pub(crate) definition: u64,
	/// The definition being compiled; see [`PlannedBody::compiled_from`].
	pub(crate) compiled_from: Arc<crate::catalog::FunctionDefinition>,
	/// Where the compiled plan is published for the recursive edges inside it.
	body: Arc<crate::exec::physical_expr::RecursiveBody>,
	/// Whether the body is bound for a principal whose reads are checked; see
	/// [`PlannedBody::checked`](crate::exec::physical_expr::PlannedBody).
	pub(crate) checked: bool,
}

impl<'ctx> Planner<'ctx> {
	/// Whether the named table is a lightweight relation.
	///
	/// A lightweight relation keeps no records: its edges live as the two
	/// vertex-side adjacency keys of their endpoints, so it has no record
	/// range, and a caller that would open one asks here first.
	pub(crate) async fn table_is_lightweight(
		&self,
		table: &surrealdb_strand::TableName,
	) -> Result<bool, crate::err::Error> {
		use crate::catalog::providers::TableProvider;
		let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return Ok(false);
		};
		let def = txn
			.get_tb_by_name(ns, db, table, None)
			.await
			.map_err(|e| crate::err::EngineError::Internal(e.to_string()))?;
		Ok(def.is_some_and(|def| {
			crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some()
		}))
	}

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
			matches_scope: None,
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
			kept: false,
			within: CatalogWithin::default(),
			stored_clause: false,
		}
	}

	/// Create a new planner with the given context and transaction.
	///
	/// When a transaction is provided, the planner can resolve table
	/// definitions and indexes at plan time, producing concrete scan
	/// operators (IndexScan, TableScan, etc.) and enabling optimizations
	/// like sort elimination.
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
			matches_scope: None,
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
			kept: false,
			within: CatalogWithin::default(),
			stored_clause: false,
		}
	}

	#[cfg(test)]
	/// Set the VERSION expression for propagation to graph edge scans.
	pub fn with_version(mut self, version: Option<Arc<dyn PhysicalExpr>>) -> Self {
		self.version = version;
		self
	}

	#[cfg(test)]
	/// Set the version the planner's catalog reads are made at; see
	/// [`PlanTimeVersion`].
	pub(crate) fn with_plan_time_version(mut self, version: PlanTimeVersion) -> Self {
		self.plan_time_version = version;
		self
	}

	/// This planner for a statement whose `VERSION` clause is `version`: every
	/// catalog read it makes, and every plan derived from it, is made as of the
	/// stamp the clause denotes ([`Self::plan_time_version_of`]).
	pub(crate) async fn under_version(&self, version: &LogicalExpr) -> Planner<'ctx> {
		let mut scoped = self.shadowing([]);
		scoped.plan_time_version = self.plan_time_version_of(Some(version)).await;
		scoped
	}

	/// Whether what this planner plans reads at a `VERSION`: the statement
	/// names one itself (`own`), or it is planned beneath a statement that
	/// does.
	pub(crate) fn reads_versioned(&self, own: bool) -> bool {
		own || !matches!(self.plan_time_version, PlanTimeVersion::Current)
	}

	/// The version a pipeline's sources are read at, for the catalog reads
	/// planning makes, from the pipeline's `VERSION` expression: the
	/// transaction's current state for none (`VERSION NONE` is none), the
	/// stamp of a datetime literal or of a parameter the context binds to a
	/// datetime — the two forms the analyzer resolves a source under — and
	/// [`PlanTimeVersion::Unknown`] for any other expression, which only the
	/// runtime evaluates. The inherited version stands where the pipeline
	/// names none.
	pub(crate) async fn plan_time_version_of(
		&self,
		version: Option<&LogicalExpr>,
	) -> PlanTimeVersion {
		let stamps = || self.txn.as_ref().map(|txn| txn.timestamp_impl());
		let stamp_of = |datetime: crate::val::Datetime| match stamps() {
			Some(stamps) => match datetime.to_version_stamp(stamps.as_ref()) {
				Ok(stamp) => PlanTimeVersion::At(stamp),
				Err(_) => PlanTimeVersion::Unknown,
			},
			None => PlanTimeVersion::Unknown,
		};
		match version {
			None | Some(LogicalExpr::Literal(LogicalLiteral::None)) => self.plan_time_version,
			Some(LogicalExpr::Literal(LogicalLiteral::Datetime(datetime))) => stamp_of(*datetime),
			Some(LogicalExpr::Param(param)) => {
				let value = util::resolve_param_value(
					param.as_str(),
					&self.ctx,
					self.txn.as_ref(),
					self.definition_ids().await,
					&self.row_scoped(),
				)
				.await;
				match value {
					Some(crate::val::Value::Datetime(datetime)) => stamp_of(datetime),
					Some(_) | None => PlanTimeVersion::Unknown,
				}
			}
			Some(_) => PlanTimeVersion::Unknown,
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

	#[cfg(test)]
	/// Get a clone of the cycle guard (cheap `Arc` bump). Pass to
	/// `with_cycle_guard` on a child planner to share the same set of
	/// in-progress tables.
	#[inline]
	pub(crate) fn cycle_guard(&self) -> CycleGuard {
		self.cycle_guard.clone()
	}

	#[cfg(test)]
	/// Set the per-SELECT MATCHES registration scope (see the field docs).
	/// Called by the fold on the inner planner it spawns for a `SELECT`, once
	/// the WHERE condition has been bound.
	#[inline]
	pub(crate) fn set_matches_scope(
		&mut self,
		scope: Arc<crate::exec::physical_expr::MatchesScope>,
	) {
		self.matches_scope = Some(scope);
	}

	/// Whether a `KNN` operator is in scope for the expressions planned here:
	/// without one, a `KNN` operator evaluated as an expression is `false`
	/// for every row (see the [`knn_context`](Self::knn_context) field docs).
	pub(crate) fn has_knn_scope(&self) -> bool {
		self.knn_context.is_some()
	}

	#[cfg(test)]
	/// Establish the MATCHES and KNN planning scopes for the SELECT about to be
	/// planned here (see the [`matches_context`](Self::matches_context) and
	/// [`knn_context`](Self::knn_context) field docs).
	///
	/// `own_matches` is what this SELECT's own WHERE declares and `has_knn`
	/// whether it declares a KNN operator; either one present replaces the
	/// enclosing scope. What the SELECT does not declare is inherited from
	/// `parent`, so an index function in a nested SELECT still resolves against
	/// the MATCHES or KNN operator that introduced it.
	pub(crate) fn set_planning_scopes(
		&mut self,
		own_matches: Option<Arc<crate::exec::function::MatchesContext>>,
		has_knn: bool,
		parent: &Planner<'_>,
	) {
		self.matches_context = own_matches.or_else(|| parent.matches_context.clone());
		self.knn_context = if has_knn {
			Some(Arc::new(crate::exec::function::KnnContext::new()))
		} else {
			parent.knn_context.clone()
		};
	}

	/// Seed the re-entry nesting depth (see the `depth` field).
	///
	/// Used when a runtime re-entry (`eval` re-planning its query string, a
	/// user-defined-function body) starts a fresh planner but must continue
	/// counting from the depth of the boundary that triggered it (`parent + 1`),
	/// so the limit reflects the total nesting across the boundary rather than
	/// resetting to 0.
	#[must_use]
	/// This planner, planning stored code of which `sites` is the measure
	/// ([`Self::depth_sites`]).
	pub(crate) fn with_depth_sites(
		mut self,
		sites: Arc<crate::exec::vm::depth::ExprDepth>,
	) -> Self {
		self.depth_sites = Some(sites);
		self
	}

	/// What the stored code being planned spends of the computation-depth
	/// budget where it is evaluated one expression at a time: the event clause
	/// this planner plans, or else the body on top of its function path.
	/// `None` for a statement the session runs.
	pub(crate) fn depth_sites(&self) -> Option<Arc<crate::exec::vm::depth::ExprDepth>> {
		if let Some(sites) = &self.depth_sites {
			return Some(Arc::clone(sites));
		}
		self.function_path.last().map(|frame| {
			Arc::new(crate::exec::vm::depth::ExprDepth::of_body(&frame.compiled_from.block))
		})
	}

	/// A planner for a statement nested in the one this plans, inside `ifs`
	/// more `IF` branches and, where `nested`, inside a block or loop.
	pub(crate) fn nested_statement(&self, ifs: u32, nested: bool) -> Planner<'ctx> {
		let mut planner = self.shadowing(std::iter::empty());
		planner.statement_nesting = StatementNesting {
			ifs: self.statement_nesting.ifs + ifs,
			nested: self.statement_nesting.nested || nested,
		};
		planner
	}

	/// The depth a write planned here resumes the computation-depth count
	/// from ([`Self::statement_nesting`]).
	pub(crate) fn statement_nesting(&self) -> u32 {
		2 * self.statement_nesting.ifs + u32::from(self.statement_nesting.nested)
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
	/// `max_computation_depth`. Checked at the entry of [`Planner::physical_expr`]
	/// and [`Planner::plan_logical`] — the two points at which a runtime re-entry
	/// begins compiling a fresh query — so a runaway `eval`/UDF chain stops here
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

	/// The context the planner compiles against.
	#[inline]
	pub(crate) fn frozen_ctx(&self) -> &FrozenContext {
		&self.ctx
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
			versioned: self.version.is_some()
				|| !matches!(self.plan_time_version, PlanTimeVersion::Current),
			folds_clock: self.folds_clock(),
			inlines_definitions: !self.within.calls_current,
			binds_nested: !self.within.nested_late,
		};
		// Binding reads the catalog, so it runs unsealed wherever it is
		// reached from ([`crate::kvs::catalog_reads::sealed`]).
		crate::kvs::catalog_reads::sealed(false, crate::exec::logical::bind(op, &bind_ctx)).await
	}

	/// The body of `fn::name`, bound and compiled once for this plan, to be
	/// evaluated by every call the plan makes for as long as the stored
	/// definition is the one it was compiled from.
	///
	/// The body is bound as the runtime would bind it for a call in the
	/// caller's frame — for the planner's principal capped by the function's
	/// `AUTH LIMIT`, with the arguments as names bound per call — and planned
	/// in its own scope one re-entry deeper than the call. The runtime keeps
	/// binding per call where this cannot stand in for it: a call evaluated in
	/// a permission frame, which binds without a principal; a `VERSION`ed
	/// read, whose definition is read at the stamp; a definition that has
	/// changed since, told by its content hash
	/// ([`UserDefinedFunctionExec`]).
	///
	/// A call to a function whose body this planner is already compiling is a
	/// recursive edge: it is handed the plan of that body, published once the
	/// body has compiled, so the recursion evaluates one plan rather than
	/// binding and planning the body again at every level. The runtime's
	/// depth budget bounds the recursion.
	///
	/// `None`, leaving the call runtime-bound as it is without a plan, when
	/// the planner has no transaction or database, is planning under a
	/// `VERSION`, or when the function is undefined, disallowed, has no
	/// logical form, or does not compile; the runtime then reports whichever
	/// of those it meets.
	///
	/// A name the body reads that is not an argument or a session value is
	/// left a runtime lookup rather than resolved at plan time, so no caller's
	/// binding and no `DEFINE PARAM` value is baked into the body. A body that
	/// reads an
	/// argument in a predicate or a source is left to the runtime as well,
	/// which binds the argument as the call's value and so gives the access
	/// path a literal to answer; a plan compiled here would carry a name
	/// there, which no index answers.
	async fn plan_function_body(
		&self,
		name: &str,
	) -> Option<crate::exec::physical_expr::PlannedBody> {
		use crate::exec::physical_expr::{BodyPlan, PlannedBody};

		if self.version.is_some() {
			return None;
		}
		if let Some(frame) = self.function_path.iter().find(|f| f.name == name) {
			return Some(PlannedBody {
				definition: frame.definition,
				compiled_from: Arc::clone(&frame.compiled_from),
				plan: BodyPlan::Recursive(Arc::clone(&frame.body)),
				checked: frame.checked,
			});
		}
		let prepared = crate::exec::vm::out_of_line(|| self.prepare_function_body(name)).await?;
		let compiled = match prepared.planner.check_depth() {
			Ok(()) => {
				crate::exec::vm::out_of_line(|| {
					crate::exec::vm::plan_checked(&prepared.planner, &prepared.bound, None)
				})
				.await
			}
			Err(e) => Err(e),
		};
		match compiled {
			Ok(plan) => {
				prepared.frame.body.resolve(&plan);
				Some(PlannedBody {
					definition: prepared.frame.definition,
					compiled_from: prepared.frame.compiled_from,
					plan: BodyPlan::Compiled(plan),
					checked: prepared.frame.checked,
				})
			}
			Err(e) => {
				tracing::debug!("fn::{name} body is left for the runtime to plan: {e:#}");
				None
			}
		}
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
			&& self.matches_scope.is_none()
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
	/// clock: a `time::now()` in a unit's condition is not folded
	/// ([`Self::folds_clock`]).
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
		planner.kept = true;
		planner.version.clone_from(&self.version);
		planner.plan_time_version = self.plan_time_version;
		planner
	}

	/// This planner with the session's own values ([`UNIT_RUNTIME_PARAMS`])
	/// shadowed, so what it compiles reads them when it runs and never folds
	/// the planning context's. Every planner whose compilation the compile
	/// cache keeps and serves to other principals of the frame's class plans
	/// with them shadowed: the class is not the principal, and a folded
	/// `$auth` would answer one principal's rows to another. The code it
	/// compiles outlives the statement, so it reads the clock when it runs
	/// too ([`Self::folds_clock`]).
	pub(crate) fn reading_session_values(&self) -> Planner<'ctx> {
		let missing: Vec<String> = UNIT_RUNTIME_PARAMS
			.iter()
			.filter(|name| !self.param_is_shadowed(name))
			.map(|name| (*name).to_owned())
			.collect();
		let mut planner = self.shadowing(missing);
		planner.kept = true;
		planner
	}

	/// Whether a `time::now()` in a condition this planner compiles folds to
	/// the time of planning, one value for the statement. Code that runs after
	/// a change it was not planned against ([`Self::late_binding`]) or that
	/// the compile cache keeps for later statements ([`Self::kept`]) reads
	/// the clock when it runs.
	pub(crate) fn folds_clock(&self) -> bool {
		!self.late_bound && !self.kept
	}

	/// What the node's units key catalog unit `unit` lowered by this planner
	/// for `frame` on; `None` where the planner plans with something the key
	/// does not hold, whose unit is then lowered for this plan alone: a
	/// `VERSION`, a planning scope of a `MATCHES` or KNN predicate, or a table
	/// or event compilation open on its cycle guard. The caller takes the key
	/// only where [`Self::unit_cache`] answers.
	pub(crate) async fn catalog_unit_key(
		&self,
		unit: crate::exec::vm::units::CatalogUnit,
		frame: crate::exec::vm::program::UnitFrame,
	) -> Option<crate::exec::vm::units::CatalogUnitKey> {
		if !self.keeps_catalog_units() {
			return None;
		}
		let ids = self.ns_db_ids().await?;
		let mut shadowed: Vec<String> = self.shadowed_params.to_vec();
		shadowed.sort();
		shadowed.dedup();
		Some(crate::exec::vm::units::CatalogUnitKey {
			ns: self.ns.clone()?,
			db: self.db.clone()?,
			ids,
			unit,
			isa: crate::exec::vm::program::ISA_VERSION,
			frame,
			depth: self.depth,
			shadowed,
			capabilities: crate::exec::vm::units::CapabilitiesId::new(self.ctx.get_capabilities()),
		})
	}

	/// Whether the body of `fn::name` is on the path of bodies being compiled.
	pub(crate) fn on_function_path(&self, name: &str) -> bool {
		self.function_path.iter().any(|f| f.name == name)
	}

	/// Whether compiling `fn::name` binds a value from the planning context
	/// into its code: whether its body, or the body of any function it calls,
	/// reads from outside a session value this planner neither shadows nor
	/// lacks, which binding substitutes where it is read as a predicate or a
	/// source. A body binds only the session's own values
	/// ([`UNIT_RUNTIME_PARAMS`]) at plan time and reads every other name when
	/// it runs, so no other name of the planning context is folded into it.
	/// `true` when a body cannot be read.
	pub(crate) async fn folds_context(
		&self,
		name: &str,
		txn: &crate::kvs::Transaction,
		(ns, db): (crate::catalog::NamespaceId, crate::catalog::DatabaseId),
	) -> bool {
		use crate::kvs::cache::ds::FunctionSnapshot;
		let cache = self.ctx.get_cache();
		let Ok(Some(snapshot)) =
			FunctionSnapshot::for_function(cache.as_deref(), txn, ns, db, name).await
		else {
			return true;
		};
		let mut names = vec![name.to_owned()];
		names.extend(snapshot.facts().calls.iter().cloned());
		for called in names {
			let Ok(Some(snapshot)) =
				FunctionSnapshot::for_function(cache.as_deref(), txn, ns, db, &called).await
			else {
				return true;
			};
			let Ok(body) = snapshot.body() else {
				return true;
			};
			let args: Vec<&str> =
				snapshot.definition().args.iter().map(|(arg, _)| arg.as_str()).collect();
			let mut folds = false;
			let _ = body.visit_scalars_deep(&mut |scalar| {
				let _ = scalar.visit_deep(&mut |e| {
					if let LogicalExpr::Param(param) = e {
						let param = param.as_str();
						if !args.contains(&param)
							&& !util::SELECT_ITERATION_PARAMS.contains(&param)
							&& UNIT_RUNTIME_PARAMS.contains(&param)
							&& !self.param_is_shadowed(param)
							&& self.ctx.value(param).is_some()
						{
							folds = true;
						}
					}
					std::ops::ControlFlow::<()>::Continue(())
				});
				Ok::<_, std::convert::Infallible>(())
			});
			if folds {
				return true;
			}
		}
		false
	}

	/// The body of `fn::name` as a call binds it, ready to be compiled: the
	/// planner it is compiled with and the body bound by it.
	///
	/// The body is bound as the runtime would bind it for a call in the
	/// caller's frame — for the planner's principal capped by the function's
	/// `AUTH LIMIT`, with the arguments as names bound per call — and is
	/// planned in its own scope one re-entry deeper than the call.
	///
	/// `None` when the planner has no transaction or database, is planning
	/// under a `VERSION`, or when the function is undefined, disallowed, has
	/// no logical form, is on the path being compiled, or does not bind.
	///
	/// A name the body reads that is not an argument or a session value is
	/// left a runtime lookup rather than resolved at plan time, so no caller's
	/// binding and no `DEFINE PARAM` value is baked into the body. A body that
	/// reads an argument in a predicate or a source is declined as well: a
	/// plan compiled here would carry a name there, which no index answers.
	pub(crate) async fn prepare_function_body(&self, name: &str) -> Option<PreparedBody<'ctx>> {
		if self.on_function_path(name) {
			return None;
		}
		let FunctionLookup::Found(snapshot) = self.function_snapshot(name).await else {
			return None;
		};
		crate::exec::vm::out_of_line(|| self.prepare_snapshot(name, &snapshot)).await
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
			PlanTimeVersion::Unknown => return FunctionLookup::Unavailable,
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

	/// [`Self::prepare_function_body`] for the function `snapshot` reads,
	/// including one on the path being compiled, whose recursive edge the
	/// caller finds.
	pub(crate) async fn prepare_snapshot(
		&self,
		name: &str,
		snapshot: &Arc<crate::kvs::cache::ds::FunctionSnapshot>,
	) -> Option<PreparedBody<'ctx>> {
		let (Some(txn), Some((ns, db))) = (&self.txn, self.ns_db_ids().await) else {
			return None;
		};
		let definition = snapshot.definition();
		let body = snapshot.body().ok()?.clone();

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
		let (planner, frame) = self.body_scope(name, snapshot)?;
		let mut row_scoped: Vec<&str> = util::SELECT_ITERATION_PARAMS.to_vec();
		row_scoped.extend(definition.args.iter().map(|(arg, _)| arg.as_str()));
		let mut unbound = Vec::new();
		for param in &referenced {
			if row_scoped.contains(&param.as_str()) {
				continue;
			}
			// Only the session's own values are read here. Any other name is
			// left to the runtime, which binds it in the frame the call runs in:
			// a caller's binding of it is then visible to an ordinary call and
			// absent from a privileged frame, and no unit folds one caller's
			// binding for another. A `DEFINE PARAM` is read at run time too.
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

		let bound = match planner.bind_scoped(body, &row_scoped).await {
			Ok(bound) => bound,
			Err(e) => {
				tracing::debug!("fn::{name} body is left for the runtime to bind: {e:#}");
				return None;
			}
		};
		let args: Vec<&str> = definition.args.iter().map(|(arg, _)| arg.as_str()).collect();
		if crate::exec::logical::reads_in_resolvable_position(&bound, &args) {
			return None;
		}
		Some(PreparedBody {
			planner,
			bound,
			frame,
			specialised: false,
		})
	}

	/// The body of `fn::name` left as written, for each call to run compiled
	/// for the values its arguments hold, as the compile service compiles it
	/// on a miss: the planner the body is planned with and the body, which no
	/// pass has bound.
	///
	/// This is the body of a function [`Self::prepare_snapshot`] does not
	/// bind, because it reads an argument where only a value can be
	/// answered, or because it does not bind against the catalog as it
	/// stands. `None` for a function whose body has no logical form, or whose
	/// `AUTH LIMIT` does not parse.
	pub(crate) fn specialised_snapshot(
		&self,
		name: &str,
		snapshot: &Arc<crate::kvs::cache::ds::FunctionSnapshot>,
	) -> Option<PreparedBody<'ctx>> {
		let bound = snapshot.body().ok()?.clone();
		let (planner, frame) = self.body_scope(name, snapshot)?;
		Some(PreparedBody {
			planner,
			bound,
			frame,
			specialised: true,
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
	/// The body's context is this planner's: a privileged frame's planner
	/// already holds an isolated one, so the body reads nothing of the caller
	/// there ([`ExecutionContext::privileged_frame`]).
	fn body_scope(
		&self,
		name: &str,
		snapshot: &Arc<crate::kvs::cache::ds::FunctionSnapshot>,
	) -> Option<(Planner<'ctx>, FunctionFrame)> {
		let definition = snapshot.definition();
		let mut planner =
			self.shadowing(definition.args.iter().map(|(arg, _)| arg.as_str().to_owned()));
		planner.version = None;
		planner.matches_scope = None;
		planner.matches_context = None;
		planner.knn_context = None;
		planner.within = CatalogWithin::default();
		planner.depth = self.depth + 1;
		planner.auth = self.function_principal(definition)?;
		let frame = FunctionFrame {
			name: name.to_owned(),
			definition: snapshot.content_hash(),
			compiled_from: Arc::clone(snapshot.definition()),
			body: crate::exec::physical_expr::RecursiveBody::new(),
			checked: planner.auth.is_some()
				&& !planner.perms_suppressed
				&& planner.compiles_for_checked_caller(),
		};
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
			matches_scope: None,
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
			kept: self.kept,
			within: CatalogWithin::default(),
			stored_clause: false,
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
		planner
	}

	/// This planner with `names` added to [`Self::shadowed_params`]: the
	/// planner for the body of a `FOR`, a closure or a user-defined function,
	/// which binds those names around it.
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
			matches_scope: self.matches_scope.clone(),
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
			kept: self.kept,
			within: self.within,
			stored_clause: self.stored_clause,
		}
	}

	/// What this planner plans with, beyond its caches: two planners with
	/// equal keys plan the same statement to the same plan. The shared parts
	/// — the transaction, the `VERSION` expression, the planning scopes and
	/// the speculation — are compared by identity, so a key is meaningful only
	/// among planners derived from one another while one statement is planned.
	pub(crate) fn frame_key(&self) -> PlannerFrameKey {
		fn identity<T: ?Sized>(shared: Option<&Arc<T>>) -> usize {
			shared.map_or(0, |shared| Arc::as_ptr(shared).cast::<()>() as usize)
		}
		let mut shadowed = self.shadowed_params.to_vec();
		shadowed.sort_unstable();
		shadowed.dedup();
		PlannerFrameKey {
			ns: self.ns.clone(),
			db: self.db.clone(),
			auth: self.auth.as_deref().cloned(),
			perms_suppressed: self.perms_suppressed,
			plan_time_version: self.plan_time_version,
			depth: self.depth,
			shadowed,
			functions: self
				.function_path
				.iter()
				.map(|frame| (frame.name.clone(), frame.definition, frame.checked))
				.collect(),
			shared: [
				identity(self.txn.as_ref()),
				identity(self.version.as_ref()),
				identity(self.matches_scope.as_ref()),
				identity(self.matches_context.as_ref()),
				identity(self.knn_context.as_ref()),
				identity(self.speculation.as_ref()),
			],
			late_bound: self.late_bound,
			when_reached: self.when_reached,
			within: self.within,
			stored_clause: self.stored_clause,
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

	/// The namespace and database whose `DEFINE PARAM`s a parameter may be
	/// bound to at plan time: none where the statement inlines no catalog
	/// definition ([`CatalogWithin::calls_current`]), whose parameters are
	/// read when it runs.
	pub(crate) async fn definition_ids(
		&self,
	) -> Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)> {
		match self.within.calls_current {
			true => None,
			false => self.ns_db_ids().await,
		}
	}

	/// How the statement this planner plans reaches what code it runs may
	/// redefine ([`CatalogWithin`]).
	pub(crate) fn catalog_within(&self) -> CatalogWithin {
		self.within
	}

	/// Whether this planner plans a statement whose catalog is loaded when it
	/// runs ([`Self::late_binding`]).
	pub(crate) fn is_late_bound(&self) -> bool {
		self.late_bound
	}

	/// This planner planning a statement a block compiles when it reaches it,
	/// after the block's first schema statement: an `IF` in it evaluates only
	/// the branch taken.
	pub(crate) fn compiled_when_reached(&self) -> Planner<'ctx> {
		let mut reached = self.shadowing(std::iter::empty());
		reached.when_reached = true;
		reached
	}

	/// Whether this planner plans a statement a block compiles when it
	/// reaches it ([`Self::compiled_when_reached`]).
	pub(crate) fn is_compiled_when_reached(&self) -> bool {
		self.when_reached
	}

	/// This planner planning a stored clause, or a statement nested in one
	/// ([`Self::is_stored_clause`]).
	pub(crate) fn in_stored_clause(mut self) -> Self {
		self.stored_clause = true;
		self
	}

	/// Whether this planner plans a stored clause, or a statement nested in
	/// one: an event's `WHEN` or `THEN`, a field's `VALUE`, `ASSERT`,
	/// `DEFAULT` or `COMPUTED`. In such code a `LET` written where its value
	/// is read raises when it is evaluated.
	pub(crate) fn is_stored_clause(&self) -> bool {
		self.stored_clause
	}

	/// Whether this planner plans what follows a statement of a block that
	/// may change what the rest of it resolves against late-bound
	/// ([`Self::binding_late_after_schema`]).
	pub(crate) fn binds_late_after_schema(&self) -> bool {
		self.late_after_schema
	}

	/// This planner over `txn` in place of its own transaction: a
	/// speculation's view, which plans against the catalog the speculation's
	/// writes leave ([`crate::exec::vm`]).
	pub(crate) fn over(&self, txn: Arc<crate::kvs::Transaction>) -> Planner<'ctx> {
		let mut over = self.shadowing(std::iter::empty());
		over.txn = Some(txn);
		over
	}

	/// This planner compiling for a frame that waives permission checks, for
	/// the same principal: binding resolves every source, its computed fields
	/// derived and no permission or field guard planted, as a frame that
	/// skips permissions reads it. What a call evaluated in a permission frame
	/// is planned with.
	pub(crate) fn waiving_permissions(&self) -> Planner<'ctx> {
		self.shadowing(std::iter::empty()).with_perms_suppressed(true)
	}

	/// Whether an enclosing `FOR`, closure or user-defined function binds
	/// `name`, so that a read of it is not the ambient context's value.
	pub(crate) fn param_is_shadowed(&self, name: &str) -> bool {
		self.shadowed_params.iter().any(|shadowed| shadowed == name)
	}

	/// The access mode a stored function — `fn::`, module or silo — needs of
	/// the transaction; a module or silo function is a write when its
	/// signature says so or when that cannot be resolved.
	pub(crate) async fn resolve_function_access_mode(
		&self,
		function: &crate::expr::Function,
	) -> crate::exec::AccessMode {
		use crate::exec::AccessMode;
		use crate::expr::Function;

		let writeable = |resolved: Result<bool, Error>| {
			if resolved.unwrap_or(true) {
				AccessMode::ReadWrite
			} else {
				AccessMode::ReadOnly
			}
		};
		match function {
			Function::Custom(name) => self.resolve_custom_access_mode(name).await,
			Function::Module(module, sub) => {
				writeable(self.resolve_module_writeable(module, sub.as_deref()).await)
			}
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => writeable(
				self.resolve_silo_writeable(org, pkg, *major, *minor, *patch, sub.as_deref()).await,
			),
			Function::Normal(_) | Function::Script(_) | Function::Model(_) => AccessMode::ReadOnly,
		}
	}

	/// The transaction and the namespace and database ids a definition whose
	/// `PERMISSIONS` predicate a node carries compiled is read through.
	///
	/// Each `planned_*_guard` reads the definition and compiles its clause
	/// while the node is planned, unsealed off the catalog even where the
	/// planner compiles the node for a lowering that is sealed: the reads are
	/// plan-time reads, recorded by the transaction like any other, and
	/// nothing is read when the program runs. `None`
	/// where the planner reads the catalog at another version than the
	/// current one, or has no transaction or database, and the node compiles
	/// the predicate for the definition its evaluation reads.
	async fn guard_catalog(
		&self,
	) -> Option<(
		Arc<crate::kvs::Transaction>,
		crate::catalog::NamespaceId,
		crate::catalog::DatabaseId,
	)> {
		if self.version.is_some() || !matches!(self.plan_time_version, PlanTimeVersion::Current) {
			return None;
		}
		let txn = self.txn.clone()?;
		let (ns, db) = self.ns_db_ids().await?;
		Some((txn, ns, db))
	}

	/// The `PERMISSIONS` predicate of `fn::name` compiled for a call planned
	/// here ([`crate::exec::physical_expr::PlannedGuard`]).
	async fn planned_function_guard(
		&self,
		name: &str,
	) -> crate::exec::physical_expr::PlannedGuard<crate::catalog::FunctionDefinition> {
		use crate::catalog::providers::DatabaseProvider;
		let Some((txn, ns, db)) = self.guard_catalog().await else {
			return Default::default();
		};
		let Ok(definition) = txn.get_db_function(ns, db, name, None).await else {
			return Default::default();
		};
		let Ok(permission) = definition.permissions_body.logical_ref() else {
			return Default::default();
		};
		crate::exec::physical_expr::PlannedGuard::compile(
			self,
			Arc::clone(&definition),
			permission,
			Some(&definition.auth_limit),
		)
		.await
	}

	/// The `PERMISSIONS` predicate of the `DEFINE PARAM` a read of `$name`
	/// planned here reaches, compiled; empty for a name the plan binds, the
	/// context holds, or the engine binds around an evaluation, none of which
	/// reaches a definition.
	async fn planned_param_guard(
		&self,
		name: &str,
	) -> crate::exec::physical_expr::PlannedGuard<crate::catalog::ParamDefinition> {
		use crate::catalog::providers::DatabaseProvider;
		const ENGINE_BOUND: &[&str] =
			&["this", "self", "parent", "value", "before", "after", "input", "event", "reference"];
		if ENGINE_BOUND.contains(&name)
			|| surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name)
			|| self.param_is_shadowed(name)
			|| self.ctx.value(name).is_some()
		{
			return Default::default();
		}
		let Some((txn, ns, db)) = self.guard_catalog().await else {
			return Default::default();
		};
		let Ok(definition) = txn.get_db_param(ns, db, name, None).await else {
			return Default::default();
		};
		let Ok(permission) = definition.permissions_body.logical_ref() else {
			return Default::default();
		};
		crate::exec::physical_expr::PlannedGuard::compile(
			self,
			Arc::clone(&definition),
			permission,
			None,
		)
		.await
	}

	/// The `PERMISSIONS` predicate of model `model` compiled for a call
	/// planned here.
	#[cfg(feature = "ml")]
	async fn planned_model_guard(
		&self,
		model: &crate::expr::Model,
	) -> crate::exec::physical_expr::PlannedGuard<crate::catalog::MlModelDefinition> {
		use crate::catalog::providers::DatabaseProvider;
		let Some((txn, ns, db)) = self.guard_catalog().await else {
			return Default::default();
		};
		let Ok(Some(definition)) =
			txn.get_db_model(ns, db, &model.name, &model.version, None).await
		else {
			return Default::default();
		};
		let Ok(permission) = definition.permissions_body.logical_ref() else {
			return Default::default();
		};
		crate::exec::physical_expr::PlannedGuard::compile(
			self,
			Arc::clone(&definition),
			permission,
			None,
		)
		.await
	}

	/// The `PERMISSIONS` predicate of the module stored as `storage_name`
	/// compiled for a call planned here.
	#[cfg(feature = "surrealism")]
	async fn planned_module_guard(
		&self,
		storage_name: &str,
	) -> crate::exec::physical_expr::PlannedGuard<crate::catalog::ModuleDefinition> {
		use crate::catalog::providers::DatabaseProvider;
		let Some((txn, ns, db)) = self.guard_catalog().await else {
			return Default::default();
		};
		let Ok(definition) = txn.get_db_module(ns, db, storage_name, None).await else {
			return Default::default();
		};
		let Ok(permission) = definition.permissions_body.logical_ref() else {
			return Default::default();
		};
		crate::exec::physical_expr::PlannedGuard::compile(
			self,
			Arc::clone(&definition),
			permission,
			None,
		)
		.await
	}

	/// Resolve the access mode of a user-defined function's body at plan time,
	/// by walking the call graph in the plan-time snapshot
	/// ([`crate::fnc::mutability`], conservative polarity).
	///
	/// Returns `AccessMode::ReadWrite` — the safe over-approximation — when the
	/// planner lacks the transaction or namespace/database context to resolve,
	/// or when the resolution errors. [`Self::resolve_module_writeable`] takes
	/// the same write-side default for missing context, though it propagates a
	/// resolution error rather than absorbing it. A `ReadOnly` answer is
	/// returned only when the whole reachable call graph is provably write-free
	/// in this snapshot, so a `ReadWrite` fallback can never mis-license a
	/// write onto a read-only fast path.
	///
	/// The snapshot is the one the call will run against: the runtime reads a
	/// body at the statement's `VERSION`, so the walk reads it at the same
	/// stamp. A definition redefined since that stamp says nothing about the
	/// body that will run, and a `VERSION` only the runtime can evaluate
	/// leaves the stamp unknown, which is the `ReadWrite` case.
	pub(crate) async fn resolve_custom_access_mode(&self, name: &str) -> crate::exec::AccessMode {
		self.resolve_custom_function(name).await.access_mode
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
			PlanTimeVersion::Unknown => return UNRESOLVED,
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
						// Not an unresolved flag but a resolved absence: the
						// invocation repeats this lookup and fails on it, so the
						// call cannot reach a module body and cannot write.
						return Ok(false);
					}
					return Err(EngineError::Internal(e.to_string()).into());
				}
			};
		let executable: ModuleExecutable = val.executable.clone().into();
		// The planner's self.ctx does not carry a transaction (the executor
		// sets it after planning). Derive a context with the planner's txn
		// so that a cache-miss in get_surrealism_runtime can access the
		// bucket store without panicking.
		let mut plan_ctx = Context::new_child(&self.ctx);
		plan_ctx.set_transaction(Arc::clone(txn));
		let frozen = plan_ctx.freeze();
		let sig = crate::fnc::module::module_executable_signature(
			&executable,
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
		// Same as resolve_module_writeable: derive a context with the
		// planner's transaction so signature resolution can access stores.
		let mut plan_ctx = Context::new_child(&self.ctx);
		plan_ctx.set_transaction(Arc::clone(txn));
		let frozen = plan_ctx.freeze();
		let sig = crate::fnc::module::silo_executable_signature(
			&executable,
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

	/// Compile a bound logical plan into an operator tree.
	///
	/// The only way a plan becomes operators: a bottom-up fold over the tree
	/// (`fold.rs`), each variant compiling from its already-compiled inputs.
	/// A GQL `MATCH` plan is a leaf of that fold, compiled whole by
	/// `plan_match` (`match_plan.rs`).
	///
	/// The re-entry depth is checked here as well as in
	/// [`Self::physical_expr`], because a runtime re-entry can begin at the
	/// statement level — a user-defined function body — as well as at a scalar.
	#[cfg(test)]
	pub(crate) async fn plan_logical(&self, op: LogicalOp) -> Result<Arc<dyn ExecOperator>, Error> {
		crate::exec::compiles::note();
		#[cfg(test)]
		COMPILES.with(|compiles| compiles.set(compiles.get() + 1));
		self.check_depth()?;
		self.fold(op).await
	}

	// ========================================================================
	// Scalar compilation
	// ========================================================================

	/// Compile a scalar into a physical expression.
	///
	/// Physical expressions are evaluated at runtime to produce values. This
	/// is used for expressions within operators (WHERE predicates, SELECT
	/// field expressions, ORDER BY expressions) and is the only way a
	/// [`LogicalExpr`] becomes one.
	///
	/// A scalar that denotes a value — a literal, or a collection or record id
	/// whose every part is one — becomes that value once, at plan time. Without
	/// this a whole object or array literal is rebuilt per row from its
	/// per-entry physical expressions, which is the bulk of the cost of
	/// importing records.
	pub(crate) async fn physical_expr(
		&self,
		expr: LogicalExpr,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let compiled = Box::pin(self.physical_expr_node(expr)).await?;
		Ok(crate::exec::physical_expr::tree_guard::guarded(compiled))
	}

	/// [`Self::physical_expr`], unguarded.
	async fn physical_expr_node(&self, expr: LogicalExpr) -> Result<Arc<dyn PhysicalExpr>, Error> {
		#[cfg(test)]
		COMPILES.with(|compiles| compiles.set(compiles.get() + 1));
		// Reject if this planner's re-entry depth (carried in from the boundary
		// that started it) has already exceeded `max_computation_depth`. The depth
		// recorded onto `eval` / UDF / JS nodes below lets a runtime re-entry
		// continue the count from here.
		crate::exec::compiles::note();
		self.check_depth()?;

		if let Some(value) = static_value(&expr) {
			return Ok(Arc::new(PhysicalLiteral(value)));
		}

		match expr {
			LogicalExpr::Literal(lit) => Ok(Arc::new(PhysicalLiteral(literal_value(lit)))),
			LogicalExpr::Mock(mock) => Ok(Arc::new(MockExpr(mock))),
			LogicalExpr::Array(items) => Ok(Arc::new(ArrayLiteral {
				elements: self.physical_args(items).await?,
			})),
			LogicalExpr::Set(items) => Ok(Arc::new(SetLiteral {
				elements: self.physical_args(items).await?,
			})),
			LogicalExpr::Object(entries) => {
				let mut phys_entries = Vec::with_capacity(entries.len());
				for (key, value) in entries {
					let value = crate::exec::vm::out_of_line(|| self.physical_expr(value)).await?;
					phys_entries.push((Strand::from(key), value));
				}
				Ok(Arc::new(ObjectLiteral {
					entries: phys_entries,
				}))
			}
			LogicalExpr::RecordId(rid) => {
				let key = self.physical_record_key(rid.key).await?;
				Ok(Arc::new(RecordIdExpr {
					table: rid.table,
					key,
				}))
			}
			LogicalExpr::Param(p) => {
				let name = p.into_strand();
				let planned = crate::exec::vm::out_of_line(|| {
					crate::kvs::catalog_reads::sealed(
						false,
						self.planned_param_guard(name.as_str()),
					)
				})
				.await;
				Ok(Arc::new(Param::with_planned_guard(name, planned)))
			}
			LogicalExpr::Table(t) => Ok(Arc::new(PhysicalLiteral(Value::Table(t)))),
			LogicalExpr::Constant(c) => Ok(Arc::new(PhysicalLiteral(c.compute()))),
			// Every path form compiles as a path; see `path.rs`.
			expr @ (LogicalExpr::Field(_)
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
			}) => self.physical_path(expr).await,
			LogicalExpr::SetField {
				base,
				field,
				value,
			} => {
				let base = crate::exec::vm::out_of_line(|| self.physical_expr(*base)).await?;
				let value = crate::exec::vm::out_of_line(|| self.physical_expr(*value)).await?;
				Ok(Arc::new(SetFieldExpr {
					base,
					field,
					value,
				}))
			}

			LogicalExpr::Binary {
				left,
				op,
				right,
			} => crate::exec::vm::out_of_line(|| self.physical_binary_expr(*left, op, *right)).await,
			LogicalExpr::Prefix {
				op,
				expr,
			} => crate::exec::vm::out_of_line(|| self.physical_prefix_expr(op, *expr)).await,
			LogicalExpr::Postfix {
				expr,
				op,
			} => crate::exec::vm::out_of_line(|| self.physical_postfix_expr(*expr, op)).await,
			LogicalExpr::Invoke {
				target,
				args,
			} => crate::exec::vm::out_of_line(|| self.physical_invoke(*target, args)).await,

			LogicalExpr::Call(call) => {
				crate::exec::vm::out_of_line(|| self.physical_call(*call)).await
			}
			LogicalExpr::Closure(closure) => {
				crate::exec::vm::out_of_line(|| self.physical_closure(*closure)).await
			}
			LogicalExpr::If(branches) => {
				crate::exec::vm::out_of_line(|| self.physical_if(*branches)).await
			}

			LogicalExpr::Break => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Break,
				inner: None,
			})),
			LogicalExpr::Continue => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Continue,
				inner: None,
			})),
			LogicalExpr::Throw(e) => {
				let inner = crate::exec::vm::out_of_line(|| self.physical_expr(*e)).await?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Throw,
					inner: Some(inner),
				}))
			}

			// A statement in scalar position: its plan is compiled like any
			// other and evaluated per row through a scalar subquery. `LET`
			// binds a name and yields nothing, so it is a statement only.
			LogicalExpr::Subquery(op) => {
				if let LogicalOp::Let(_) = *op {
					return Err(ExecError::InvalidStatement(
						"LET statements can only appear at the top level of a query or inside a \
						 block expression"
							.to_string(),
					)
					.into());
				}
				let inherits_row = op.inherits_row();
				// A planner with no transaction cannot bind the statement or
				// lower it, which read the catalog; what holds it runs as a
				// unit lowered over one, which plans it there.
				if self.txn.is_none() {
					return Ok(Arc::new(UnplannedSubquery::new(&op, self.function_registry)));
				}
				// The scalar may be a stored clause, which holds its statements
				// as written, or part of a bound statement; binding does not
				// disturb what is bound already.
				let unbound =
					matches!(
						*op,
						LogicalOp::Sequence(_)
							| LogicalOp::Foreach(_)
							| LogicalOp::Let(_) | LogicalOp::Eval(_)
							| LogicalOp::If(_) | LogicalOp::Throw(_)
							| LogicalOp::Break(_) | LogicalOp::Continue(_)
					)
					.then(|| (*op).clone());
				// As `plan_statement_with` does: a schema statement the plan
				// cannot see the effect of is not a refusal here, since
				// `plan_checked` plans what follows it again late-bound.
				if let Some(why) = crate::exec::logical::unplannable_write(&op) {
					return Err(ExecError::PlannerUnsupported {
						reason: why,
					}
					.into());
				}
				// A statement written in an expression of one that may redefine
				// what it names is planned late-bound; any other answers for
				// what it runs itself.
				let planner = if self.within.nested_late {
					self.late_binding()
				} else {
					let within = crate::exec::vm::out_of_line(|| {
						crate::exec::vm::catalog_within(self, &op, &self.shadowed_params)
					})
					.await;
					self.shadowing(std::iter::empty()).reaching(within)
				};
				let bound = crate::exec::vm::out_of_line(|| planner.bind(*op)).await?;
				let plan = crate::exec::vm::out_of_line(|| {
					crate::exec::vm::plan_checked(&planner, &bound, unbound.as_ref())
				})
				.await?;
				Ok(Arc::new(ScalarSubquery {
					plan,
					inherits_row,
				}))
			}
		}
	}

	/// Compile a binary expression.
	///
	/// Handles the MATCHES operator special-case (full-text index evaluation),
	/// the KNN membership form, and the `SimpleBinaryOp` optimisation for
	/// `field op literal` patterns.
	async fn physical_binary_expr(
		&self,
		left: LogicalExpr,
		op: BinaryOperator,
		right: LogicalExpr,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		// Every MATCHES becomes a `MatchesOp`, which owns the operator's whole
		// per-row decision tree (see that type's module docs). A path on the
		// left with a plan-time-resolvable right additionally carries a
		// `MatchProbe`, naming the path and query string an index can be
		// resolved from; the shapes that cannot name an index carry none and so
		// answer `false` for rows of a source that registers no full-text
		// index and `NoIndexFoundForMatch` for the rest.
		if let BinaryOperator::Matches(ref matches_op) = op {
			// Determine whether this exact expression is registered in the
			// enclosing SELECT's WHERE condition (see `MatchesScope`). The lookup key is the node
			// as written; the allowlist contains both the original and the bound condition
			// forms, so both the projection-side and residual-cond-side
			// compilations of the same source expression hit it. The bind
			// passes substitute a statement-scoped parameter in a predicate
			// but not in a projection, so a projection whose query is a
			// parameter is also looked up with the parameter's value in place,
			// the form the WHERE holds after binding. A parameter bound around
			// the plan is substituted in neither, and has no value here.
			let (registered, executor_tables) = match &self.matches_scope {
				Some(scope) => {
					let key = |right: LogicalExpr| LogicalExpr::Binary {
						left: Box::new(left.clone()),
						op: BinaryOperator::Matches(matches_op.clone()),
						right: Box::new(right),
					};
					let registered = scope.allowlist.contains(&key(right.clone()))
						|| match &right {
							LogicalExpr::Param(param)
								if !self.param_is_shadowed(param.as_str()) =>
							{
								self.ctx
									.value(param.as_str())
									.cloned()
									.and_then(try_value_to_logical_expr)
									.is_some_and(|value| scope.allowlist.contains(&key(value)))
							}
							_ => false,
						};
					(registered, scope.executor_tables.clone())
				}
				None => (false, crate::exec::physical_expr::ExecutorTables::none()),
			};

			// The probe is a path on the left and a right operand that names a
			// value, rendered with `Value::to_raw_string()`. That keeps a bare
			// string literal as its own contents and renders every other value
			// the way the analyzer receives it — `title @@ 42` searches for the
			// term `42`. A parameter bound around the plan — a closure's
			// argument, a `FOR` variable — holds the binding's value when the
			// operator runs, not the planning context's, so its query is read
			// then.
			let probe =
				|query: &Value, path: &LogicalExpr| crate::exec::physical_expr::MatchProbe {
					field: path.clone(),
					query: Some(query.to_raw_string()),
				};
			let probe = match (&left, &right) {
				(path, LogicalExpr::Param(param))
					if path.is_path() && self.param_is_shadowed(param.as_str()) =>
				{
					Some(crate::exec::physical_expr::MatchProbe {
						field: path.clone(),
						query: None,
					})
				}
				(path, LogicalExpr::Param(param)) if path.is_path() => {
					self.ctx.value(param.as_str()).map(|v| probe(v, path))
				}
				(path, other) if path.is_path() => static_value(other).map(|v| probe(&v, path)),
				_ => None,
			};

			// The right side is rendered from the resolved query when there is
			// one, so `ToSql` shows what was actually searched for rather than
			// the unresolved parameter.
			let right_source = match probe.as_ref().and_then(|probe| probe.query.as_ref()) {
				Some(query) => {
					LogicalExpr::Literal(LogicalLiteral::String(Strand::from(query.clone())))
				}
				None => right,
			};
			let left_phys = crate::exec::vm::out_of_line(|| self.physical_expr(left)).await?;
			let right_phys =
				crate::exec::vm::out_of_line(|| self.physical_expr(right_source)).await?;
			return Ok(Arc::new(crate::exec::physical_expr::MatchesOp::new(
				left_phys,
				right_phys,
				matches_op.clone(),
				probe,
				registered,
				executor_tables,
			)));
		}

		// KNN operators evaluated as expressions (projection position, a
		// residual conjunct no KNN source consumed, bare expressions):
		// per-row membership in the statement's KNN result set, `false`
		// without one. The
		// KnnContext is bound here because the execution context does not
		// carry it.
		if matches!(op, BinaryOperator::NearestNeighbor(_)) {
			let left_phys = crate::exec::vm::out_of_line(|| self.physical_expr(left)).await?;
			let right_phys = crate::exec::vm::out_of_line(|| self.physical_expr(right)).await?;
			return Ok(Arc::new(crate::exec::physical_expr::KnnMembershipOp::new(
				left_phys,
				right_phys,
				op,
				self.knn_context.clone(),
			)));
		}

		// All other binary operators
		let left_phys = crate::exec::vm::out_of_line(|| self.physical_expr(left)).await?;
		let right_phys = crate::exec::vm::out_of_line(|| self.physical_expr(right)).await?;

		// Optimisation: detect `field op literal` or `literal op field`
		// patterns and emit a SimpleBinaryOp that inlines field access
		// and avoids per-record async dispatch + Value cloning.
		if is_simple_binary_eligible(&op) {
			if let Some(field) = left_phys.try_simple_field()
				&& let Some(lit) = right_phys.try_literal()
			{
				return Ok(Arc::new(crate::exec::physical_expr::SimpleBinaryOp {
					field_name: field.to_string(),
					op,
					literal: lit.clone(),
					reversed: false,
				}));
			} else if let Some(field) = right_phys.try_simple_field()
				&& let Some(lit) = left_phys.try_literal()
			{
				return Ok(Arc::new(crate::exec::physical_expr::SimpleBinaryOp {
					field_name: field.to_string(),
					op,
					literal: lit.clone(),
					reversed: true,
				}));
			}
		}

		Ok(Arc::new(BinaryOp {
			left: left_phys,
			op,
			right: right_phys,
		}))
	}

	/// Compile a prefix (unary) expression.
	async fn physical_prefix_expr(
		&self,
		op: PrefixOperator,
		expr: LogicalExpr,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		// Prefix/cast chains (`!!!!x`, repeated casts) build a `UnaryOp` spine that
		// the evaluator walks recursively at runtime. The parser's
		// `expr_recursion_limit` can sit above `max_computation_depth`, so a chain
		// in that window would slip past parsing and overflow at evaluation. Bound
		// the spine length here, iteratively (no recursion), against the
		// computation limit — offset by this planner's carried re-entry `depth` so
		// the bound is continuous across `eval`/UDF boundaries, as the
		// `Options::dive` accounting is.
		{
			let mut d = self.depth;
			let mut cur = &expr;
			while let LogicalExpr::Prefix {
				expr: inner,
				..
			} = cur
			{
				d += 1;
				if d > self.ctx.config.exec.max_computation_depth {
					return Err(ExecError::ComputationDepthExceeded.into());
				}
				cur = inner;
			}
		}
		let inner = crate::exec::vm::out_of_line(|| self.physical_expr(expr)).await?;
		Ok(Arc::new(UnaryOp {
			op,
			expr: inner,
		}))
	}

	/// Compile a postfix expression.
	///
	/// The open-ended range operators wrap the operand.
	async fn physical_postfix_expr(
		&self,
		expr: LogicalExpr,
		op: LogicalPostfixOperator,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let inner = crate::exec::vm::out_of_line(|| self.physical_expr(expr)).await?;
		Ok(Arc::new(match op {
			LogicalPostfixOperator::Range => PostfixOp::range(inner),
			LogicalPostfixOperator::RangeSkip => PostfixOp::range_skip(inner),
		}))
	}

	/// An application of a value: the target evaluates to a closure, which is
	/// applied to the arguments.
	async fn physical_invoke(
		&self,
		target: LogicalExpr,
		args: Vec<LogicalExpr>,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let target = crate::exec::vm::out_of_line(|| self.physical_expr(target)).await?;
		let arguments = self.physical_args(args).await?;
		Ok(Arc::new(ClosureCallExec {
			target,
			arguments,
		}))
	}

	/// The `count(->edge)` projection shape the graph-degree fast path serves.
	///
	/// One argument, a bare graph lookup over the row — no clauses, no
	/// `ONLY`, plain table subjects — counted for a session whose permission
	/// checks are provably skipped (the id-only resolve such a session takes
	/// never fetches records either, so the raw adjacency cardinality is
	/// exactly its answer) and reading current data. The lowering gives a
	/// single trailing lookup as `Lookup` over `Current` with no broadcast,
	/// which is the only shape that qualifies.
	pub(crate) fn graph_degree_shape(
		&self,
		name: &str,
		arguments: &[LogicalExpr],
	) -> Option<(crate::exec::parts::LookupDirection, Vec<surrealdb_strand::TableName>)> {
		use crate::expr::logical::LogicalLookupSubject;
		use crate::expr::lookup::LookupKind;
		if name != "count" || arguments.len() != 1 || self.version.is_some() {
			return None;
		}
		let (Some(ns), Some(db)) = (&self.ns, &self.db) else {
			return None;
		};
		if self.should_check_perms_for_view(ns, db) {
			return None;
		}
		let LogicalExpr::Lookup {
			base,
			lookup,
		} = &arguments[0]
		else {
			return None;
		};
		if !matches!(base.as_ref(), LogicalExpr::Current) {
			return None;
		}
		let LookupKind::Graph(dir) = &lookup.kind else {
			return None;
		};
		if lookup.only
			|| lookup.expr.is_some()
			|| lookup.cond.is_some()
			|| lookup.split.is_some()
			|| lookup.group.is_some()
			|| lookup.order.is_some()
			|| lookup.limit.is_some()
			|| lookup.start.is_some()
			|| lookup.alias.is_some()
		{
			return None;
		}
		let mut tables = Vec::with_capacity(lookup.what.len());
		for subject in &lookup.what {
			let LogicalLookupSubject::Table {
				table,
				referencing_field: None,
			} = subject
			else {
				return None;
			};
			tables.push(table.clone());
		}
		Some(((*dir).into(), tables))
	}

	/// Compile a function call.
	async fn physical_call(&self, call: LogicalCall) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let LogicalCall {
			receiver,
			args,
		} = call;

		match receiver {
			Function::Normal(name) => {
				let registry = self.function_registry();

				if registry.is_index_function(&name) {
					return crate::exec::vm::out_of_line(|| self.plan_index_function(&name, args))
						.await;
				}

				// A `count(->edge)`-shaped projection over a pure graph
				// lookup is a degree question the adjacency storage can
				// often answer without materialising the edges: wrap the
				// normal expression in the degree fast path when the
				// count's answer is provably the raw adjacency cardinality.
				let degree = self.graph_degree_shape(&name, &args);

				let arguments = self.physical_args(args).await?;
				if registry.is_projection(&name) {
					let func_ctx = registry
						.get_projection(&name)
						.map(|f| f.required_context())
						.unwrap_or(crate::exec::ContextLevel::Database);
					Ok(Arc::new(ProjectionFunctionExec {
						name,
						arguments,
						func_required_context: func_ctx,
					}))
				} else {
					let func_ctx = registry
						.get(&name)
						.map(|f| f.required_context())
						.unwrap_or(crate::exec::ContextLevel::Root);
					let normal: Arc<dyn PhysicalExpr> = Arc::new(BuiltinFunctionExec {
						name,
						arguments,
						func_required_context: func_ctx,
						// Recorded so `eval::*` can continue the depth count from
						// here when it compiles its query string at runtime.
						plan_depth: self.current_depth(),
					});
					if let Some((direction, edge_tables)) = degree {
						return Ok(Arc::new(
							crate::exec::physical_expr::graph_degree::GraphDegreeExpr::new(
								direction,
								edge_tables,
								normal,
							)
							.map_err(|e| crate::err::EngineError::Internal(e.to_string()))?,
						));
					}
					Ok(normal)
				}
			}
			Function::Custom(name) => {
				let name_for_guard = name.clone();
				let arguments = self.physical_args(args).await?;
				let body_access_mode = self.resolve_custom_access_mode(&name).await;
				let body = crate::exec::vm::out_of_line(|| self.plan_function_body(&name)).await;
				Ok(Arc::new(UserDefinedFunctionExec {
					call: FunctionCall {
						name,
						// Recorded so a function body compiled on call continues
						// the depth count from here rather than resetting.
						plan_depth: self.current_depth(),
						body_access_mode,
						body,
						unchecked_body: None,
						permission: CompiledGuard::default(),
						planned_permission: crate::exec::vm::out_of_line(|| {
							crate::kvs::catalog_reads::sealed(
								false,
								self.planned_function_guard(&name_for_guard),
							)
						})
						.await,
					},
					arguments,
				}))
			}
			Function::Script(script) => {
				let arguments = self.physical_args(args).await?;
				Ok(Arc::new(JsFunctionExec {
					script,
					arguments,
					// Recorded so a script that re-enters SurrealQL starts from the
					// remaining budget rather than a full fresh one.
					plan_depth: self.current_depth(),
				}))
			}
			Function::Model(model) => {
				let arguments = self.physical_args(args).await?;
				#[cfg(feature = "ml")]
				let planned_permission = crate::exec::vm::out_of_line(|| {
					crate::kvs::catalog_reads::sealed(false, self.planned_model_guard(&model))
				})
				.await;
				Ok(Arc::new(ModelFunctionExec {
					model,
					arguments,
					#[cfg(feature = "ml")]
					permission: CompiledGuard::default(),
					#[cfg(feature = "ml")]
					planned_permission,
				}))
			}
			Function::Module(module, sub) => {
				let arguments = self.physical_args(args).await?;
				let writeable = self.resolve_module_writeable(&module, sub.as_deref()).await?;
				#[cfg(feature = "surrealism")]
				let storage = format!("mod::{module}");
				#[cfg(feature = "surrealism")]
				let planned_permission = crate::exec::vm::out_of_line(|| {
					crate::kvs::catalog_reads::sealed(false, self.planned_module_guard(&storage))
				})
				.await;
				Ok(Arc::new(SurrealismModuleExec {
					module,
					sub,
					arguments,
					writeable,
					#[cfg(feature = "surrealism")]
					permission: CompiledGuard::default(),
					#[cfg(feature = "surrealism")]
					planned_permission,
				}))
			}
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => {
				let arguments = self.physical_args(args).await?;
				let writeable = self
					.resolve_silo_writeable(&org, &pkg, major, minor, patch, sub.as_deref())
					.await?;
				#[cfg(feature = "surrealism")]
				let storage = format!("silo::{org}::{pkg}::<{major}.{minor}.{patch}>");
				#[cfg(feature = "surrealism")]
				let planned_permission = crate::exec::vm::out_of_line(|| {
					crate::kvs::catalog_reads::sealed(false, self.planned_module_guard(&storage))
				})
				.await;
				Ok(Arc::new(SiloModuleExec {
					org,
					pkg,
					major,
					minor,
					patch,
					sub,
					arguments,
					writeable,
					#[cfg(feature = "surrealism")]
					permission: CompiledGuard::default(),
					#[cfg(feature = "surrealism")]
					planned_permission,
				}))
			}
		}
	}

	/// Compile a list of argument expressions.
	pub(crate) async fn physical_args(
		&self,
		args: Vec<LogicalExpr>,
	) -> Result<Vec<Arc<dyn PhysicalExpr>>, Error> {
		let mut phys = Vec::with_capacity(args.len());
		for arg in args {
			phys.push(crate::exec::vm::out_of_line(|| self.physical_expr(arg)).await?);
		}
		Ok(phys)
	}

	/// Compile a closure literal.
	///
	/// The body is compiled here, in its own scope (see [`Self::own_scope`]),
	/// for what it does to the transaction and for the error compiling it
	/// raises, which the literal raises; the compilation is not kept, since
	/// every invocation runs the body as a unit ([`crate::exec::vm::closure`]).
	async fn physical_closure(
		&self,
		closure: LogicalClosure,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		// The closure's own parameters bind around its body, whatever they
		// are named, so a read of one there is the argument the call passes
		// and never the ambient context's value.
		let args = closure.args.iter().map(|(arg, _)| arg.as_str().to_owned());
		// A planner over a transaction, for a principal, compiles the body in
		// its own scope with it, planning the statements the body holds with
		// the query. With no transaction or no principal, nothing of the
		// creating statement — a table context, an index choice, a `VERSION`
		// stamp, a pre-planned function body — is read. The depth and cycle
		// guard are the enclosing planner's: the body is compiled within the
		// same statement, not by a re-entry.
		let planner = if self.txn.is_some() && self.auth.is_some() {
			self.own_scope().shadowing(args)
		} else {
			Planner::new(&self.ctx, self.function_registry)
				.with_depth(self.depth)
				.with_cycle_guard(self.cycle_guard.clone())
				.shadowing(args)
		};
		let body =
			crate::exec::vm::out_of_line(|| planner.physical_expr(closure.body.clone())).await?;
		Ok(Arc::new(ClosureExec::new(closure, body.access_mode())))
	}

	/// Compile an `IF … THEN … ELSE … END` expression. Every branch is
	/// compiled here; which one runs is decided per evaluation.
	async fn physical_if(&self, branches: LogicalIf) -> Result<Arc<dyn PhysicalExpr>, Error> {
		let LogicalIf {
			branches,
			otherwise,
		} = branches;
		let mut phys_branches = Vec::with_capacity(branches.len());
		for (condition, body) in branches {
			let cond_phys = crate::exec::vm::out_of_line(|| self.physical_expr(condition)).await?;
			let body_phys = crate::exec::vm::out_of_line(|| self.physical_expr(body)).await?;
			phys_branches.push((cond_phys, body_phys));
		}
		let otherwise = match otherwise {
			Some(else_expr) => {
				Some(crate::exec::vm::out_of_line(|| self.physical_expr(else_expr)).await?)
			}
			None => None,
		};
		Ok(Arc::new(IfElseExpr {
			branches: phys_branches,
			otherwise,
		}))
	}

	// ========================================================================
	// Record ID keys
	// ========================================================================

	/// Compile a record-id key for runtime evaluation.
	///
	/// Scalar key types (Number, String, Uuid, Generate) are mapped directly.
	/// Array and Object elements are compiled through `physical_expr()` so
	/// they can contain arbitrary expressions (function calls, params, etc.).
	/// Range bounds recurse through this method.
	fn physical_record_key(
		&self,
		key: LogicalRecordIdKey,
	) -> crate::exec::BoxFut<'_, Result<PhysicalRecordIdKey, Error>> {
		crate::exec::vm::out_of_line(|| async move {
			match key {
				LogicalRecordIdKey::Number(n) => Ok(PhysicalRecordIdKey::Number(n)),
				LogicalRecordIdKey::String(s) => Ok(PhysicalRecordIdKey::String(s)),
				LogicalRecordIdKey::Uuid(u) => Ok(PhysicalRecordIdKey::Uuid(u)),
				LogicalRecordIdKey::Generate(generator) => {
					Ok(PhysicalRecordIdKey::Generate(generator))
				}
				LogicalRecordIdKey::Array(items) => {
					Ok(PhysicalRecordIdKey::Array(self.physical_args(items).await?))
				}
				LogicalRecordIdKey::Object(entries) => {
					let mut phys = Vec::with_capacity(entries.len());
					for (key, value) in entries {
						let value =
							crate::exec::vm::out_of_line(|| self.physical_expr(value)).await?;
						phys.push((Strand::from(key), value));
					}
					Ok(PhysicalRecordIdKey::Object(phys))
				}
				LogicalRecordIdKey::Range(range) => {
					let start = self.physical_key_bound(range.start).await?;
					let end = self.physical_key_bound(range.end).await?;
					Ok(PhysicalRecordIdKey::Range {
						start,
						end,
					})
				}
			}
		})
	}

	/// Compile one bound of a record-id key range.
	async fn physical_key_bound(
		&self,
		bound: std::ops::Bound<LogicalRecordIdKey>,
	) -> Result<std::ops::Bound<Box<PhysicalRecordIdKey>>, Error> {
		match bound {
			std::ops::Bound::Unbounded => Ok(std::ops::Bound::Unbounded),
			std::ops::Bound::Included(key) => {
				Ok(std::ops::Bound::Included(Box::new(self.physical_record_key(key).await?)))
			}
			std::ops::Bound::Excluded(key) => {
				Ok(std::ops::Bound::Excluded(Box::new(self.physical_record_key(key).await?)))
			}
		}
	}
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
		.with_perms_suppressed(perms_suppressed);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	if !shadowed.is_empty() {
		planner = planner.shadowing(shadowed.iter().cloned());
	}
	// Boxed so the planner's state machine — over ten kilobytes, most of it the
	// SELECT pipeline compiler's locals — is allocated once rather than embedded
	// in, and copied through, every frame between the statement loop and here.
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
	plan_statement_with(op, ctx, registry, txn, auth, depth, speculation, perms_suppressed, &[])
		.await
}

/// [`plan_statement_on`] for a statement whose parameters `shadowed` names
/// are looked up when the program runs, never substituted into the plan: the
/// plan then serves every evaluation that binds them differently.
#[expect(
	clippy::too_many_arguments,
	reason = "the arguments of plan_statement_on and the names the plan leaves to the run"
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
) -> Result<Arc<dyn ExecOperator>, Error> {
	plan_statement_with(op, ctx, registry, txn, auth, depth, None, perms_suppressed, shadowed).await
}

/// A top-level statement of a request, planned when the request arrives
/// ([`plan_request_statement`]).
pub(crate) struct RequestStatementPlan {
	/// Where the statement was compiled to, or the error planning raised.
	pub(crate) entry: Result<crate::exec::vm::RequestEntry, Error>,
	/// Whether binding folded a call that reads the clock, so the plan holds
	/// the time it was planned at.
	pub(crate) binds_the_clock: bool,
}

/// Plan a top-level statement of a request when the request arrives, in the
/// transaction of the request's first planned statement, lowering it into the
/// request's program as `request` builds it.
///
/// `ctx` holds the session and the parameters the statements before it leave.
/// The executor plans here only a statement that runs in this transaction with
/// no statement before it that may have changed the catalog, so the plan
/// meets the catalog it was planned against.
pub(crate) async fn plan_request_statement(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	speculation: Option<Arc<crate::dbs::Options>>,
	request: &mut crate::exec::vm::RequestBuilder,
) -> RequestStatementPlan {
	if let Some(why) = crate::exec::logical::unplannable_write(&op) {
		return RequestStatementPlan {
			entry: Err(Error::Exec(ExecError::PlannerUnsupported {
				reason: why,
			})),
			binds_the_clock: false,
		};
	}
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, Arc::clone(&txn), ns, db);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	planner.speculate_with(speculation);
	let binds_the_clock = Box::pin(crate::exec::vm::binds_the_clock(&op, &planner)).await;
	let unbound = matches!(
		op,
		LogicalOp::Sequence(_) | LogicalOp::Foreach(_) | LogicalOp::Let(_) | LogicalOp::Eval(_)
	)
	.then(|| op.clone());
	let entry = match Box::pin(planner.bind(op)).await {
		Ok(bound) => Box::pin(request.plan(&planner, &bound, unbound.as_ref())).await,
		Err(e) => Err(e),
	};
	RequestStatementPlan {
		entry,
		binds_the_clock,
	}
}

/// The program of a request whose statements `request` lowered, taking
/// `steps`, planned in `txn` as its statements were.
pub(crate) fn finish_request(
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	request: crate::exec::vm::RequestBuilder,
	steps: Vec<crate::exec::vm::program::RequestStep>,
) -> Result<crate::exec::vm::request::CompiledRequest, Error> {
	let (ns, db) = session_ns_db(ctx);
	let planner = Planner::with_txn(ctx, registry, txn, ns, db);
	request.finish(&planner, steps)
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
) -> Result<Arc<dyn ExecOperator>, Error> {
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
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, txn, ns, db)
		.with_depth(depth)
		.with_perms_suppressed(perms_suppressed);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	if !shadowed.is_empty() {
		planner = planner.shadowing(shadowed.iter().cloned());
	}
	planner.speculate_with(speculation);
	let within =
		crate::exec::vm::out_of_line(|| crate::exec::vm::catalog_within(&planner, &op, shadowed))
			.await;
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
	let bound = crate::exec::vm::out_of_line(|| planner.bind(op)).await?;
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

/// A function body bound for a call and the planner that compiles it
/// ([`Planner::prepare_function_body`]).
pub(crate) struct PreparedBody<'ctx> {
	/// The planner the body is compiled with: the call's principal capped by
	/// the definition's `AUTH LIMIT`, one re-entry deeper, with the arguments
	/// shadowed.
	pub(crate) planner: Planner<'ctx>,
	/// The body, bound by [`Self::planner`]; as written, when
	/// [`Self::specialised`].
	pub(crate) bound: LogicalOp,
	/// The definition the body was bound from.
	pub(crate) frame: FunctionFrame,
	/// Whether the body is left unbound, to be compiled for the values each
	/// call's arguments hold ([`Planner::specialised_snapshot`]).
	pub(crate) specialised: bool,
}

/// Bind `op`, a statement nested in another, with `planner`, refusing the
/// forms `plan_statement_inner` declines.
pub(crate) async fn bind_statement(
	planner: &Planner<'_>,
	op: LogicalOp,
) -> Result<LogicalOp, Error> {
	// As `plan_statement_inner` declines them, and for the same reasons.
	if let Some(why) = crate::exec::logical::unplannable_write(&op) {
		return Err(Error::Exec(ExecError::PlannerUnsupported {
			reason: why,
		}));
	}
	// A planner that plans what follows a block's schema statements
	// late-bound resolves the catalog and the session's names those
	// statements leave when each runs; any other binds the statement against
	// the catalog it finds, so a schema statement nested in it is one the
	// operator tree's compilation would not see the effect of.
	if !planner.binds_late_after_schema()
		&& let Some(why) = crate::exec::logical::unplannable_context_change(&op)
	{
		return Err(Error::Exec(ExecError::PlannerUnsupported {
			reason: why,
		}));
	}
	crate::exec::vm::out_of_line(|| planner.bind(op)).await
}

/// Compile one scalar from inside a running query.
///
/// The runtime re-entry for a scalar: a dynamic source resolved per statement,
/// a `DEFINE PARAM` value, a stored permission predicate. The planner takes
/// `txn` when the caller has one, so a nested `SELECT` inside the scalar can
/// resolve its access path at compile time; without one, sources resolve at
/// execution time. `depth` continues the count from the node that re-entered
/// (see [`Planner::with_depth`]).
pub(crate) async fn compile_expr(
	expr: LogicalExpr,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Option<Arc<crate::kvs::Transaction>>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	let compiled =
		expr_planner(ctx, registry, txn, auth, depth).physical_expr(expr.clone()).await?;
	Ok(crate::exec::scalar_unit::UnitClause::of(expr, compiled))
}

/// [`compile_expr`] as the operator tree's compilation alone, which the
/// oracle the VM's tests compare against evaluates.
#[cfg(test)]
pub(crate) async fn compile_expr_tree(
	expr: LogicalExpr,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Option<Arc<crate::kvs::Transaction>>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	expr_planner(ctx, registry, txn, auth, depth).physical_expr(expr).await
}

/// [`compile_expr`] for code the compile cache keeps and serves to every
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
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	let compiled = expr_planner(exec.ctx(), exec.function_registry(), exec.try_txn(), auth, depth)
		.with_perms_suppressed(exec.root().perms_suppressed)
		.reading_session_values()
		.shadowing(bound.iter().cloned())
		.physical_expr(expr.clone())
		.await?;
	Ok(crate::exec::scalar_unit::UnitClause::of(expr, compiled))
}

/// The planner [`compile_expr`] compiles with.
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

/// Returns `true` if the binary operator is eligible for `SimpleBinaryOp` optimisation.
///
/// Only comparison and containment operators are eligible — these take `(&Value, &Value)`
/// and produce a boolean result. Operators that produce non-boolean results (arithmetic,
/// ranges), require short-circuit logic (And, Or, NullCoalescing), or need special index
/// context (Matches, NearestNeighbor) are excluded.
pub(crate) fn is_simple_binary_eligible(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
	)
}

/// The namespace and database the session is using, when it named them.
fn session_ns_db(ctx: &FrozenContext) -> (Option<String>, Option<String>) {
	let read = |key: &str| {
		ctx.value("session").and_then(|v| v.as_object()).and_then(|o| o.get(key)).and_then(|v| {
			match v {
				Value::String(s) => Some(s.as_str().to_owned()),
				_ => None,
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
		LogicalLiteral::Integer(x) => Value::Number(Number::Int(x)),
		LogicalLiteral::Decimal(x) => Value::Number(Number::Decimal(x)),
		LogicalLiteral::String(x) => Value::String(x),
		LogicalLiteral::Bytes(x) => Value::Bytes(x),
		LogicalLiteral::Regex(x) => Value::Regex(x),
		LogicalLiteral::Duration(x) => Value::Duration(x),
		LogicalLiteral::Datetime(x) => Value::Datetime(x),
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

#[cfg(test)]
mod planner_tests {
	use super::*;
	use crate::ctx::Context;
	use crate::expr::logical::lower;

	/// Lower a statement and compile it through a txn-less planner.
	async fn plan(src: &str) -> Arc<dyn ExecOperator> {
		let expr = crate::syn::expr(src).expect("statement should parse");
		let op = lower(expr).expect("statement should have a logical form");
		let ctx = Arc::new(Context::new_test());
		let registry = FunctionRegistry::with_builtins();
		Planner::new(&ctx, &registry).plan_logical(op).await.expect("Planning failed")
	}

	/// A mutation compiles to one write operator holding its per-record
	/// orders. Whether a form runs is a property of the orders it names — a
	/// form is declined when an order it names holds a stage with no body —
	/// and not of the statement kind, so nothing here may refuse a mutation
	/// by kind.
	#[tokio::test]
	async fn a_mutation_compiles_to_a_write_operator() {
		let plan = plan("CREATE person SET a = 1").await;
		assert_eq!(plan.name(), "Write");
		assert_eq!(plan.access_mode(), crate::exec::AccessMode::ReadWrite);
	}

	/// A mutation in scalar position compiles with the statement holding it,
	/// and carries its access mode up: a read holding a write is a write, and
	/// the executor selects the transaction from that answer.
	#[tokio::test]
	async fn a_mutation_nested_in_a_read_widens_the_access_mode() {
		let plan = plan("SELECT * FROM t WHERE a IN (CREATE u).id").await;
		assert_eq!(plan.access_mode(), crate::exec::AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn a_static_collection_compiles_to_one_literal() {
		let ctx = Arc::new(Context::new_test());
		let registry = FunctionRegistry::with_builtins();
		let planner = Planner::new(&ctx, &registry);
		let expr = crate::expr::logical::lower_expr(
			crate::syn::expr("[1, { a: 'b' }, person:one]").expect("should parse"),
		)
		.expect("should lower");
		let phys = planner.physical_expr(expr).await.expect("should compile");
		assert_eq!(phys.name(), "Literal");

		// A parameter inside the collection keeps it a per-row expression.
		let expr =
			crate::expr::logical::lower_expr(crate::syn::expr("[1, $x]").expect("should parse"))
				.expect("should lower");
		let phys = planner.physical_expr(expr).await.expect("should compile");
		assert_eq!(phys.name(), "ArrayLiteral");
	}
}
