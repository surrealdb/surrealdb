//! Query planner for the streaming executor.
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
//! - [`Planner::physical_expr`] compiles a scalar. A [`LogicalExpr::Subquery`] compiles its plan
//!   through [`Planner::plan_logical`] and wraps it in a [`ScalarSubquery`].
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
mod fold;
mod path;
// GQL `MATCH` planning (`plan_match`).
mod match_plan;
pub(crate) mod row_scope;
mod select;
mod source;
pub(crate) mod util;

use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use cycle_guard::CycleGuard;
use surrealdb_strand::Strand;

use crate::ctx::FrozenContext;
use crate::dbs::NewPlannerStrategy;
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::record_id::PhysicalRecordIdKey;
use crate::exec::physical_expr::{
	ArrayLiteral, BinaryOp, BuiltinFunctionExec, ClosureCallExec, ClosureExec, ControlFlowExpr,
	ControlFlowKind, IfElseExpr, JsFunctionExec, Literal as PhysicalLiteral, ModelFunctionExec,
	ObjectLiteral, Param, PostfixOp, ProjectionFunctionExec, RecordIdExpr, ScalarSubquery,
	SetFieldExpr, SetLiteral, SiloModuleExec, SurrealismModuleExec, UnaryOp,
	UserDefinedFunctionExec,
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

pub struct Planner<'ctx> {
	/// The frozen context containing query parameters, capabilities, and session info.
	ctx: &'ctx FrozenContext,
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
	/// planner it spawns for a `SELECT`. Mirrors which MATCHES expressions the
	/// legacy planner registers on a table's `QueryExecutor` so
	/// [`MatchesOp`](crate::exec::physical_expr::MatchesOp) can reproduce the
	/// legacy evaluate/error/false decision per row. `None` outside SELECT
	/// planning (RETURN, LET, the literal record-id fast path, …), where the
	/// legacy executor has no query planner and MATCHES evaluates to `false`.
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
	/// Re-entry nesting depth for this planner, the streaming engine's analogue
	/// of the legacy executor's `Options::dive` (and bounded by the same
	/// `max_computation_depth`).
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
	/// growing the native stack without limit (the legacy path can't overflow — it
	/// runs on a heap `TreeStack`).
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
}

/// One function body under compilation on a planner's [`Planner::function_path`].
#[derive(Clone, Debug)]
struct FunctionFrame {
	/// The function's name, without its `fn::` prefix.
	name: String,
	/// The content hash of the definition being compiled
	/// ([`FunctionSnapshot`](crate::kvs::cache::ds::FunctionSnapshot)).
	definition: u64,
	/// The definition being compiled; see [`PlannedBody::compiled_from`].
	compiled_from: Arc<crate::catalog::FunctionDefinition>,
	/// Where the compiled plan is published for the recursive edges inside it.
	body: Arc<crate::exec::physical_expr::RecursiveBody>,
}

impl<'ctx> Planner<'ctx> {
	/// Declines planning when the named table is a lightweight relation:
	/// its edges have no record range, so every streaming scan shape over
	/// it is wrong-empty. The fallback executor's collectors enumerate the
	/// edges from the relation's IN tables' adjacency instead. Callable
	/// only where a static table name is known; runtime-resolved scans
	/// perform the equivalent check at execute time and fail closed. A
	/// catalog read failure also declines rather than failing open: the
	/// fallback executor re-reads the catalog itself, surfacing a
	/// persistent datastore error and executing correctly a transient one.
	pub(crate) async fn decline_lightweight_table(
		&self,
		table: &surrealdb_strand::TableName,
	) -> Result<(), crate::exec::Error> {
		use crate::catalog::providers::TableProvider;
		if let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) {
			let def = match txn.get_tb_by_name(ns, db, table, None).await {
				Ok(def) => def,
				Err(_) => {
					return Err(crate::exec::Error::PlannerUnsupported(
						"the table catalog could not be read at plan time".to_owned(),
					));
				}
			};
			if def.is_some_and(|def| {
				crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some()
			}) {
				return Err(crate::exec::Error::PlannerUnsupported(
					"lightweight relations are scanned by the fallback executor".to_owned(),
				));
			}
		}
		Ok(())
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
	pub fn new(ctx: &'ctx FrozenContext, registry: &'ctx FunctionRegistry) -> Self {
		Self {
			ctx,
			function_registry: registry,
			txn: None,
			ns: None,
			db: None,
			version: None,
			plan_time_version: PlanTimeVersion::Current,
			auth: None,
			cycle_guard: CycleGuard::default(),
			ns_db_ids_cache: tokio::sync::OnceCell::new(),
			matches_scope: None,
			matches_context: None,
			knn_context: None,
			function_path: Vec::new(),
			depth: 0,
			shadowed_params: Arc::from(Vec::new()),
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
		ctx: &'ctx FrozenContext,
		registry: &'ctx FunctionRegistry,
		txn: Arc<crate::kvs::Transaction>,
		ns: Option<String>,
		db: Option<String>,
	) -> Self {
		Self {
			ctx,
			function_registry: registry,
			txn: Some(txn),
			ns,
			db,
			version: None,
			plan_time_version: PlanTimeVersion::Current,
			auth: None,
			cycle_guard: CycleGuard::default(),
			ns_db_ids_cache: tokio::sync::OnceCell::new(),
			matches_scope: None,
			matches_context: None,
			knn_context: None,
			function_path: Vec::new(),
			depth: 0,
			shadowed_params: Arc::from(Vec::new()),
		}
	}

	/// Runtime-only fallback constructor: build a planner from an already-active
	/// [`DatabaseContext`] (and therefore from a live transaction + ns/db). This
	/// is **not** the canonical entry point — top-level planning goes through
	/// [`plan_statement_inner`], which routes via [`Planner::with_txn`].
	/// `for_database` exists for the narrow case of a scan operator that hits a
	/// cache miss mid-execution and has to compile a permission /
	/// computed-field body without a parent planner to inherit from.
	///
	/// The cycle guard starts empty. Same-table recursion at this point is
	/// broken by table-context resolution pushing onto this fresh guard — no
	/// separate runtime mechanism applies.
	#[inline]
	pub(crate) fn for_database(
		ctx: &'ctx FrozenContext,
		txn: Arc<crate::kvs::Transaction>,
		db_ctx: &'ctx crate::exec::DatabaseContext,
	) -> Self {
		Self::with_txn(
			ctx,
			&db_ctx.ns_ctx.root.function_registry,
			txn,
			Some(db_ctx.ns_name().to_owned()),
			Some(db_ctx.db_name().to_owned()),
		)
	}

	/// Set the VERSION expression for propagation to graph edge scans.
	pub fn with_version(mut self, version: Option<Arc<dyn PhysicalExpr>>) -> Self {
		self.version = version;
		self
	}

	/// Set the version the planner's catalog reads are made at; see
	/// [`PlanTimeVersion`].
	pub(crate) fn with_plan_time_version(mut self, version: PlanTimeVersion) -> Self {
		self.plan_time_version = version;
		self
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
					self.ctx,
					self.txn.as_ref(),
					self.ns_db_ids().await,
					util::SELECT_ITERATION_PARAMS,
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

	/// Get a clone of the cycle guard (cheap `Arc` bump). Pass to
	/// `with_cycle_guard` on a child planner to share the same set of
	/// in-progress tables.
	#[inline]
	pub(crate) fn cycle_guard(&self) -> CycleGuard {
		self.cycle_guard.clone()
	}

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
		self.bind_scoped(op, util::SELECT_ITERATION_PARAMS).await
	}

	/// [`Self::bind`] with `row_scoped` as the names the plan binds per row
	/// or per call, which binding never substitutes a value for. A statement
	/// binds the row parameters; a function body binds its arguments too.
	async fn bind_scoped(&self, op: LogicalOp, row_scoped: &[&str]) -> Result<LogicalOp, Error> {
		let bind_ctx = crate::exec::logical::BindContext {
			ctx: self.ctx,
			txn: self.txn.clone(),
			ns_db: self.ns_db_ids().await,
			auth: self.auth.clone(),
			row_scoped,
			registry: self.function_registry,
		};
		crate::exec::logical::bind(op, &bind_ctx).await
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
	/// A name the body reads that is bound nowhere the planner can see is
	/// left a runtime lookup rather than resolved to `NONE`: the statement
	/// may bind it with a `LET` before the call. A body that reads an
	/// argument in a predicate or a source is left to the runtime as well,
	/// which binds the argument as the call's value and so gives the access
	/// path a literal to answer; a plan compiled here would carry a name
	/// there, which no index answers.
	async fn plan_function_body(
		&self,
		name: &str,
	) -> Option<crate::exec::physical_expr::PlannedBody> {
		use crate::exec::physical_expr::{BodyPlan, PlannedBody, RecursiveBody};
		use crate::kvs::cache::ds::FunctionSnapshot;

		if self.version.is_some() {
			return None;
		}
		if let Some(frame) = self.function_path.iter().find(|f| f.name == name) {
			return Some(PlannedBody {
				definition: frame.definition,
				compiled_from: Arc::clone(&frame.compiled_from),
				plan: BodyPlan::Recursive(Arc::clone(&frame.body)),
			});
		}
		let (Some(txn), Some((ns, db))) = (&self.txn, self.ns_db_ids().await) else {
			return None;
		};
		if !self.ctx.get_capabilities().allows_function_name(&format!("fn::{name}")) {
			return None;
		}
		let cache = self.ctx.get_cache();
		let snapshot =
			match FunctionSnapshot::for_function(cache.as_deref(), txn, ns, db, name).await {
				Ok(snapshot) => snapshot?,
				Err(e) => {
					tracing::debug!("fn::{name} could not be read at plan time: {e:#}");
					return None;
				}
			};
		let definition = snapshot.definition();
		let body = snapshot.body().ok()?.clone();
		let limit = crate::iam::AuthLimit::try_from(&definition.auth_limit).ok()?;

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
		let mut row_scoped: Vec<&str> = util::SELECT_ITERATION_PARAMS.to_vec();
		row_scoped.extend(definition.args.iter().map(|(arg, _)| arg.as_str()));
		let mut unbound = Vec::new();
		for param in &referenced {
			if row_scoped.contains(&param.as_str()) {
				continue;
			}
			if let util::ParamBinding::Unbound =
				util::bind_param(param, self.ctx, Some(txn), Some((ns, db)), &row_scoped).await
			{
				unbound.push(param.as_str());
			}
		}
		row_scoped.extend(unbound);

		let mut planner =
			self.shadowing(definition.args.iter().map(|(arg, _)| arg.as_str().to_owned()));
		planner.version = None;
		planner.plan_time_version = PlanTimeVersion::Current;
		planner.matches_scope = None;
		planner.matches_context = None;
		planner.knn_context = None;
		planner.depth = self.depth + 1;
		planner.auth = self.auth.as_ref().map(|auth| Arc::new(auth.new_limited(&limit)));
		let frame = FunctionFrame {
			name: name.to_owned(),
			definition: snapshot.content_hash(),
			compiled_from: Arc::clone(snapshot.definition()),
			body: RecursiveBody::new(),
		};
		planner.function_path.push(frame.clone());
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
		match Box::pin(planner.plan_logical(bound)).await {
			Ok(plan) => {
				frame.body.resolve(&plan);
				Some(PlannedBody {
					definition: frame.definition,
					compiled_from: frame.compiled_from,
					plan: BodyPlan::Compiled(plan),
				})
			}
			Err(e) => {
				tracing::debug!("fn::{name} body is left for the runtime to plan: {e:#}");
				None
			}
		}
	}

	/// A planner for a body compiled in its own scope: a closure body.
	///
	/// Shares the context, transaction, principal, cycle guard and re-entry
	/// depth, and carries none of the enclosing statement's planning scope — no
	/// MATCHES registrations, no KNN slot, no `VERSION`, no `FOR UPDATE`. A
	/// closure body is evaluated wherever the closure is finally called, which
	/// is not necessarily the statement that wrote it, so it must not inherit
	/// what that statement registered.
	fn own_scope(&self) -> Planner<'ctx> {
		Planner {
			ctx: self.ctx,
			function_registry: self.function_registry,
			txn: self.txn.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			version: None,
			plan_time_version: PlanTimeVersion::Current,
			auth: self.auth.clone(),
			cycle_guard: self.cycle_guard.clone(),
			ns_db_ids_cache: self.ns_db_ids_cache.clone(),
			matches_scope: None,
			matches_context: None,
			knn_context: None,
			function_path: self.function_path.clone(),
			depth: self.depth,
			shadowed_params: Arc::clone(&self.shadowed_params),
		}
	}

	/// This planner with `names` added to [`Self::shadowed_params`]: the
	/// planner for the body of a `FOR`, a closure or a user-defined function,
	/// which binds those names around it.
	///
	/// Every other part of the planning scope is carried through unchanged —
	/// a `FOR` inside a `VERSION` is still a versioned read — except the
	/// catalog id cache, which is a cache and is re-filled on demand.
	fn shadowing(&self, names: impl IntoIterator<Item = String>) -> Planner<'ctx> {
		let mut shadowed: Vec<String> = self.shadowed_params.to_vec();
		shadowed.extend(names);
		Planner {
			ctx: self.ctx,
			function_registry: self.function_registry,
			txn: self.txn.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			version: self.version.clone(),
			plan_time_version: self.plan_time_version,
			auth: self.auth.clone(),
			cycle_guard: self.cycle_guard.clone(),
			ns_db_ids_cache: tokio::sync::OnceCell::new(),
			matches_scope: self.matches_scope.clone(),
			matches_context: self.matches_context.clone(),
			knn_context: self.knn_context.clone(),
			function_path: self.function_path.clone(),
			depth: self.depth,
			shadowed_params: Arc::from(shadowed),
		}
	}

	/// Whether an enclosing `FOR`, closure or user-defined function binds
	/// `name`, so that a read of it is not the ambient context's value.
	pub(crate) fn param_is_shadowed(&self, name: &str) -> bool {
		self.shadowed_params.iter().any(|shadowed| shadowed == name)
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
	async fn resolve_custom_access_mode(&self, name: &str) -> crate::exec::AccessMode {
		use crate::catalog::providers::DatabaseProvider;
		use crate::exec::AccessMode;

		let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return AccessMode::ReadWrite;
		};
		let version = match self.plan_time_version {
			PlanTimeVersion::Current => None,
			PlanTimeVersion::At(stamp) => Some(stamp),
			PlanTimeVersion::Unknown => return AccessMode::ReadWrite,
		};
		// A UDF call reaches its callee's body through the call graph; the call
		// itself contributes exactly that one edge.
		let mut facts = crate::expr::function_facts::FunctionFacts::default();
		facts.calls.insert(name.to_owned());

		let resolved = async {
			let db_def = txn.get_db_by_name(ns, db, version).await?.ok_or_else(|| {
				anyhow::anyhow!("database `{ns}/{db}` not found while resolving `fn::{name}`")
			})?;
			crate::fnc::mutability::resolve_mutability(
				txn,
				db_def.namespace_id,
				db_def.database_id,
				version,
				&facts,
			)
			.await
		}
		.await;
		match resolved {
			Ok(r) if !r.possibly_writes() => AccessMode::ReadOnly,
			// A write is possible, or the snapshot could not be read: keep the
			// safe over-approximation.
			Ok(_) | Err(_) => AccessMode::ReadWrite,
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
		let mut plan_ctx = Context::new_child(self.ctx);
		plan_ctx.set_transaction(Arc::clone(txn));
		let frozen = plan_ctx.freeze();
		let sig = crate::legacy::module_executable_signature(
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
		let mut plan_ctx = Context::new_child(self.ctx);
		plan_ctx.set_transaction(Arc::clone(txn));
		let frozen = plan_ctx.freeze();
		let sig = crate::legacy::silo_executable_signature(
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
	pub(crate) async fn plan_logical(&self, op: LogicalOp) -> Result<Arc<dyn ExecOperator>, Error> {
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
		// Reject if this planner's re-entry depth (carried in from the boundary
		// that started it) has already exceeded `max_computation_depth`. The depth
		// recorded onto `eval` / UDF / JS nodes below lets a runtime re-entry
		// continue the count from here.
		self.check_depth()?;

		if let Some(value) = static_value(&expr) {
			return Ok(Arc::new(PhysicalLiteral(value)));
		}

		match expr {
			LogicalExpr::Literal(lit) => Ok(Arc::new(PhysicalLiteral(literal_value(lit)))),
			LogicalExpr::Array(items) => Ok(Arc::new(ArrayLiteral {
				elements: self.physical_args(items).await?,
			})),
			LogicalExpr::Set(items) => Ok(Arc::new(SetLiteral {
				elements: self.physical_args(items).await?,
			})),
			LogicalExpr::Object(entries) => {
				let mut phys_entries = Vec::with_capacity(entries.len());
				for (key, value) in entries {
					let value = Box::pin(self.physical_expr(value)).await?;
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
			LogicalExpr::Param(p) => Ok(Arc::new(Param::new(p.into_strand()))),
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
				let base = Box::pin(self.physical_expr(*base)).await?;
				let value = Box::pin(self.physical_expr(*value)).await?;
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
			} => Box::pin(self.physical_binary_expr(*left, op, *right)).await,
			LogicalExpr::Prefix {
				op,
				expr,
			} => Box::pin(self.physical_prefix_expr(op, *expr)).await,
			LogicalExpr::Postfix {
				expr,
				op,
			} => Box::pin(self.physical_postfix_expr(*expr, op)).await,
			LogicalExpr::Invoke {
				target,
				args,
			} => Box::pin(self.physical_invoke(*target, args)).await,

			LogicalExpr::Call(call) => Box::pin(self.physical_call(*call)).await,
			LogicalExpr::Closure(closure) => Box::pin(self.physical_closure(*closure)).await,
			LogicalExpr::If(branches) => Box::pin(self.physical_if(*branches)).await,

			LogicalExpr::Break => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Break,
				inner: None,
			})),
			LogicalExpr::Continue => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Continue,
				inner: None,
			})),
			LogicalExpr::Throw(e) => {
				let inner = Box::pin(self.physical_expr(*e)).await?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Throw,
					inner: Some(inner),
				}))
			}

			// A statement in scalar position: its plan is compiled like any
			// other and evaluated per row through a scalar subquery. `LET`
			// binds a name and yields nothing, so it is a statement only; the
			// same text is refused by the legacy engine with the same error.
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
				let plan = Box::pin(self.plan_logical(*op)).await?;
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
		// answer `false` for rows no legacy `QueryExecutor` would have seen and
		// `NoIndexFoundForMatch` for the rest — the same two outcomes the legacy
		// executor reaches for them, since its tree registers neither shape.
		if let BinaryOperator::Matches(ref matches_op) = op {
			// Determine whether this exact expression is registered in the
			// enclosing SELECT's WHERE condition (legacy executor parity — see
			// `MatchesScope`). The lookup key is the node as written; the
			// allowlist contains both the original and the bound condition
			// forms, so both the projection-side and residual-cond-side
			// compilations of the same source expression hit it. The bind
			// passes substitute a statement-scoped parameter in a predicate
			// but not in a projection, so a projection whose query is a
			// parameter is also looked up with the parameter's value in place,
			// the form the WHERE holds after binding.
			let (registered, executor_tables) = match &self.matches_scope {
				Some(scope) => {
					let key = |right: LogicalExpr| LogicalExpr::Binary {
						left: Box::new(left.clone()),
						op: BinaryOperator::Matches(matches_op.clone()),
						right: Box::new(right),
					};
					let registered = scope.allowlist.contains(&key(right.clone()))
						|| match &right {
							LogicalExpr::Param(param) => self
								.ctx
								.value(param.as_str())
								.cloned()
								.and_then(try_value_to_logical_expr)
								.is_some_and(|value| scope.allowlist.contains(&key(value))),
							_ => false,
						};
					(registered, scope.executor_tables.clone())
				}
				None => (false, crate::exec::physical_expr::ExecutorTables::none()),
			};

			// The probe mirrors legacy `Tree::eval_matches_operator`: a path on
			// the left, and a right operand the legacy tree would see as a computed
			// node, rendered with `Value::to_raw_string()`. That keeps a bare
			// string literal as its own contents and renders every other value
			// the way the legacy analyzer receives it — `title @@ 42` searches
			// for the term `42`.
			let probe =
				|query: &Value, path: &LogicalExpr| crate::exec::physical_expr::MatchProbe {
					field: path.clone(),
					query: query.to_raw_string(),
				};
			let probe = match (&left, &right) {
				(path, LogicalExpr::Param(param)) if path.is_path() => {
					self.ctx.value(param.as_str()).map(|v| probe(v, path))
				}
				(path, other) if path.is_path() => static_value(other).map(|v| probe(&v, path)),
				_ => None,
			};

			// The right side is rendered from the resolved query when there is
			// one, so `ToSql` shows what was actually searched for rather than
			// the unresolved parameter.
			let right_source = match &probe {
				Some(probe) => {
					LogicalExpr::Literal(LogicalLiteral::String(Strand::from(probe.query.clone())))
				}
				None => right,
			};
			let left_phys = Box::pin(self.physical_expr(left)).await?;
			let right_phys = Box::pin(self.physical_expr(right_source)).await?;
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
		// without one — the legacy checker / missing-entry semantics. The
		// KnnContext is bound here because the execution context does not
		// carry it.
		if matches!(op, BinaryOperator::NearestNeighbor(_)) {
			let left_phys = Box::pin(self.physical_expr(left)).await?;
			let right_phys = Box::pin(self.physical_expr(right)).await?;
			return Ok(Arc::new(crate::exec::physical_expr::KnnMembershipOp::new(
				left_phys,
				right_phys,
				op,
				self.knn_context.clone(),
			)));
		}

		// All other binary operators
		let left_phys = Box::pin(self.physical_expr(left)).await?;
		let right_phys = Box::pin(self.physical_expr(right)).await?;

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
		// the streaming evaluator walks recursively at runtime. The parser's
		// `expr_recursion_limit` can sit above `max_computation_depth`, so a chain
		// in that window would slip past parsing and overflow at evaluation. Bound
		// the spine length here, iteratively (no recursion), against the
		// computation limit — offset by this planner's carried re-entry `depth` so
		// the bound is continuous across `eval`/UDF boundaries, matching the legacy
		// `Options::dive` accounting.
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
		let inner = Box::pin(self.physical_expr(expr)).await?;
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
		let inner = Box::pin(self.physical_expr(expr)).await?;
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
		let target = Box::pin(self.physical_expr(target)).await?;
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
	fn graph_degree_shape(
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
					return Box::pin(self.plan_index_function(&name, args)).await;
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
				let arguments = self.physical_args(args).await?;
				let body_access_mode = self.resolve_custom_access_mode(&name).await;
				let body = Box::pin(self.plan_function_body(&name)).await;
				Ok(Arc::new(UserDefinedFunctionExec {
					name,
					arguments,
					// Recorded so a function body compiled on call continues
					// the depth count from here rather than resetting.
					plan_depth: self.current_depth(),
					body_access_mode,
					body,
					permission: CompiledGuard::default(),
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
				Ok(Arc::new(ModelFunctionExec {
					model,
					arguments,
					#[cfg(feature = "ml")]
					permission: CompiledGuard::default(),
				}))
			}
			Function::Module(module, sub) => {
				let arguments = self.physical_args(args).await?;
				let writeable = self.resolve_module_writeable(&module, sub.as_deref()).await?;
				Ok(Arc::new(SurrealismModuleExec {
					module,
					sub,
					arguments,
					writeable,
					#[cfg(feature = "surrealism")]
					permission: CompiledGuard::default(),
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
			phys.push(Box::pin(self.physical_expr(arg)).await?);
		}
		Ok(phys)
	}

	/// Compile a closure literal.
	///
	/// The body is compiled here, in its own scope (see [`Self::own_scope`]),
	/// so a call evaluates a compiled body rather than compiling one per call.
	async fn physical_closure(
		&self,
		closure: LogicalClosure,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		// The body is compiled without this planner's transaction, namespace,
		// database, auth or version: the closure value is invoked by later
		// statements, each in a transaction of its own, so nothing of the
		// creating statement's catalog — a table context, an index choice, a
		// `VERSION` stamp, a pre-planned function body — may be baked into
		// it. Its sources resolve when it runs, in the invoking statement's
		// transaction and frame, and with no principal attached permission
		// enforcement is assumed. The depth and cycle guard are the enclosing
		// planner's: the body is compiled within the same statement, not by
		// a re-entry.
		// The closure's own parameters bind around its body, whatever they
		// are named, so a read of one there is the argument the call passes
		// and never the ambient context's value.
		let planner = Planner::new(self.ctx, self.function_registry)
			.with_depth(self.depth)
			.with_cycle_guard(self.cycle_guard.clone())
			.shadowing(closure.args.iter().map(|(arg, _)| arg.as_str().to_owned()));
		let body = Box::pin(planner.physical_expr(closure.body.clone())).await?;
		Ok(Arc::new(ClosureExec::new(closure, body)))
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
			let cond_phys = Box::pin(self.physical_expr(condition)).await?;
			let body_phys = Box::pin(self.physical_expr(body)).await?;
			phys_branches.push((cond_phys, body_phys));
		}
		let otherwise = match otherwise {
			Some(else_expr) => Some(Box::pin(self.physical_expr(else_expr)).await?),
			None => None,
		};
		Ok(Arc::new(IfElseExpr {
			branches: phys_branches,
			otherwise,
		}))
	}

	/// Compile an expression, treating a bare identifier as a string.
	///
	/// Used for `INFO FOR USER test` where `test` is a name, not a variable.
	pub(crate) async fn physical_expr_as_name(
		&self,
		expr: LogicalExpr,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		if let LogicalExpr::Field(name) = expr {
			return Ok(Arc::new(PhysicalLiteral(Value::String(name))));
		}

		if let LogicalExpr::Table(name) = expr {
			return Ok(Arc::new(PhysicalLiteral(Value::String(name.as_str().into()))));
		}

		Box::pin(self.physical_expr(expr)).await
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
		Box::pin(async move {
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
						let value = Box::pin(self.physical_expr(value)).await?;
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

/// Which engine runs a statement.
///
/// The choice is made from the statement and the session alone — no
/// transaction, no catalog — so it is decidable before any work begins. It is
/// not the last word: lowering can decline a statement accepted here
/// (`NoLogicalForm`), and so can the planner while compiling it
/// (`ExecError::PlannerUnsupported`). Callers must handle that late decline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Engine {
	/// Compiled into an operator tree and executed by the streaming engine.
	Streaming,
	/// Evaluated by the legacy `compute()` recursor.
	Legacy(LegacyReason),
}

/// Why a statement goes to the legacy engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyReason {
	/// The statement kind has no lowering and no planner support yet. This is
	/// the count that has to reach zero before the streaming engine is the
	/// only engine.
	UnplannedStatement,
	/// The session is pinned to the legacy engine
	/// (`NewPlannerStrategy::ComputeOnly`), whatever the statement is.
	Pinned,
}

/// Choose the engine for a top-level statement.
///
/// The statement kind is checked before the session setting, so a statement
/// the engine could never run reports that rather than the session's
/// preference.
///
/// A pre-filter, not the decision. It reads variant kinds only — nothing here
/// reads a scalar, so nothing here needs the expression layer — which is what
/// lets the executor choose from the surface statement before converting
/// anything. DML and DDL have no lowering, so there is nothing to try for
/// them; every other statement is decided by attempting the streaming path,
/// which can still decline while lowering or compiling.
pub(crate) fn engine_for(expr: &crate::sql::Expr, strategy: NewPlannerStrategy) -> Engine {
	use crate::sql::Expr;
	// DML and DDL have no lowering and no planner support; they are evaluated
	// by the legacy engine, which owns the document pipeline they write
	// through.
	if matches!(
		expr,
		Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Insert(_)
			| Expr::Relate(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
	) {
		return Engine::Legacy(LegacyReason::UnplannedStatement);
	}
	if strategy == NewPlannerStrategy::ComputeOnly {
		return Engine::Legacy(LegacyReason::Pinned);
	}
	Engine::Streaming
}

/// Plan an already-lowered logical plan into an executable operator tree.
///
/// The entry for a front end that emits a plan rather than a statement, so
/// there is no lowering step and no legacy `compute()` to fall back to: the
/// plan is compiled or the query fails.
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
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, txn, ns, db).with_depth(depth);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	// Boxed so the planner's state machine — over ten kilobytes, most of it the
	// SELECT pipeline compiler's locals — is allocated once rather than embedded
	// in, and copied through, every frame between the statement loop and here.
	Box::pin(planner.plan_logical(op)).await
}

/// Bind a lowered statement for `txn` and compile it into an operator tree.
///
/// The executor lowers the surface statement; this binds the result for the
/// transaction and the caller's principal, then compiles the bound plan. It
/// is also the runtime re-entry for a statement compiled from inside a running
/// query — a user-defined function body — which seeds `depth` from the node
/// that called it so the count continues toward `max_computation_depth`.
pub(crate) async fn plan_statement_inner(
	op: LogicalOp,
	ctx: &FrozenContext,
	registry: &FunctionRegistry,
	txn: Arc<crate::kvs::Transaction>,
	auth: Option<Arc<crate::iam::Auth>>,
	depth: u32,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let (ns, db) = session_ns_db(ctx);
	let mut planner = Planner::with_txn(ctx, registry, txn, ns, db).with_depth(depth);
	if let Some(auth) = auth {
		planner = planner.with_auth(auth);
	}
	// Boxed for the same reason as `plan_logical_inner`: the bind passes and the
	// planner together are the bulk of a statement's future, and every enclosing
	// async frame would otherwise carry and copy them.
	let bound = Box::pin(planner.bind(op)).await?;
	Box::pin(planner.plan_logical(bound)).await
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
	planner.physical_expr(expr).await
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
fn static_value(expr: &LogicalExpr) -> Option<Value> {
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

	#[tokio::test]
	async fn test_planner_creates_let_operator() {
		let plan = plan("LET $x = 42").await;
		assert_eq!(plan.name(), "Let");
		assert!(plan.mutates_context());
	}

	#[tokio::test]
	async fn test_planner_creates_scalar_plan() {
		let plan = plan("42").await;
		assert_eq!(plan.name(), "Expr");
		assert!(plan.output_shape().is_scalar());
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
