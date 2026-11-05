//! Hierarchical execution contexts for the engine.
//!
//! The context hierarchy provides type-safe access to resources at different levels:
//! - `RootContext`: Always available, wraps a FrozenContext with auth + session
//! - `NamespaceContext`: Root + namespace definition
//! - `DatabaseContext`: Namespace + database definition
//!
//! `FrozenContext` is the single source of truth for parameters, transactions,
//! capabilities, and every other context field. `RootContext` adds only the fields
//! that are not trivially accessible from `FrozenContext` (auth, session info).
//!
//! Operators declare their minimum required context level via `ExecutionPlan::required_context()`,
//! and the executor validates requirements before execution begins.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_strand::Strand;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::catalog::{DatabaseDefinition, IndexDefinition, NamespaceDefinition, TableDefinition};
use crate::ctx::{Binding, Context, FrozenContext};
use crate::dbs::{Capabilities, Options};
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::eval_context::ComputingRecord;
use crate::exec::function::FunctionRegistry;
use crate::exec::permission::action_is_checked;
use crate::expr::Base;
use crate::iam::{Action, Auth, ResourceKind};
use crate::kvs::index::filter_online_indexes;
use crate::kvs::{Datastore, Transaction};
use crate::val::{Datetime, TableName, Value};

/// Parameters passed to queries (e.g., `$param` values).
pub(crate) type Parameters = HashMap<Strand, Arc<Value>>;

/// A change a statement's program makes to the request's session, which the executor owns.
///
/// The program does the work that belongs to the statement's transaction (target evaluation,
/// authorisation, catalog creation) and records an effect only after the part of the statement it
/// stands for has succeeded. The executor applies the effects in record order
/// (`Executor::apply_session_effects`) after the program ends, whether it returned or raised.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SessionEffect {
	/// `OPTION IMPORT` or `OPTION FORCE`: `what` is the value the statement sets.
	Option {
		name: Strand,
		what: bool,
	},
	/// `USE`: the namespace and the database one level of the statement selected, each recorded
	/// once the catalog work of its level has succeeded, namespace first.
	Use {
		ns: Option<Strand>,
		db: Option<Strand>,
	},
	/// A session `LET`: the coerced value the request binds to `name` for the statements after.
	Let {
		name: Strand,
		value: Arc<Value>,
	},
}

/// Where a statement's program records the [`SessionEffect`]s it makes.
///
/// A sink is installed only on the context of a top-level statement of the request
/// (`Executor::statement_context`). Every context derived to run other code (a function body, a
/// closure, a subquery, a table event, a subscription, `eval::*`) holds none, so a program that
/// runs there cannot switch the caller's namespace or database, set its options or bind a
/// request parameter.
#[derive(Debug, Default)]
pub(crate) struct SessionSink(parking_lot::Mutex<Vec<SessionEffect>>);

impl SessionSink {
	/// Records `effect` after the ones recorded so far. The lock is never held across an await.
	pub(crate) fn record(&self, effect: SessionEffect) {
		self.0.lock().push(effect);
	}

	/// Takes the recorded effects, in the order they were recorded, leaving the sink empty.
	pub(crate) fn take(&self) -> Vec<SessionEffect> {
		std::mem::take(&mut *self.0.lock())
	}
}

/// Shared cache of [`FieldState`](crate::exec::operators::scan::pipeline::FieldState)
/// keyed by `(table_name, check_perms)`.
pub(crate) type FieldStateCache = Arc<
	tokio::sync::RwLock<
		HashMap<(TableName, bool), Arc<crate::exec::operators::scan::pipeline::FieldState>>,
	>,
>;

/// The minimum context level required by an execution plan.
///
/// Used for pre-flight validation: the executor checks that the current session
/// has at least the required level before execution begins.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub enum ContextLevel {
	/// No namespace or database required (e.g., `INFO FOR ROOT`)
	#[default]
	Root = 0,
	/// Namespace must be selected (e.g., `INFO FOR NS`)
	Namespace = 1,
	/// Both namespace and database must be selected (e.g., `SELECT * FROM table`)
	Database = 2,
}

impl ContextLevel {
	pub fn short_name(self) -> &'static str {
		match self {
			Self::Root => "Rt",
			Self::Namespace => "Ns",
			Self::Database => "Db",
		}
	}
}

/// Session information for context-aware functions.
///
/// This contains session data that can be accessed by functions like
/// `session::ns()`, `session::db()`, `session::id()`, etc.
///
/// String-shaped fields are stored as [`Strand`] so that extracting them from the session `Value`
/// and re-emitting them via `session::ns()` / `session::db()` / etc. is a move rather than a
/// `Strand -> String -> Strand` round-trip on every call.
#[derive(Debug, Clone, Default)]
pub(crate) struct SessionInfo {
	/// The currently selected namespace
	pub ns: Option<Strand>,
	/// The currently selected database
	pub db: Option<Strand>,
	/// The current session ID
	pub id: Option<Uuid>,
	/// The current connection IP address
	pub ip: Option<Strand>,
	/// The current connection origin
	pub origin: Option<Strand>,
	/// The current access method
	pub ac: Option<Strand>,
	/// The current record authentication data
	pub rd: Option<Value>,
	/// The current authentication token
	pub token: Option<Value>,
	/// The current expiration time of the session
	pub exp: Option<Datetime>,
}

impl SessionInfo {
	/// The session a [`FrozenContext`] carries as its `$session` value, or
	/// `None` when the context has no session bound.
	///
	/// `$session` is an object keyed by the session paths (`ns`, `db`, `id`,
	/// `ip`, `or`, `ac`, `rd`, `tk`); a field of the wrong shape reads as
	/// absent. The expiry is not part of that object and is always `None`
	/// here.
	pub(crate) fn from_context(ctx: &FrozenContext) -> Option<Self> {
		use crate::expr::paths::{AC, DB, ID, IP, NS, OR, RD, TK};

		let session = ctx.value("session")?;
		let strand = |path: &[crate::expr::Part]| {
			if let Value::String(s) = session.pick(path) {
				Some(s)
			} else {
				None
			}
		};
		let value = |path: &[crate::expr::Part]| {
			let v = session.pick(path);
			if let Value::None = v {
				None
			} else {
				Some(v)
			}
		};
		Some(Self {
			ns: strand(NS.as_ref()),
			db: strand(DB.as_ref()),
			id: if let Value::Uuid(u) = session.pick(ID.as_ref()) {
				Some(*u.as_ref())
			} else {
				None
			},
			ip: strand(IP.as_ref()),
			origin: strand(OR.as_ref()),
			ac: strand(AC.as_ref()),
			rd: value(RD.as_ref()),
			token: value(TK.as_ref()),
			exp: None,
		})
	}
}

/// Root-level context - always available.
///
/// Wraps a `FrozenContext` (the single source of truth for params, txn,
/// capabilities, etc.) and adds fields that are not trivially accessible
/// from `FrozenContext`:
/// - Authentication context (`Auth` struct, not a `Value`)
/// - Session info (pre-extracted typed fields)
/// - Datastore handle (for root-level operations)
/// - Cancellation token (tokio-based, supplements FrozenContext's AtomicBool)
/// - The evaluation `Options` (permission flags, import mode, computation-depth budget)
#[derive(Clone)]
pub struct RootContext {
	/// The underlying FrozenContext -- single source of truth for
	/// params, txn, capabilities, and every other context field.
	pub ctx: FrozenContext,
	/// The evaluation `Options` the builtin functions, scripting, schema statements and
	/// the write path read: permission flags, import mode and the computation-depth
	/// budget. Absent only in a test harness that builds a context without them.
	pub options: Option<Options>,
	/// The underlying datastore (optional - only needed for root-level operations
	/// like INFO FOR ROOT, DEFINE USER ON ROOT, etc.)
	///
	/// Note: This is None when executing from a borrowed Datastore reference.
	/// Root-level operations must handle a None datastore.
	pub datastore: Option<Arc<Datastore>>,
	/// Cancellation token for cooperative cancellation, behind an `Arc` so that
	/// deriving a context takes a reference count and no lock on the token's
	/// tree of children.
	pub cancellation: Arc<CancellationToken>,
	/// Authentication context for the current session
	pub auth: Arc<Auth>,
	/// Session information for context-aware functions
	pub(crate) session: Option<Arc<SessionInfo>>,
	/// Current value for correlated sub-execution (e.g., graph lookups).
	///
	/// When an operator chain is executed per-row (e.g., `GraphEdgeScan` sourced
	/// from the current record), this holds the input value that the
	/// `CurrentValueSource` operator yields into the stream. This is the
	/// explicit input binding: operators read it through `CurrentValueSource`,
	/// and it is set by `LookupPart` before executing the lookup's operator chain.
	pub(crate) current_value: Option<Arc<Value>>,
	/// The enclosing row a statement evaluated in scalar position reads as its
	/// document.
	///
	/// A block, `RETURN`, `LET` or `FOR` written where a scalar is expected —
	/// a projection, a `WHERE`, an `IF` branch, a function body — runs as a
	/// plan of its own, but its expressions evaluate against the row of the
	/// statement that holds it, exactly as `$this` denotes that row. Set by
	/// `ScalarSubquery` for such plans and read by the operators that evaluate
	/// a scalar without a row of their own (a bare expression, a `FOR` range).
	/// `RecursePart` sets it too, so the recursion operator can evaluate the
	/// recursed path's expressions against the idiom's document root. A
	/// relational plan never sets it: its scans supply their own rows.
	pub(crate) current_row: Option<Arc<Value>>,
	/// Set while a KNN query vector is evaluated: where a per-row evaluation
	/// would read the row being scanned, a read fails rather than finding
	/// `NONE` or another row. See [`ForbiddenRow`].
	pub(crate) forbidden_row: Option<Arc<ForbiddenRow>>,
	/// The distances the KNN search of the statement around a statement in value position
	/// publishes, which an index function of the inner statement reports against where the inner
	/// statement runs no search of its own.
	pub(crate) inherited_knn: Option<Arc<crate::exec::function::KnnContext>>,
	/// When true, RecordId dereferences bypass permission checks.
	///
	/// Propagated from `EvalContext` at subquery / lookup / recursion boundaries
	/// so that permission predicate evaluation does not re-enter table permissions
	/// and recurse infinitely on cyclic record links.
	///
	/// This is the *permission predicate* frame, and it carries two things at
	/// once: enforcement is off, and a write reached from here is refused. Use
	/// [`RootContext::perms_suppressed`] for a frame that suppresses
	/// enforcement and still performs writes.
	pub(crate) skip_fetch_perms: bool,
	/// When true, permission enforcement is suppressed for this frame, and the
	/// writes it performs are still allowed.
	///
	/// The referential-integrity cascades: a `REFERENCE ON DELETE` must be able
	/// to reach the referencing record whatever the actor may do to it, or
	/// integrity could not be maintained. Distinct from `skip_fetch_perms`
	/// because that frame also refuses the write.
	pub(crate) perms_suppressed: bool,
	/// The suppression the actor's own statement carried, which a nested scope
	/// with its own permission contract returns to.
	///
	/// Equal to `perms_suppressed` in every frame but one: a cascade bypasses
	/// enforcement for the record it is repairing, and the graph-edge cascade
	/// nested inside it must run as the actor all the same — an edge table's
	/// `PERMISSIONS FOR delete` still applies when deleting a vertex triggers
	/// it. See [`ExecutionContext::with_caller_perms`].
	pub(crate) caller_perms_suppressed: bool,
	/// The record whose computed fields are being evaluated, when a statement
	/// plan runs in scalar position inside one of those bodies.
	///
	/// A `COMPUTED` body written as a block is a plan of its own, and the
	/// `EvalContext` its expressions rebuild from this context must answer a
	/// dereference of the record being computed from the row rather than by
	/// re-fetching it, which would evaluate the same body again without end.
	/// Set by `ScalarSubquery` from the evaluating expression's context; read
	/// by `EvalContext::from_exec_ctx`.
	pub(crate) computing_record: Option<Arc<ComputingRecord>>,
	/// The frame that refuses a write reached from here, if any.
	///
	/// Two frames refuse one, and for different reasons: a `PERMISSIONS`
	/// predicate is evaluated with enforcement disabled and must therefore be
	/// free of observable side effects, and a `COMPUTED`
	/// body runs on every read of the field and under the definer's auth. Both
	/// are enforced twice, because a write can be reached by either route: the
	/// write path refuses a statement it runs under the frame, and the frame is
	/// mirrored into `Options::no_write` for code that rebuilds an execution
	/// frame from the options rather than carrying this context, such as an
	/// embedded script's `surrealdb.query()` bridge, `api::invoke`, or a
	/// Surrealism module call. `ExecutionContext::refusing_writes` mirrors it
	/// for a context derived from this one; `frame::options_for_frame` for a
	/// frame built from the options directly.
	///
	/// This is not [`RootContext::skip_fetch_perms`], and the two move
	/// independently: a *write* permission predicate sets that and still
	/// writes — its clauses are reached from a mutation and may carry side
	/// effects — and so does an event body.
	pub(crate) no_write: Option<crate::dbs::NoWriteFrame>,
	/// Evaluated VERSION timestamp for time-travel queries.
	///
	/// Set by the `VersionScope` operator when a SELECT has a VERSION clause.
	/// Read by `fetch_record`, `fetch_record_no_perms`, and `FieldPart` so
	/// that record dereferences and FETCH resolution honour the same version
	/// as the source scan.
	pub(crate) version_stamp: Option<u64>,
	/// The admissions of definitions' `PERMISSIONS` predicates this
	/// statement holds, shared by every context derived for it (see
	/// [`crate::exec::guard_verdicts`]).
	pub(crate) guard_verdicts: Arc<crate::exec::guard_verdicts::GuardVerdicts>,
	/// Whether the code running here is reached from a write statement, or
	/// from evaluating a `DEFINE PARAM` value. Such code names an argument
	/// error a method raises as the method's function, and leaves one a
	/// closure the method calls raises as the closure raised it; anywhere
	/// else a method names every argument error raised beneath it as its own
	/// (`InvalidMethodArguments`).
	pub(crate) writing: bool,
	/// Whether the caller whose write this is has permission-checked reads,
	/// captured once as the outermost write begins and carried unchanged into
	/// every frame the write reaches. It records the caller's real status before
	/// any privileged event or reference-cascade frame can mask it, so a redacted
	/// UNIQUE-index conflict can be withheld from a permission-checked caller even
	/// where a nested write stringifies the error (an event clause, a reference
	/// repair) before the write boundary inspects its typed form. A frame that
	/// runs definer code on a caller's behalf with permissions off (a `DEFINE
	/// API` chain) is seeded with the caller's status before any write begins
	/// ([`ExecutionContext::with_caller_view_checked`]); a write never clears a
	/// recorded `true`. See [`ExecutionContext::caller_view_checked`].
	pub(crate) caller_view_checked: bool,
	/// Where the statement's program records the changes it makes to the request's session.
	///
	/// `Some` only on the context of a top-level statement of the request. The derivations that
	/// run other code clear it: [`ExecutionContext::with_new_ctx`] (an isolated or call-body
	/// scope), [`ExecutionContext::with_principal`], [`ExecutionContext::with_limited_auth`],
	/// [`ExecutionContext::with_replaced_principal`], and those that enter another permission
	/// contract or the body of a computed field. The other guard does not depend on a context:
	/// lowering emits the instructions that record into it only for a statement of the request,
	/// never into a unit. See [`SessionSink`].
	pub(crate) session_effects: Option<Arc<SessionSink>>,
	/// How many statements the evaluations under this context have started
	/// nested — subqueries, blocks, nested statements of an expression,
	/// late-bound statements, `fn::` calls and the builtins that run a
	/// statement — and each block evaluated in place, counted as it is
	/// entered. Shared by every context derived from this one, so an evaluation
	/// that reaches a nested statement through a closure, a computed field or
	/// a lookup still counts it; a context [`ExecutionContext::with_own_nested_runs`]
	/// or a new root starts its own. An aggregate reads a value's failure about
	/// the shape of the data as `NONE` only where the count did not move while
	/// the value was evaluated (`exec::vm::interp::Region::Try`).
	pub(crate) nested_runs: Arc<std::sync::atomic::AtomicU64>,
}

/// The state of a KNN query vector's evaluation.
///
/// A KNN search or ranking evaluates a query vector computed when the
/// statement runs once, with no row in scope, before it reads any input. A
/// per-row evaluation of the same expression would have had the row being
/// scanned; that row is absent rather than `NONE`, and an expression that
/// reads it fails with [`ForbiddenRow::error`], the planner's refusal of a
/// vector that depends on it. Where a read meets it follows where a per-row
/// evaluation would have found that row:
///
/// - with `row` set, the value in scope of a frame that has none — a field, `@`, the row as the
///   root of a path, `$this` and `$self`, the document root a `$parent` with no binding falls back
///   to, a projection or index function, a script's or a module's document — and, through it, the
///   same reads in a function body or a closure invoked without a row;
/// - `$parent` and `$this` bound to the forbidden row ([`Binding::Forbidden`]) rather than to a
///   value: inside a statement the vector holds, which binds the row being scanned as both, and
///   inside a function body called without a row, which binds the caller's row as both.
///
/// The bindings are entries of the parameter chain ([`Context::add_forbidden`]),
/// so they follow its scoping: a binding of either name made inside the
/// statement — a statement nested in a SELECT of the vector binds that
/// SELECT's row — is its own, and a frame isolated from the statement's
/// parameters (a closure's body, a table's permission) does not see them. A
/// closure's parameters are its own bindings and never meet them. A forbidden
/// binding holds no value: [`ExecutionContext::value`] answers `None` for it,
/// and only [`ExecutionContext::binding`] tells it apart from no binding.
#[derive(Debug, Clone)]
pub(crate) struct ForbiddenRow {
	/// The query vector as written, which the error renders.
	vector: Arc<str>,
	/// Whether the value in scope of a frame that has none is the row being
	/// scanned.
	row: bool,
}

/// The names a statement or a body binds to the row it is entered from.
const ROW_PARAMS: [&str; 2] = ["parent", "this"];

impl ForbiddenRow {
	/// A forbidden row of the query vector `vector`, for a test that binds one by hand.
	#[cfg(test)]
	pub(crate) fn for_test(vector: &str) -> Self {
		Self {
			vector: Arc::from(vector),
			row: true,
		}
	}

	/// The error a read of the row being scanned raises: the planner's refusal
	/// of a query vector that depends on it.
	#[cold]
	pub(crate) fn error(&self) -> crate::expr::ControlFlow {
		crate::expr::ControlFlow::Err(self.refusal())
	}

	/// [`Self::error`], for a caller that returns `anyhow::Result`.
	#[cold]
	pub(crate) fn refusal(&self) -> anyhow::Error {
		anyhow::Error::new(ExecError::Query {
			message: format!(
				"The KNN query vector cannot depend on the record being scanned: {}",
				self.vector
			),
		})
	}

	/// Whether the value in scope of a frame that has none is the row being
	/// scanned.
	pub(crate) fn covers_row(&self) -> bool {
		self.row
	}
}

/// What a frame holds as its row ([`ExecutionContext::row_access`]): a row, no
/// row, or no row where a per-row evaluation would have held a KNN query
/// vector's row being scanned. Code that reads the row reads it through
/// [`Self::read`], which refuses the forbidden one; code that hands it to a
/// nested frame hands the whole of it on
/// ([`ExecutionContext::entering_statement`],
/// [`ExecutionContext::entering_call_body`]), so the nested frame forbids it in
/// turn.
#[must_use]
#[derive(Debug, Clone, Copy)]
pub(crate) enum RowAccess<'f, T> {
	/// The row.
	Present(T),
	/// No row.
	Absent,
	/// No row, where a per-row evaluation would have held the row being
	/// scanned.
	Forbidden(&'f Arc<ForbiddenRow>),
}

impl<'f, T> RowAccess<'f, T> {
	/// The row, for code that reads it: `None` where there is none, the
	/// vector's refusal where it is forbidden.
	pub(crate) fn read(self) -> anyhow::Result<Option<T>> {
		match self {
			Self::Present(row) => Ok(Some(row)),
			Self::Absent => Ok(None),
			Self::Forbidden(forbidden) => Err(forbidden.refusal()),
		}
	}

	/// The row, for a callee that runs in the same execution context as the
	/// frame and so meets a forbidden row through that context itself
	/// ([`RootContext::forbidden_row`]): the row where one is present, `None`
	/// otherwise.
	pub(crate) fn within_context(self) -> Option<T> {
		match self {
			Self::Present(row) => Some(row),
			Self::Absent | Self::Forbidden(_) => None,
		}
	}

	/// Whether there is a row.
	pub(crate) fn is_present(&self) -> bool {
		matches!(self, Self::Present(_))
	}

	/// The same access, borrowing the row.
	pub(crate) fn as_ref(&self) -> RowAccess<'f, &T> {
		match self {
			Self::Present(row) => RowAccess::Present(row),
			Self::Absent => RowAccess::Absent,
			Self::Forbidden(forbidden) => RowAccess::Forbidden(forbidden),
		}
	}

	/// The same access, with the row mapped by `f`.
	pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> RowAccess<'f, U> {
		match self {
			Self::Present(row) => RowAccess::Present(f(row)),
			Self::Absent => RowAccess::Absent,
			Self::Forbidden(forbidden) => RowAccess::Forbidden(forbidden),
		}
	}
}

impl std::fmt::Debug for RootContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("RootContext")
			.field("datastore", &self.datastore.as_ref().map(|_| "<Datastore>"))
			.field("cancellation", &self.cancellation)
			.field("auth", &self.auth)
			.field("session", &self.session)
			.field("current_value", &self.current_value.as_ref().map(|_| "<Value>"))
			.field("skip_fetch_perms", &self.skip_fetch_perms)
			.field("computing_record", &self.computing_record.as_ref().map(|r| &r.rid))
			.field("no_write", &self.no_write)
			.field("version_stamp", &self.version_stamp)
			.field("ctx", &"<FrozenContext>")
			.finish()
	}
}

/// Namespace-level context - root + namespace.
///
/// Contains everything from RootContext plus:
/// - Namespace definition
#[derive(Clone)]
pub struct NamespaceContext {
	/// Root context
	pub root: RootContext,
	/// The selected namespace definition
	pub ns: Arc<NamespaceDefinition>,
}

impl std::fmt::Debug for NamespaceContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("NamespaceContext").field("root", &self.root).field("ns", &self.ns).finish()
	}
}

impl NamespaceContext {
	/// Get the namespace name
	pub fn ns_name(&self) -> &str {
		&self.ns.name
	}

	/// Get the transaction (delegates to FrozenContext)
	pub fn txn(&self) -> Arc<Transaction> {
		self.root.ctx.tx()
	}

	/// Get the datastore (if available)
	pub fn datastore(&self) -> Option<&Datastore> {
		self.root.datastore.as_deref()
	}
}

/// Database-level context - namespace + database.
///
/// Contains everything from NamespaceContext plus:
/// - Database definition
#[derive(Clone)]
pub struct DatabaseContext {
	/// Namespace context (root + ns)
	pub ns_ctx: NamespaceContext,
	/// The selected database definition
	pub db: Arc<DatabaseDefinition>,
	/// Cache of field states (computed fields + permissions) keyed by (table, check_perms).
	/// Avoids repeated KV lookups for the same table within a single query execution.
	///
	/// Uses `tokio::sync::RwLock` so lock acquisition is async and cannot
	/// block the tokio runtime. The access pattern is heavily read-biased:
	/// the first scan operator populates the cache, and all subsequent operators
	/// only read.
	pub(crate) field_state_cache: FieldStateCache,
	/// Cache of table definitions keyed by table name.
	/// Avoids repeated `get_tb_by_name` KV lookups across scan operators
	/// within the same query execution.
	pub(crate) table_def_cache:
		Arc<tokio::sync::RwLock<HashMap<TableName, Option<Arc<TableDefinition>>>>>,
	/// Cache of index definitions keyed by table name.
	/// Avoids repeated `all_tb_indexes` KV lookups in DynamicScan's
	/// runtime index analysis within the same query execution.
	pub(crate) index_def_cache:
		Arc<tokio::sync::RwLock<HashMap<TableName, Arc<[IndexDefinition]>>>>,
	/// The compiled table `SELECT` permission a record read by id — a `FETCH`
	/// target, a record dereference — is checked against, keyed by table.
	/// Compiled once per table per execution by
	/// `exec::operators::fetch::table_select_permission`; a versioned read
	/// bypasses it, as [`Self::get_table_def`] bypasses its cache.
	pub(crate) select_permission_cache:
		Arc<tokio::sync::RwLock<HashMap<TableName, crate::exec::permission::PhysicalPermission>>>,
}

impl std::fmt::Debug for DatabaseContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DatabaseContext")
			.field("ns_ctx", &self.ns_ctx)
			.field("db", &self.db)
			.field("field_state_cache", &"<cache>")
			.field("table_def_cache", &"<cache>")
			.finish()
	}
}

impl DatabaseContext {
	/// Get the namespace name
	pub fn ns_name(&self) -> &str {
		self.ns_ctx.ns_name()
	}

	/// Get the database name
	pub fn db_name(&self) -> &str {
		&self.db.name
	}

	/// [`ExecutionContext::should_check_perms`] for a context known to be at
	/// database level: the same gate, with the namespace and database this
	/// context is in as the level the principal is compared against.
	pub(crate) fn should_check_perms(&self, action: Action) -> Result<bool, Error> {
		Ok(should_check_perms_at(&self.ns_ctx.root, Some((self.ns_name(), self.db_name())), action))
	}

	/// Get the namespace definition
	pub fn ns(&self) -> &NamespaceDefinition {
		&self.ns_ctx.ns
	}

	/// Get the transaction (delegates to FrozenContext)
	pub fn txn(&self) -> Arc<Transaction> {
		self.ns_ctx.txn()
	}

	/// Get the datastore (if available)
	pub fn datastore(&self) -> Option<&Datastore> {
		self.ns_ctx.datastore()
	}

	/// Look up a table definition, checking the execution-level cache first.
	///
	/// This avoids repeated `get_tb_by_name` KV roundtrips for the same table
	/// across multiple scan operators within the same query execution (e.g.,
	/// repeated record lookups).
	///
	/// When `version` is `Some`, the cache is bypassed and the lookup goes
	/// directly to versioned storage so that the schema matches the point
	/// in time being queried.
	pub(crate) async fn get_table_def(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> anyhow::Result<Option<Arc<TableDefinition>>> {
		if version.is_none() {
			// Check execution-level cache (read lock — concurrent reads allowed)
			let cache = self.table_def_cache.read().await;
			if let Some(cached) = cache.get(table) {
				return Ok(cached.clone());
			}
		}

		// Cache miss or versioned lookup — go to the transaction
		let result = crate::exec::vm::descriptor::definition(self, table, version).await?;

		if version.is_none() {
			// Populate cache (write lock — brief exclusive access)
			self.table_def_cache.write().await.insert(table.clone(), result.clone());
		}

		Ok(result)
	}

	/// Whether `table` is not defined at `version`, for a read that found no
	/// record in it and fails with `TbNotFound` where the answer is yes. The
	/// absence is the one [`Self::current_table_def`] answers.
	pub(crate) async fn table_is_undefined(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> anyhow::Result<bool> {
		Ok(self.current_table_def(table, version).await?.is_none())
	}

	/// The definition of `table` at `version`, as [`Self::get_table_def`]
	/// answers it, except that an absence the execution-level cache holds is
	/// read again from the transaction before it is believed, and the cache
	/// takes the answer.
	///
	/// A statement block that implicitly defines the table after an earlier
	/// read cached its absence leaves the cache behind the catalog; whatever
	/// fails, or decides what to write, on the table being absent must not do
	/// so for a table the block itself defined. The repeat is paid only where
	/// the cache answers no definition. A versioned lookup bypasses the cache,
	/// so its absence is read once.
	pub(crate) async fn current_table_def(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> anyhow::Result<Option<Arc<TableDefinition>>> {
		let cached = self.get_table_def(table, version).await?;
		if cached.is_some() || version.is_some() {
			return Ok(cached);
		}
		let current = crate::exec::vm::descriptor::definition(self, table, version).await?;
		self.table_def_cache.write().await.insert(table.clone(), current.clone());
		Ok(current)
	}

	/// Look up all index definitions for a table, checking the execution-level cache first.
	///
	/// This avoids repeated `all_tb_indexes` KV roundtrips for the same table
	/// across multiple DynamicScan operations within the same query execution.
	/// The cache stores the catalog definitions, not the durable build status;
	/// non-versioned callers still filter the cached list through durable state
	/// on every read so an index does not become queryable until its `!bs`
	/// record is `Online`.
	///
	/// When `version` is `Some`, the cache is bypassed for the same reason
	/// as `get_table_def`, and the historical catalog is returned without
	/// applying current durable build-state filtering.
	pub(crate) async fn get_table_indexes(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> anyhow::Result<Arc<[IndexDefinition]>> {
		use crate::catalog::providers::TableProvider;
		if version.is_none() {
			let cached = {
				// Release the cache guard before durable filtering, which awaits KV reads.
				let cache = self.index_def_cache.read().await;
				cache.get(table).cloned()
			};
			if let Some(cached) = cached {
				let txn = self.txn();
				return filter_online_indexes(
					&txn,
					self.ns_ctx.ns.namespace_id,
					self.db.database_id,
					cached,
				)
				.await;
			}
		}

		// Cache miss or versioned lookup — go to the transaction
		let txn = self.txn();
		let result = match txn
			.all_tb_indexes(self.ns_ctx.ns.namespace_id, self.db.database_id, table, version)
			.await
		{
			Ok(result) => result,
			// An index definition this build cannot read is skipped.
			// The scan reads the table without any of the indexes, which
			// returns the same rows, and the list is not cached.
			Err(e)
				if matches!(
					crate::err::Error::entry_refusal(&e),
					Some(surrealdb_catalog::stored::EntryError::NeedsRelease { .. })
				) =>
			{
				return Ok(Arc::from([]));
			}
			Err(e) => return Err(e),
		};

		if version.is_none() {
			// Populate cache (write lock — brief exclusive access)
			self.index_def_cache.write().await.insert(table.clone(), Arc::clone(&result));
		}

		if version.is_none() {
			filter_online_indexes(&txn, self.ns_ctx.ns.namespace_id, self.db.database_id, result)
				.await
		} else {
			Ok(result)
		}
	}
}

/// Unified execution context - a discriminated union of all context levels.
///
/// Operators receive this enum and use typed accessor methods to get the
/// appropriate context level. The accessors return `Result` for levels that
/// may not be available, providing runtime safety in addition to the
/// compile-time safety from the typed context structs.
#[derive(Debug, Clone)]
pub enum ExecutionContext {
	/// Root-level context (no ns/db selected)
	Root(RootContext),
	/// Namespace-level context (ns selected, no db)
	Namespace(NamespaceContext),
	/// Database-level context (both ns and db selected)
	Database(DatabaseContext),
}

/// The gate behind [`ExecutionContext::should_check_perms`] and
/// [`DatabaseContext::should_check_perms`]: the permission-frame bypass, then
/// the role-and-level rule shared with the plan-time gates.
fn should_check_perms_at(root: &RootContext, level: Option<(&str, &str)>, action: Action) -> bool {
	if root.skip_fetch_perms || root.perms_suppressed {
		return false;
	}
	let (ns, db) = match level {
		Some((ns, db)) => (Some(ns), Some(db)),
		None => (None, None),
	};
	action_is_checked(root.ctx.auth_enabled(), &root.auth, action, ns, db)
}

impl ExecutionContext {
	/// The database-level context a task that holds no session reads rows in:
	/// `ns` and `db` as `ctx` and `opt` select them, with the authority of
	/// `opt` and no permission evaluated, so a table's rows are read as stored
	/// and brought through the table's migration steps for any principal alike.
	///
	/// `ctx` carries the transaction the task reads in, and no parameter is
	/// bound.
	pub(crate) fn privileged_database(
		ctx: FrozenContext,
		opt: Options,
		ns: Arc<NamespaceDefinition>,
		db: Arc<DatabaseDefinition>,
	) -> Self {
		let root = RootContext {
			ctx,
			auth: Arc::clone(&opt.auth),
			options: Some(opt),
			datastore: None,
			cancellation: Arc::new(CancellationToken::new()),
			session: None,
			current_value: None,
			current_row: None,
			skip_fetch_perms: false,
			perms_suppressed: true,
			caller_perms_suppressed: true,
			computing_record: None,
			no_write: None,
			version_stamp: None,
			guard_verdicts: Default::default(),
			writing: false,
			caller_view_checked: false,
			forbidden_row: None,
			inherited_knn: None,
			nested_runs: Default::default(),
			session_effects: None,
		};
		Self::Database(DatabaseContext {
			ns_ctx: NamespaceContext {
				root,
				ns,
			},
			db,
			field_state_cache: Default::default(),
			table_def_cache: Default::default(),
			index_def_cache: Default::default(),
			select_permission_cache: Default::default(),
		})
	}

	/// Get root-level access (always succeeds).
	///
	/// Returns a reference to the root context, which is available at all levels.
	pub fn root(&self) -> &RootContext {
		match self {
			Self::Root(r) => r,
			Self::Namespace(n) => &n.root,
			Self::Database(d) => &d.ns_ctx.root,
		}
	}

	/// How many statements the evaluations under this context have started
	/// nested ([`RootContext::nested_runs`]).
	pub(crate) fn nested_runs(&self) -> u64 {
		self.root().nested_runs.load(std::sync::atomic::Ordering::Relaxed)
	}

	/// Count a statement an evaluation under this context starts nested, or a
	/// block evaluated in place that it enters ([`RootContext::nested_runs`]).
	pub(crate) fn note_nested_run(&self) {
		self.root().nested_runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	}

	/// This context with a count of nested statements of its own, for work
	/// that runs beside the evaluation that holds this one, such as a morsel's
	/// worker, so that neither counts what the other starts.
	pub(crate) fn with_own_nested_runs(&self) -> Self {
		let mut own = self.clone();
		own.root_mut().nested_runs = Default::default();
		own
	}

	/// The sink the running statement records its session effects in: `Some` only for the
	/// program of a top-level statement of the request.
	pub(crate) fn session_sink(&self) -> Option<&SessionSink> {
		self.root().session_effects.as_deref()
	}

	/// This context with `sink` installed as the sink of the statement it runs.
	pub(crate) fn with_session_sink(mut self, sink: Arc<SessionSink>) -> Self {
		self.root_mut().session_effects = Some(sink);
		self
	}

	/// This context cancelled through `cancellation` alone, rather than the token of the request
	/// it was made in: for work that outlives that request.
	pub(crate) fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
		self.root_mut().cancellation = Arc::new(cancellation);
		self
	}

	/// Get namespace-level access (may fail if only root context).
	///
	/// Returns an error if no namespace has been selected.
	pub fn namespace(&self) -> Result<&NamespaceContext, Error> {
		match self {
			Self::Root(_) => Err(ExecError::NsEmpty.into()),
			Self::Namespace(n) => Ok(n),
			Self::Database(d) => Ok(&d.ns_ctx),
		}
	}

	/// Get database-level access (may fail if root or namespace only).
	///
	/// Returns an error if no database has been selected.
	pub fn database(&self) -> Result<&DatabaseContext, Error> {
		match self {
			Self::Root(_) | Self::Namespace(_) => Err(ExecError::DbEmpty.into()),
			Self::Database(d) => Ok(d),
		}
	}

	/// Get the current context level.
	pub fn level(&self) -> ContextLevel {
		match self {
			Self::Root(_) => ContextLevel::Root,
			Self::Namespace(_) => ContextLevel::Namespace,
			Self::Database(_) => ContextLevel::Database,
		}
	}

	/// Get the underlying FrozenContext.
	///
	/// This provides access to the FrozenContext which is the single source
	/// of truth for parameters, transactions, capabilities, and all other
	/// context fields. Used both by delegation methods and by operators that
	/// need direct access to the frozen context.
	pub fn ctx(&self) -> &FrozenContext {
		&self.root().ctx
	}

	/// Get the transaction (delegates to FrozenContext).
	///
	/// Panics when the context carries no transaction. Callers that can be
	/// reached under a transaction-less context must use [`Self::try_txn`] and
	/// report the missing transaction as an error instead.
	pub fn txn(&self) -> Arc<Transaction> {
		self.root().ctx.tx()
	}

	/// Get the transaction, or `None` when this context carries none.
	///
	/// The executor always attaches one before running a plan, so operators
	/// built by it can rely on [`Self::txn`]. Contexts assembled outside that
	/// path — operator unit tests, for instance — may have none, and code
	/// reachable from them uses this to fail rather than panic.
	pub fn try_txn(&self) -> Option<Arc<Transaction>> {
		self.root().ctx.try_tx().cloned()
	}

	/// Look up a parameter value by name (delegates to FrozenContext).
	///
	/// This uses FrozenContext's parent-chain scoped lookup, which correctly
	/// handles shadowing and protected parameter names ($auth, $session, etc.).
	pub fn value(&self, key: &str) -> Option<&Value> {
		self.root().ctx.value(key)
	}

	/// Look up a parameter value by name as the code of a function body reads
	/// it ([`Context::lexical_value`]): outside a body the same as
	/// [`Self::value`], inside one never a caller's `LET` or `FOR` binding.
	pub(crate) fn lexical_value(&self, key: &str) -> Option<&Value> {
		self.root().ctx.lexical_value(key).map(Arc::as_ref)
	}

	/// What `key` is bound to as the code of a function body reads it
	/// ([`Context::lexical_binding`]): a value, a KNN query vector's row being
	/// scanned ([`Binding::Forbidden`]), or nothing.
	pub(crate) fn lexical_binding(&self, key: &str) -> Option<Binding<'_>> {
		self.root().ctx.lexical_binding(key)
	}

	/// Collect all parameter values from the context chain into a HashMap.
	///
	/// This walks the FrozenContext parent chain and collects all values,
	/// with child values taking precedence over parent values (shadowing).
	/// Protected parameter names are excluded.
	pub fn collect_params(&self) -> Parameters {
		self.root().ctx.collect_values(HashMap::new())
	}

	/// Get the datastore (if available).
	///
	/// Returns None when executing from a borrowed Datastore reference.
	/// Root-level operations that need direct datastore access should handle this case.
	pub fn datastore(&self) -> Option<&Datastore> {
		self.root().datastore.as_deref()
	}

	/// Get the authentication context.
	pub fn auth(&self) -> &Auth {
		&self.root().auth
	}

	/// Check if authentication is enabled.
	pub fn auth_enabled(&self) -> bool {
		self.root().ctx.auth_enabled()
	}

	/// Whether permission checks apply to `action` in this context.
	///
	/// The one gate every permission check in the engine consults:
	/// the scan, graph, reference and join operators through
	/// [`DatabaseContext::should_check_perms`], and the reads of a `DEFINE
	/// FUNCTION`, `DEFINE PARAM`, module or model `PERMISSIONS` clause
	/// directly. Returns `false`, bypassing every check, when:
	///
	/// - the context is inside a permission predicate (`skip_fetch_perms`). A definer-authored
	///   predicate reads freely, as it is evaluated with `perms = false`; exempting it here
	///   disables the whole-scan `Deny` short-circuits, the per-row table and field permission
	///   filtering, edge and target enforcement, and the function and parameter permission checks
	///   in one place;
	/// - server auth is disabled and the session is anonymous;
	/// - the principal's role and actor level exempt it
	///   ([`crate::exec::permission::action_is_checked`]). Without a database context a namespace
	///   or database user's level cannot be compared, so only a root user with the role is exempt.
	pub fn should_check_perms(&self, action: Action) -> Result<bool, Error> {
		let level = self.database().ok().map(|db_ctx| (db_ctx.ns_name(), db_ctx.db_name()));
		Ok(should_check_perms_at(self.root(), level, action))
	}

	/// Rebuild this ExecutionContext with a new FrozenContext, preserving
	/// ns/db definitions, auth, session, options, datastore, and cancellation.
	///
	/// `pub(crate)` so `eval::*` can run a nested query against an *isolated*
	/// child context (only the explicit bindings visible, never the call site's
	/// scope) while keeping the current transaction, auth and ns/db level.
	pub(crate) fn with_new_ctx(&self, ctx: FrozenContext) -> Self {
		match self {
			Self::Root(r) => Self::Root(RootContext {
				ctx,
				options: r.options.clone(),
				datastore: r.datastore.clone(),
				cancellation: Arc::clone(&r.cancellation),
				auth: Arc::clone(&r.auth),
				session: r.session.clone(),
				current_value: r.current_value.clone(),
				current_row: r.current_row.clone(),
				forbidden_row: r.forbidden_row.clone(),
				inherited_knn: r.inherited_knn.clone(),
				skip_fetch_perms: r.skip_fetch_perms,
				perms_suppressed: r.perms_suppressed,
				caller_perms_suppressed: r.caller_perms_suppressed,
				computing_record: r.computing_record.clone(),
				no_write: r.no_write,
				version_stamp: r.version_stamp,
				guard_verdicts: Arc::clone(&r.guard_verdicts),
				writing: r.writing,
				caller_view_checked: r.caller_view_checked,
				nested_runs: Arc::clone(&r.nested_runs),
				session_effects: None,
			}),
			Self::Namespace(n) => Self::Namespace(NamespaceContext {
				root: RootContext {
					ctx,
					options: n.root.options.clone(),
					datastore: n.root.datastore.clone(),
					cancellation: Arc::clone(&n.root.cancellation),
					auth: Arc::clone(&n.root.auth),
					session: n.root.session.clone(),
					current_value: n.root.current_value.clone(),
					current_row: n.root.current_row.clone(),
					forbidden_row: n.root.forbidden_row.clone(),
					inherited_knn: n.root.inherited_knn.clone(),
					skip_fetch_perms: n.root.skip_fetch_perms,
					perms_suppressed: n.root.perms_suppressed,
					caller_perms_suppressed: n.root.caller_perms_suppressed,
					computing_record: n.root.computing_record.clone(),
					no_write: n.root.no_write,
					version_stamp: n.root.version_stamp,
					guard_verdicts: Arc::clone(&n.root.guard_verdicts),
					writing: n.root.writing,
					caller_view_checked: n.root.caller_view_checked,
					nested_runs: Arc::clone(&n.root.nested_runs),
					session_effects: None,
				},
				ns: Arc::clone(&n.ns),
			}),
			Self::Database(d) => Self::Database(DatabaseContext {
				ns_ctx: NamespaceContext {
					root: RootContext {
						ctx,
						options: d.ns_ctx.root.options.clone(),
						datastore: d.ns_ctx.root.datastore.clone(),
						cancellation: Arc::clone(&d.ns_ctx.root.cancellation),
						auth: Arc::clone(&d.ns_ctx.root.auth),
						session: d.ns_ctx.root.session.clone(),
						current_value: d.ns_ctx.root.current_value.clone(),
						current_row: d.ns_ctx.root.current_row.clone(),
						forbidden_row: d.ns_ctx.root.forbidden_row.clone(),
						inherited_knn: d.ns_ctx.root.inherited_knn.clone(),
						skip_fetch_perms: d.ns_ctx.root.skip_fetch_perms,
						perms_suppressed: d.ns_ctx.root.perms_suppressed,
						caller_perms_suppressed: d.ns_ctx.root.caller_perms_suppressed,
						computing_record: d.ns_ctx.root.computing_record.clone(),
						no_write: d.ns_ctx.root.no_write,
						version_stamp: d.ns_ctx.root.version_stamp,
						guard_verdicts: Arc::clone(&d.ns_ctx.root.guard_verdicts),
						writing: d.ns_ctx.root.writing,
						caller_view_checked: d.ns_ctx.root.caller_view_checked,
						nested_runs: Arc::clone(&d.ns_ctx.root.nested_runs),
						session_effects: None,
					},
					ns: Arc::clone(&d.ns_ctx.ns),
				},
				db: Arc::clone(&d.db),
				field_state_cache: Arc::clone(&d.field_state_cache),
				table_def_cache: Arc::clone(&d.table_def_cache),
				index_def_cache: Arc::clone(&d.index_def_cache),
				select_permission_cache: Arc::clone(&d.select_permission_cache),
			}),
		}
	}

	/// Create a new context with the current value set for correlated sub-execution.
	///
	/// This is used by `LookupPart` to bind the current row's value (typically a
	/// RecordId) before executing a graph/reference lookup operator chain.
	/// The `CurrentValueSource` operator reads this value to seed the chain.
	pub fn with_current_value(&self, value: Value) -> Self {
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.current_value = Some(Arc::new(value));
		new
	}

	/// Get the current value for correlated sub-execution (if set).
	///
	/// Returns the value set by `with_current_value()`. Used by
	/// `CurrentValueSource` to yield its input into the operator stream.
	pub fn current_value(&self) -> Option<&Value> {
		self.root().current_value.as_deref()
	}

	/// Derive a context whose statements read `row` as the enclosing document;
	/// see [`RootContext::current_row`].
	pub(crate) fn with_current_row(&self, row: Arc<Value>) -> Self {
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.current_row = Some(row);
		new
	}

	/// The enclosing row a statement in scalar position evaluates against, if
	/// any; see [`RootContext::current_row`].
	pub(crate) fn current_row(&self) -> Option<&Value> {
		self.root().current_row.as_deref()
	}

	/// The allocation [`Self::current_row`] reads, for a derivation that holds
	/// the same row to share it rather than copy it.
	pub(crate) fn current_row_shared(&self) -> Option<&Arc<Value>> {
		self.root().current_row.as_ref()
	}

	/// Derive the context a KNN query `vector` is evaluated in: no enclosing
	/// row, and a read of the row being scanned fails (see [`ForbiddenRow`]).
	pub(crate) fn with_forbidden_row(&self, vector: Arc<str>) -> Self {
		let mut new = self.clone();
		let root = new.root_mut();
		root.current_row = None;
		root.forbidden_row = Some(Arc::new(ForbiddenRow {
			vector,
			row: true,
		}));
		new
	}

	/// Derive the context a statement in value position runs in, whose index functions report
	/// against `knn`, the distances the search of the statement around it publishes, where it runs
	/// no search of its own.
	pub(crate) fn with_inherited_knn(&self, knn: Arc<crate::exec::function::KnnContext>) -> Self {
		let mut new = self.clone();
		new.root_mut().inherited_knn = Some(knn);
		new
	}

	/// The distances of the KNN search of the statement around this one, if any
	/// ([`Self::with_inherited_knn`]).
	pub(crate) fn inherited_knn(&self) -> Option<&Arc<crate::exec::function::KnnContext>> {
		self.root().inherited_knn.as_ref()
	}

	/// Derive a context isolated from the KNN query vector evaluated around
	/// this one, for a frame that reaches no row of its caller's (a nested
	/// query run by `eval`): with no row being scanned in reach, a read of the
	/// value in scope finds none rather than failing.
	pub(crate) fn without_forbidden_row(&self) -> Self {
		let mut new = self.clone();
		new.root_mut().forbidden_row = None;
		new
	}

	/// The state of the KNN query vector evaluated around this context, if
	/// any; see [`ForbiddenRow`].
	#[cfg(test)]
	pub(crate) fn forbidden_row(&self) -> Option<&ForbiddenRow> {
		self.root().forbidden_row.as_deref()
	}

	/// The context a statement or a body entered from a frame with no row runs
	/// in, where this one's value in scope is the row being scanned of a KNN
	/// query vector: the statement or body binds that row as its `$parent`
	/// and `$this`, which read as forbidden; its own value in scope is that
	/// row only where `inherits_row` says it reads the enclosing one (a block,
	/// a function body). A binding of either name made inside it is its own.
	/// `None` where nothing changes (see [`ForbiddenRow`]).
	fn forbidding_row_params(&self, inherits_row: bool) -> Option<Self> {
		let forbidden = self.root().forbidden_row.as_ref().filter(|forbidden| forbidden.row)?;
		let state = ForbiddenRow {
			vector: Arc::clone(&forbidden.vector),
			row: inherits_row,
		};
		let mut child = Context::new_child(self.ctx());
		child.add_forbidden(&ROW_PARAMS, forbidden);
		let mut new = self.with_new_ctx(child.freeze());
		new.root_mut().forbidden_row = Some(Arc::new(state));
		Some(new)
	}

	/// `row`, a frame's row in this context, as the frame holds it: where there
	/// is none and the KNN query vector evaluated around this context forbids
	/// the value in scope, the forbidden row ([`RowAccess::Forbidden`]).
	pub(crate) fn row_access<T>(&self, row: Option<T>) -> RowAccess<'_, T> {
		match row {
			Some(row) => RowAccess::Present(row),
			None => match self.root().forbidden_row.as_ref() {
				Some(forbidden) if forbidden.covers_row() => RowAccess::Forbidden(forbidden),
				_ => RowAccess::Absent,
			},
		}
	}

	/// The context a statement entered from a frame runs in (a subquery, a
	/// nested statement): the frame's `row` bound as its `$parent` and
	/// `$this`, and as its current row where `inherits_row` says it reads the
	/// enclosing one (a block, a function body); a forbidden row bound as
	/// forbidden ([`Self::forbidding_row_params`]); nothing bound where the
	/// frame has no row.
	pub(crate) fn entering_statement(
		&self,
		row: RowAccess<'_, &Value>,
		inherits_row: bool,
	) -> Self {
		match row {
			RowAccess::Present(row) => {
				let row = Arc::new(row.clone());
				let entered = self.with_shared_param(&ROW_PARAMS, &row);
				if inherits_row {
					entered.with_current_row(row)
				} else {
					entered
				}
			}
			RowAccess::Forbidden(_) => {
				self.forbidding_row_params(inherits_row).unwrap_or_else(|| self.clone())
			}
			RowAccess::Absent => self.clone(),
		}
	}

	/// What `key` is bound to in this context's parameter chain: a value, a
	/// KNN query vector's row being scanned ([`Binding::Forbidden`]), or
	/// nothing. The resolution of a parameter an expression reads goes through
	/// it, so a forbidden binding is refused rather than read.
	#[inline]
	pub(crate) fn binding(&self, key: &str) -> Option<Binding<'_>> {
		self.root().ctx.binding(key)
	}

	/// The enclosing row a statement reads as `$this` — in its source, in a
	/// write's subjects — and the context to evaluate those reads in when it
	/// differs from this one.
	///
	/// Where `$this` is bound to a KNN query vector's row being scanned
	/// ([`Binding::Forbidden`]), there is no row to hand on, and the context
	/// returned refuses a read of the value in scope, as the per-row
	/// evaluation would have read that row (see [`ForbiddenRow`]).
	pub(crate) fn enclosing_row(&self) -> (Option<&Value>, Option<Self>) {
		match self.binding("this") {
			Some(Binding::Value(row)) => (Some(row.as_ref()), None),
			Some(Binding::Forbidden(row)) => {
				let mut forbidding = self.clone();
				forbidding.root_mut().forbidden_row = Some(Arc::new(ForbiddenRow {
					vector: Arc::clone(&row.vector),
					row: true,
				}));
				(None, Some(forbidding))
			}
			None => (None, None),
		}
	}

	/// Derive a context that skips permission checks on RecordId dereferences.
	///
	/// Used at subquery / lookup / recursion boundaries when the parent
	/// `EvalContext` has `skip_fetch_perms` set (i.e., we are inside a
	/// permission predicate evaluation).
	///
	/// This is the strictest of the three permission frames and it stands on
	/// its own: it leaves [`RootContext::perms_suppressed`] and its caller
	/// counterpart where they are, and nothing beneath it can raise
	/// enforcement back. `should_check_perms_at` reads the two as a
	/// disjunction, and `options_for_frame` rebuilds the
	/// permission-predicate options from scratch whatever a nested
	/// [`ExecutionContext::with_caller_perms`] wrote — so the predicate frame
	/// holds on both sides without the two flags being coupled.
	pub fn with_skip_fetch_perms(self, skip: bool) -> Self {
		if skip == self.root().skip_fetch_perms {
			return self;
		}
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.skip_fetch_perms = skip;
		// A frame with another permission contract runs clauses, never a statement of the request.
		root.session_effects = None;
		new
	}

	/// The context a privileged frame evaluates in: code a definer wrote — a
	/// permission, a field's `VALUE` or `ASSERT`, a computed field, an event,
	/// a gate — which runs under the definer's authority rather than the
	/// caller's bindings. It holds the session's own values (`$auth`,
	/// `$access`, `$token`, `$session`, read through the parent chain), the
	/// database's parameters, and whatever the frame binds itself after this;
	/// no query parameter, session `LET`, `LET`, `FOR` variable or closure of
	/// the caller is visible in it, nor in any context derived from it. A
	/// function called from the frame reads the arguments of the call, the
	/// row names, the session's own values and the database's parameters, and
	/// no query parameter either: the frame holds none to read past the call
	/// boundary ([`Self::entering_call_body`]).
	pub(crate) fn privileged_frame(self) -> Self {
		let isolated = Context::new_isolated(self.ctx()).freeze();
		self.with_new_ctx(isolated)
	}

	/// Enter a compiled body one re-entry deeper, consuming one unit of the
	/// computation-depth budget.
	///
	/// The budget is the dive of the [`Options`] this context carries, so one
	/// bound, `max_computation_depth`, covers a chain of re-entries however
	/// each hop is reached: a compiled closure that invokes itself, or a body
	/// that hands a sub-computation to `eval::surql` or a function that calls
	/// back into a closure (whose context is rebuilt from the options). The
	/// count lives here rather than on an `EvalContext` because the operators a
	/// body's blocks and subqueries run through rebuild their `EvalContext`
	/// from this context, and a count kept there would restart at each of
	/// them. Fails with [`ExecError::ComputationDepthExceeded`] once the budget
	/// is spent; a context built without options — only a test harness builds
	/// one — has no budget and is returned unchanged.
	pub(crate) fn dive(&self) -> anyhow::Result<Self> {
		self.dive_by(1)
	}

	/// Whether the evaluation spends the computation-depth budget per
	/// expression it enters ([`crate::exec::vm::depth`]).
	pub(crate) fn per_expression_depth(&self) -> bool {
		self.options().is_some_and(|options| options.per_expression_depth)
	}

	/// This context, spending `consumed` of the computation-depth budget
	/// without failing where less is left — the next spend fails instead —
	/// and evaluating per expression where `per_expression`.
	pub(crate) fn entering_stored_code(&self, consumed: u32, per_expression: bool) -> Self {
		let Some(options) = self.options() else {
			return self.clone();
		};
		let mut options = options.with_dive_consumed(consumed);
		options.per_expression_depth = per_expression;
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.options = Some(options);
		new
	}

	/// [`Self::dive`], consuming `cost` units of the budget at once.
	pub(crate) fn dive_by(&self, cost: u8) -> anyhow::Result<Self> {
		let Some(options) = self.options() else {
			return Ok(self.clone());
		};
		let options = options.dive(cost).map_err(anyhow::Error::new)?;
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.options = Some(options);
		Ok(new)
	}

	/// Whether a re-entry could be entered, as [`Self::dive`] would, without
	/// deriving the context it would answer.
	pub(crate) fn check_dive(&self) -> anyhow::Result<()> {
		self.check_dive_by(1)
	}

	/// Whether `cost` units of the budget are left, as [`Self::dive_by`]
	/// would find them, without deriving the context it would answer.
	pub(crate) fn check_dive_by(&self, cost: u8) -> anyhow::Result<()> {
		match self.options() {
			Some(options) => options.check_dive(cost).map_err(anyhow::Error::new),
			None => Ok(()),
		}
	}

	/// Derive the context the body of a user-defined function runs in, from
	/// `self`, the context its definition's `AUTH LIMIT` capped, in one step:
	/// one re-entry deeper ([`Self::dive`]), in one child scope that binds each
	/// of `params` and, for a call made over a row, `$parent` and `$this` to
	/// `row` — which is also the enclosing row the body's statements read as
	/// their document ([`RootContext::current_row`]) — with `units` the table
	/// the nested programs find their units in.
	///
	/// The scope is a child of `self`'s and the boundary of the call
	/// ([`Context::mark_call_boundary`]): the body is lexically scoped. It reads
	/// its own bindings, the row names and the protected names of the chain
	/// it is entered from, and the request's parameters; it never reads a
	/// binding its caller made with `LET` or `FOR`, at any depth of calls, and
	/// a name none of those holds is a `DEFINE PARAM` of the database or
	/// `NONE` ([`Context::lexical_value`]). A caller that is a privileged frame
	/// ([`Self::privileged_frame`]) holds no request parameter, so the body
	/// reads none either. The scope binds the parameters first, so
	/// `$parent` and `$this` shadow a parameter of the same name, as scopes
	/// nested in that order would.
	/// `skip_fetch_perms` and `computing_record` are carried onto the context
	/// where the caller's evaluation holds them; without one the context
	/// keeps the ones `self` has. The budget is checked before anything is
	/// derived.
	pub(crate) fn entering_call_body<N: Into<Strand>>(
		&self,
		params: impl IntoIterator<Item = (N, Value)>,
		units: crate::exec::vm::UnitTable,
		row: RowAccess<'_, &Arc<Value>>,
		skip_fetch_perms: bool,
		computing_record: Option<&Arc<ComputingRecord>>,
	) -> anyhow::Result<Self> {
		self.check_dive()?;
		let mut child = Context::new_child(self.ctx());
		for (name, value) in params {
			child.add_value(name, Arc::new(value));
		}
		child.set_vm_units(units);
		// A body called without a row, where the caller's value in scope is a
		// KNN query vector's row being scanned, binds that row as its
		// `$parent` and `$this`, which read as forbidden. Like a row, it is
		// bound after the arguments: one of either name reads as the row.
		if let RowAccess::Forbidden(forbidden) = row {
			child.add_forbidden(&ROW_PARAMS, forbidden);
		}
		let row = row.within_context();
		if let Some(row) = row {
			child.add_value("parent", Arc::clone(row));
			child.add_value("this", Arc::clone(row));
		}
		child.mark_call_boundary();
		let mut new = self.with_new_ctx(child.freeze());
		let root = new.root_mut();
		if let Some(options) = root.options.as_mut() {
			options.spend_dive(1).map_err(anyhow::Error::new)?;
		}
		if let Some(row) = row {
			root.current_row = Some(Arc::clone(row));
		}
		if skip_fetch_perms {
			root.skip_fetch_perms = true;
		}
		if let Some(record) = computing_record {
			root.computing_record = Some(Arc::clone(record));
		}
		Ok(new)
	}

	fn root_mut(&mut self) -> &mut RootContext {
		match self {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		}
	}

	/// Derive a context whose statements in scalar position evaluate inside
	/// the computed-field body of `record`; see
	/// [`RootContext::computing_record`].
	pub(crate) fn with_computing_record(self, record: Option<Arc<ComputingRecord>>) -> Self {
		let same = match (&record, &self.root().computing_record) {
			(None, None) => true,
			(Some(a), Some(b)) => Arc::ptr_eq(a, b),
			_ => false,
		};
		if same {
			return self;
		}
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.computing_record = record;
		// The body of a computed field is nested code, never a statement of the request.
		root.session_effects = None;
		new
	}

	/// Derive a context in which a write is refused, naming the frame that
	/// refuses it.
	///
	/// A frame already refusing a write is left alone: the two frames refuse
	/// alike, so which of them a nested one names changes nothing, and the
	/// outer one is the one the caller entered. See [`RootContext::no_write`].
	///
	/// The refusal is set on [`Options::no_write`] as well as on
	/// [`RootContext::no_write`], the two halves [`RootContext::no_write`]
	/// describes. A builtin that rebuilds an execution frame from the options
	/// rather than carrying this context across - an embedded script's
	/// `surrealdb.query()` bridge, `api::invoke` - seeds the new frame's refusal
	/// from the options ([`crate::exec::entry::frame`]), so a write reached
	/// through one of them is refused the same way a write made directly here
	/// is.
	pub fn refusing_writes(self, frame: crate::dbs::NoWriteFrame) -> Self {
		if self.root().no_write.is_some() {
			return self;
		}
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.no_write = Some(frame);
		if let Some(options) = root.options.as_mut() {
			options.no_write = options.no_write.or(Some(frame));
		}
		new
	}

	/// Derive the frame a field migration's transform runs in: writes and
	/// record dereference are refused, whatever frame encloses it.
	pub(crate) fn migrating(self) -> Self {
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.no_write = Some(crate::dbs::NoWriteFrame::Migration);
		new
	}

	/// Whether this context evaluates a field migration's transform, which
	/// may not dereference a record.
	pub(crate) fn refuses_dereference(&self) -> bool {
		self.root().no_write == Some(crate::dbs::NoWriteFrame::Migration)
	}

	/// Derive a context for code a write statement reaches
	/// ([`RootContext::writing`]).
	///
	/// The first hop, which enters the caller's own write, records whether the
	/// caller's reads are permission checked ([`RootContext::caller_view_checked`]):
	/// checked when this frame checks them or when the frame was seeded as
	/// checked on a caller's behalf. A nested write already runs inside the
	/// caller's frame, so it keeps that recorded status rather than reading its
	/// own suppressed frame.
	pub(crate) fn in_write(self) -> Self {
		if self.root().writing {
			return self;
		}
		let caller_view_checked = self.root().caller_view_checked
			|| self.should_check_perms(Action::View).unwrap_or(true);
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.writing = true;
		root.caller_view_checked = caller_view_checked;
		new
	}

	/// Whether the caller whose write this context runs inside has
	/// permission-checked reads, as recorded when the outermost write began
	/// ([`RootContext::caller_view_checked`]).
	///
	/// Read where a UNIQUE-index conflict a nested write raised is redacted
	/// before it is stringified, so the decision reflects the caller rather than
	/// the privileged event or reference-cascade frame the conflict is raised in.
	/// `false` before any write, which withholds nothing, unless the frame was
	/// seeded with [`Self::with_caller_view_checked`].
	pub(crate) fn caller_view_checked(&self) -> bool {
		self.root().caller_view_checked
	}

	/// This context with the caller's read-check status recorded as `checked`
	/// ([`RootContext::caller_view_checked`]), for a frame that runs definer
	/// code on a caller's behalf with permissions off, so a UNIQUE-index
	/// conflict a write in it raises is withheld from a permission-checked
	/// caller as it is from the caller's own write.
	pub(crate) fn with_caller_view_checked(mut self, checked: bool) -> Self {
		self.root_mut().caller_view_checked = checked;
		self
	}

	/// Derive a context in which a write is refused because a `COMPUTED` field
	/// body is being evaluated.
	pub fn computing_field(self) -> Self {
		self.refusing_writes(crate::dbs::NoWriteFrame::ComputedField)
	}

	/// Set the evaluated VERSION timestamp for time-travel queries.
	///
	/// Used by `VersionScope` to propagate the version to downstream
	/// operators so that record dereferences and FETCH resolution honour
	/// the same timestamp as the source scan.
	pub fn with_version_stamp(self, version: Option<u64>) -> Self {
		if version == self.root().version_stamp {
			return self;
		}
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.version_stamp = version;
		new
	}

	/// Get the evaluated VERSION timestamp (if set).
	///
	/// Returns the version stamp set by `VersionScope`. Used by
	/// `fetch_record`, `fetch_record_no_perms`, and `FieldPart` to
	/// read records at the correct point in time.
	pub fn version_stamp(&self) -> Option<u64> {
		self.root().version_stamp
	}

	/// Create a new context with an additional parameter.
	///
	/// This is used by LET statements to add variables to the execution context.
	/// Creates a proper child FrozenContext, preserving the parent chain for
	/// correct scoped parameter lookup and shadowing.
	///
	/// The derived context runs the rest of the same program, so it keeps the
	/// session sink of this one. A context that runs other code with a name
	/// bound (a subquery's `$parent`, a cascade, a function's arguments) is
	/// derived with [`Self::with_params`], which holds none.
	pub fn with_param(&self, name: impl Into<Strand>, value: Value) -> Self {
		let mut bound = self.with_params([(name, value)]);
		bound.root_mut().session_effects.clone_from(&self.root().session_effects);
		bound
	}

	/// Derive a context whose CLOCK reads answer `clock`, the instant a statement of a block
	/// started at.
	///
	/// The derived context runs the rest of the same program, so it keeps the session sink of
	/// this one, as [`Self::with_param`] does.
	pub(crate) fn with_statement_clock(&self, clock: crate::val::Datetime) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.set_statement_clock(clock);
		let mut derived = self.with_new_ctx(child.freeze());
		derived.root_mut().session_effects.clone_from(&self.root().session_effects);
		derived
	}

	/// Derive a context in one child scope binding each of `names` to
	/// `value`, the one allocation every name shares.
	pub(crate) fn with_shared_param(&self, names: &[&str], value: &Arc<Value>) -> Self {
		let mut child = Context::new_child(self.ctx());
		for name in names {
			child.add_value(*name, Arc::clone(value));
		}
		self.with_new_ctx(child.freeze())
	}

	/// [`Self::with_params`], in a scope that also makes `units` the table the
	/// nested programs beneath it find their units in, when it is given.
	pub(crate) fn with_params_in<N: Into<Strand>>(
		&self,
		params: impl IntoIterator<Item = (N, Value)>,
		units: Option<crate::exec::vm::UnitTable>,
	) -> Self {
		let mut child = Context::new_child(self.ctx());
		for (name, value) in params {
			child.add_value(name, Arc::new(value));
		}
		if let Some(units) = units {
			child.set_vm_units(units);
		}
		self.with_new_ctx(child.freeze())
	}

	/// Derive a context in one child scope binding `name` to `value`, in
	/// which `cascade` is the edge cascade running.
	pub(crate) fn with_param_in_edge_cascade(
		&self,
		name: &str,
		value: Value,
		cascade: Arc<crate::exec::vm::program::Program>,
	) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.add_value(name, Arc::new(value));
		child.set_vm_edge_cascade(cascade);
		self.with_new_ctx(child.freeze())
	}

	/// The edge cascade running around this context, if any.
	pub(crate) fn vm_edge_cascade(&self) -> Option<&Arc<crate::exec::vm::program::Program>> {
		self.ctx().vm_edge_cascade()
	}

	/// Derive a context whose nested programs find their units in `units`.
	pub(crate) fn with_vm_units(&self, units: crate::exec::vm::UnitTable) -> Self {
		self.with_params_in(std::iter::empty::<(Strand, Value)>(), Some(units))
	}

	/// The unit table of the bundle executing around this context, if any.
	pub(crate) fn vm_units(&self) -> Option<&crate::exec::vm::UnitTable> {
		self.ctx().vm_units()
	}

	/// Derive a context whose reads of records by id go through the per-table
	/// units of `deref`.
	pub(crate) fn with_vm_deref(&self, deref: Arc<crate::exec::vm::Deref>) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.set_vm_deref(deref);
		self.with_new_ctx(child.freeze())
	}

	/// Derive a context whose program serves `served`, its late-bound
	/// statements, when a block reaches them.
	pub(crate) fn with_vm_served(&self, served: crate::exec::vm::ServedStatements) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.set_vm_served(served);
		self.with_new_ctx(child.freeze())
	}

	/// The late-bound statements the program executing around this context
	/// serves, if any.
	pub(crate) fn vm_served(&self) -> Option<&crate::exec::vm::ServedStatements> {
		self.ctx().vm_served()
	}

	/// Derive a context whose program records its counters into `metrics`.
	pub(crate) fn with_vm_metrics(
		&self,
		metrics: Arc<crate::exec::metrics::OperatorMetrics>,
	) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.set_vm_metrics(metrics);
		self.with_new_ctx(child.freeze())
	}

	/// Derive a context whose statement reports to the query trace `trace`.
	pub(crate) fn with_query_trace(&self, trace: Arc<crate::exec::trace::StatementTrace>) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.set_query_trace(trace);
		self.with_new_ctx(child.freeze())
	}

	/// The metrics the program running around this context records into, if
	/// its plan is analysed.
	pub(crate) fn vm_metrics(&self) -> Option<&Arc<crate::exec::metrics::OperatorMetrics>> {
		self.ctx().vm_metrics()
	}

	/// Derive a context whose closure invocations find their units in
	/// `closures`.
	pub(crate) fn with_vm_closures(
		&self,
		closures: Arc<crate::exec::vm::closure::ClosureUnits>,
	) -> Self {
		let mut child = Context::new_child(self.ctx());
		child.set_vm_closures(closures);
		self.with_new_ctx(child.freeze())
	}

	/// The closure-body units registered around this context, if any.
	pub(crate) fn vm_closures(&self) -> Option<&Arc<crate::exec::vm::closure::ClosureUnits>> {
		self.ctx().vm_closures()
	}

	/// The per-table units the record reads of this context go through, if
	/// the program executing around it holds any.
	pub(crate) fn vm_deref(&self) -> Option<&Arc<crate::exec::vm::Deref>> {
		self.ctx().vm_deref()
	}

	/// Create a new context with several additional parameters, bound in one
	/// child scope.
	///
	/// The same binding [`Self::with_param`] makes, for a caller that has a
	/// whole frame to bind — an event body's `$before` / `$after` / `$value` /
	/// `$event` / `$input`, say. One child scope rather than one per name: a
	/// frame built per record is on the write path, and every scope in the
	/// chain is walked by every parameter read inside it.
	///
	/// A name bound twice keeps the last binding, as a scope holding one entry
	/// per name does.
	pub(crate) fn with_params<N: Into<Strand>>(
		&self,
		params: impl IntoIterator<Item = (N, Value)>,
	) -> Self {
		self.with_params_in(params, None)
	}

	/// Suppress permission enforcement for a referential-integrity cascade,
	/// keeping the actor's own state for a nested scope to return to.
	///
	/// `Options::new_for_reference_cascade`'s counterpart. The cascade must be
	/// able to modify or delete the referencing record whatever the actor may
	/// do to it; that bypass is scoped to the referencing record, and nested
	/// work with its own permission contract calls
	/// [`ExecutionContext::with_caller_perms`] to undo it.
	///
	/// Not [`ExecutionContext::with_skip_fetch_perms`]: that frame also refuses
	/// the write, and a cascade's whole purpose is to perform one.
	pub(crate) fn for_reference_cascade(&self) -> Self {
		self.with_suppression(true, self.root().caller_perms_suppressed, |options| {
			options.new_for_reference_cascade()
		})
	}

	/// Restore the actor's own permission state, undoing a
	/// [`ExecutionContext::for_reference_cascade`] bypass.
	pub(crate) fn with_caller_perms(&self) -> Self {
		let caller = self.root().caller_perms_suppressed;
		self.with_suppression(caller, caller, |options| options.new_with_caller_perms())
	}

	/// Suppress permission enforcement for this frame and for everything
	/// nested inside it.
	///
	/// `Options::with_perms(false)`'s counterpart, and the difference from
	/// [`ExecutionContext::for_reference_cascade`] is the whole of what
	/// `caller_perms` names: this leaves nothing for a nested scope to return
	/// to.
	pub(crate) fn with_perms_suppressed(&self, suppressed: bool) -> Self {
		self.with_suppression(suppressed, suppressed, |options| options.new_with_perms(!suppressed))
	}

	/// The one writer of both suppression flags.
	///
	/// `mirror` is the `Options` constructor that says the same thing to code
	/// that reads the options, applied here for the reason
	/// [`ExecutionContext::with_limited_auth`] mirrors auth: a stored clause
	/// carried from inside this frame into a nested scope must land in the frame
	/// it was entered in. Each caller names its own rather than reconstructing
	/// one from the pair, because `Options` draws the same distinctions under
	/// its own names and those are the definitions of record.
	///
	/// `options_for_frame` rebuilds the permission-predicate and
	/// computed-field frames from scratch and leaves every other frame's
	/// options alone, so what is written here reaches the code that reads them, and a
	/// frame that is both is resolved there in favour of the stricter one.
	fn with_suppression(
		&self,
		suppressed: bool,
		caller: bool,
		mirror: impl FnOnce(&Options) -> Options,
	) -> Self {
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		// A frame that changes the permission contract (a cascade, a view body, an event) runs code
		// the statement does not own, never a statement of the request.
		if root.perms_suppressed != suppressed || root.caller_perms_suppressed != caller {
			root.session_effects = None;
		}
		root.perms_suppressed = suppressed;
		root.caller_perms_suppressed = caller;
		if let Some(options) = &root.options {
			root.options = Some(mirror(options));
		}
		new
	}

	/// A context detached from this one's parameters, over the same
	/// transaction and capabilities.
	///
	/// The subscriber frame's starting point: a notification is evaluated
	/// against the subscription's own captured variables and session, not the
	/// writer's, so the writer's parameter chain must not be inherited. The
	/// transaction is, because the clauses read through it.
	///
	/// Returned unfrozen so the caller adds its own bindings — the shadowing
	/// order is the caller's to get right — and then hands the frozen result
	/// to [`Self::with_replaced_principal`].
	pub(crate) fn detached_context(&self) -> Context {
		let mut detached = Context::background(self.ctx());
		detached.set_transaction(self.txn());
		detached
	}

	/// Run as `auth` over the detached `frozen`, rather than as the caller.
	///
	/// A replacement, not a narrowing: [`ExecutionContext::with_limited_auth`]
	/// cannot express it, because `Auth::new_limited` refuses to raise or to
	/// swap laterally. The live-query reduction is the caller — each
	/// subscription's clauses run under the authority stored with the
	/// subscription, which is unrelated to the writer's, over a context
	/// carrying that subscriber's own `$session` and captured parameters.
	///
	/// Enforcement is turned back on: a write may have reached the subscriber
	/// loop from a frame that suppressed it — a view body, an event body, an
	/// owner session — and each subscriber is owed the checks its own
	/// principal would face.
	///
	/// The session is re-derived from `frozen`, so `session::db()` and its kin
	/// answer for the subscriber, and the `Options` are rewritten, so a
	/// clause evaluated from here runs as the same principal rather than as the
	/// writer.
	pub(crate) fn with_replaced_principal(&self, frozen: FrozenContext, auth: &Arc<Auth>) -> Self {
		let mut new = self.with_new_ctx(frozen);
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.session = SessionInfo::from_context(&root.ctx).map(Arc::new);
		root.auth = Arc::clone(auth);
		root.skip_fetch_perms = false;
		root.perms_suppressed = false;
		root.caller_perms_suppressed = false;
		if let Some(options) = &root.options {
			root.options = Some(options.new_with_perms(true).with_auth(Arc::clone(auth)));
		}
		new
	}

	/// Create a new context with auth limited by the given `AuthLimit`.
	///
	/// This is used by user-defined function execution to cap the caller's
	/// privileges to the definer's auth level.
	pub fn with_limited_auth(&self, limit: &crate::iam::AuthLimit) -> Self {
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.auth = Arc::new(root.auth.new_limited(limit));
		root.session_effects = None;
		// The options carry the auth too, so code that reads them sees the
		// limited auth.
		if let Some(ref opts) = root.options {
			root.options = Some(opts.clone().with_auth(Arc::clone(&root.auth)));
		}
		new
	}

	/// Derive a context that runs as `auth`, which the caller has already
	/// narrowed by an `AUTH LIMIT` ([`crate::iam::Auth::new_limited`]): the
	/// same context under another principal, in the `Options` too, so code
	/// that reads them sees it.
	pub(crate) fn with_principal(&self, auth: &Arc<Auth>) -> Self {
		let mut new = self.clone();
		let root = new.root_mut();
		root.auth = Arc::clone(auth);
		root.session_effects = None;
		if let Some(options) = root.options.as_mut() {
			options.auth = Arc::clone(auth);
		}
		new
	}

	/// Create a new context at namespace level with the given namespace definition.
	///
	/// This is used by USE NS statements to switch namespace context.
	pub fn with_namespace(&self, ns: Arc<NamespaceDefinition>) -> Self {
		Self::Namespace(NamespaceContext {
			root: self.root().clone(),
			ns,
		})
	}

	/// Create a new context at database level with the given namespace and database definitions.
	///
	/// This is used by USE DB statements to switch database context.
	pub fn with_database(&self, ns: Arc<NamespaceDefinition>, db: Arc<DatabaseDefinition>) -> Self {
		Self::Database(DatabaseContext {
			ns_ctx: NamespaceContext {
				root: self.root().clone(),
				ns,
			},
			db,
			field_state_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			table_def_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			index_def_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			select_permission_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
		})
	}

	/// Create a new context with a different transaction.
	///
	/// This is used by BEGIN statements to create a write transaction.
	/// The new transaction replaces the existing one in the context by
	/// creating a child FrozenContext with the new transaction set.
	pub fn with_transaction(&self, txn: Arc<Transaction>) -> Result<Self, Error> {
		let mut child = Context::new_child(self.ctx());
		child.set_transaction(txn);
		Ok(self.with_new_ctx(child.freeze()))
	}

	/// Get the function registry: built-in scalar, aggregate, projection and index
	/// functions, plus the method-dispatch table. The root context holds it.
	///
	/// Built once per datastore and read off the underlying context, so a
	/// caller holding one of these and a caller holding only the context —
	/// the evaluator that owns the catalog writes — resolve a name against
	/// the same registry.
	pub fn function_registry(&self) -> &Arc<FunctionRegistry> {
		self.root().ctx.function_registry()
	}

	/// Get the session information (if available).
	pub fn session(&self) -> Option<&SessionInfo> {
		self.root().session.as_deref()
	}

	/// Get the capabilities as an Arc (delegates to FrozenContext).
	pub fn capabilities(&self) -> Arc<Capabilities> {
		// FrozenContext always has capabilities (defaults to Capabilities::default())
		self.root().ctx.get_capabilities()
	}

	/// Get the cancellation token.
	pub fn cancellation(&self) -> &CancellationToken {
		&self.root().cancellation
	}

	/// Get the evaluation `Options` (if available).
	///
	/// The builtin functions, scripting, schema statements and the write path
	/// read their permission flags, import mode and depth budget from here.
	pub fn options(&self) -> Option<&Options> {
		self.root().options.as_ref()
	}

	/// Check if the current auth is allowed to perform an action on a given resource
	pub fn is_allowed(&self, action: Action, res: ResourceKind, base: Base) -> anyhow::Result<()> {
		if let Some(options) = self.options() {
			self.ctx().is_allowed(options, action, res, base)
		} else {
			Ok(())
		}
	}
}

/// Turns the values a plan produced into the statement's result value.
///
/// The plan declares the shape and the number of values that arrived does not
/// get a vote: a consumer that forwards values as they are produced knows the
/// shape long before it knows the count, so the count cannot be what decides.
/// An operator that declares [`OutputShape::Scalar`] and then emits anything
/// but one value has a bug; reporting it is what keeps that bug from reaching
/// the caller as a silently rearranged result.
pub(crate) fn shape_statement_value(
	shape: crate::exec::OutputShape,
	mut values: Vec<Value>,
) -> anyhow::Result<Value> {
	use crate::exec::OutputShape;
	match shape {
		OutputShape::Scalar if values.len() == 1 => {
			Ok(values.pop().expect("values verified non-empty"))
		}
		OutputShape::Scalar => Err(anyhow::anyhow!(crate::err::EngineError::unreachable(format!(
			"A scalar plan produced {} values instead of one",
			values.len()
		)))),
		OutputShape::Rows => Ok(Value::Array(crate::val::Array::from(values))),
	}
}

/// The context a top-level statement runs in: a fresh snapshot of the request's context `ctx`
/// with `txn`, cancelled through `cancellation`, at the highest level the session's namespace and
/// database support. Fails where either is missing at or below `required_level`, the level the
/// plan requires. `sink` receives the session effects the statement's program records, and is
/// installed only for a statement of the request.
///
/// `session` is the information of the session `ctx` carries, and `opt` its options.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn statement_context(
	ctx: &FrozenContext,
	opt: &Options,
	session: Option<Arc<SessionInfo>>,
	txn: &Arc<Transaction>,
	cancellation: &CancellationToken,
	writes: bool,
	required_level: ContextLevel,
	sink: Option<Arc<SessionSink>>,
) -> crate::expr::FlowResult<ExecutionContext> {
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
	// Build the root context using cached session info. The context
	// snapshot must be fresh per-query because it contains the
	// transaction reference which changes between statements.
	let root_ctx = RootContext {
		ctx: Context::snapshot(ctx).freeze(),
		options: Some(opt.clone()),
		datastore: None,
		cancellation: Arc::new(cancellation.clone()),
		auth: Arc::clone(&opt.auth),
		session,
		current_value: None,
		current_row: None,
		forbidden_row: None,
		inherited_knn: None,
		skip_fetch_perms: false,
		perms_suppressed: false,
		caller_perms_suppressed: false,
		computing_record: None,
		no_write: None,
		version_stamp: None,
		guard_verdicts: Arc::new(crate::exec::guard_verdicts::GuardVerdicts::for_statement(writes)),
		writing: false,
		caller_view_checked: false,
		nested_runs: Default::default(),
		session_effects: None,
	};

	// The plan's declared context level is the minimum it needs (see
	// `ContextLevel`), not a construction target: materialize the highest
	// level the session's ns/db selection supports. A closure that
	// arrives as a runtime value (`LET $f = || (SELECT ...); $f()`)
	// hides its body from the planner, so a database-touching body must
	// find the database context already in place even though the plan
	// only declared root. Above the declared minimum the upgrade is
	// opportunistic — a session pointing at a missing ns/db falls back
	// to the declared level rather than failing a statement that never
	// needed more. At or below the minimum, resolution failures surface
	// exactly as before.
	//
	// SECURITY: `USE` never implicitly creates a namespace for a session that lacks
	// `DEFINE NAMESPACE`-equivalent authorization, so this must not paper over a
	// missing target by writing a fresh namespace into the catalog on the read path
	// (which would also fail on a read-only transaction). `expect_ns_by_name` /
	// `expect_db_by_name` surface a clean `NsNotFound` / `DbNotFound` instead.

	let ns_def = if required_level >= ContextLevel::Namespace {
		Some(txn.expect_ns_by_name(opt.ns()?).await?)
	} else if let Ok(ns_name) = opt.ns() {
		txn.expect_ns_by_name(ns_name).await.ok()
	} else {
		None
	};

	let db_def = match &ns_def {
		Some(_) if required_level >= ContextLevel::Database => {
			Some(txn.expect_db_by_name(opt.ns()?, opt.db()?).await?)
		}
		Some(_) => match (opt.ns(), opt.db()) {
			(Ok(ns_name), Ok(db_name)) => txn.expect_db_by_name(ns_name, db_name).await.ok(),
			_ => None,
		},
		None => None,
	};

	let exec_ctx = match (ns_def, db_def) {
		(Some(ns_def), Some(db_def)) => ExecutionContext::Database(DatabaseContext {
			ns_ctx: NamespaceContext {
				root: root_ctx,
				ns: ns_def,
			},
			db: db_def,
			field_state_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			table_def_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			index_def_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			select_permission_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
		}),
		(Some(ns_def), None) => ExecutionContext::Namespace(NamespaceContext {
			root: root_ctx,
			ns: ns_def,
		}),
		(None, _) => ExecutionContext::Root(root_ctx),
	};
	Ok(match sink {
		Some(sink) => exec_ctx.with_session_sink(sink),
		None => exec_ctx,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::eval_context::ComputingRecord;
	use crate::exec::operators::test_util::root_ctx_with_options;

	fn units() -> crate::exec::vm::UnitTable {
		Arc::from(Vec::new())
	}

	fn budget(ctx: &ExecutionContext) -> u32 {
		ctx.options().expect("options").dive
	}

	#[test]
	fn a_call_body_binds_its_parameters_and_row_in_one_scope() {
		let ctx = root_ctx_with_options();
		let row = Arc::new(Value::from("row"));
		let table = units();
		let params =
			[(Strand::from("x"), Value::from(1)), (Strand::from("this"), Value::from("param"))];
		let body = ctx
			.entering_call_body(params, Arc::clone(&table), RowAccess::Present(&row), false, None)
			.unwrap();
		assert_eq!(body.value("x"), Some(&Value::from(1)));
		// The row shadows a parameter of the same name, as the scope nested
		// over the parameters' did.
		assert_eq!(body.value("this"), Some(&Value::from("row")));
		assert_eq!(body.value("parent"), Some(&Value::from("row")));
		// One allocation: the scope's names and the document read it.
		assert!(Arc::ptr_eq(body.current_row_shared().expect("the row"), &row));
		assert!(Arc::ptr_eq(body.vm_units().expect("units"), &table));
		assert_eq!(budget(&body), budget(&ctx) - 1);
		// The caller's context is untouched.
		assert_eq!(ctx.value("x"), None);
		assert!(ctx.current_row().is_none());
		assert_eq!(budget(&ctx), budget(&root_ctx_with_options()));
	}

	#[test]
	fn a_call_body_never_reads_the_callers_bindings() {
		let ctx = root_ctx_with_options()
			.with_param("limit", Value::from(1000))
			.with_param("session", Value::from("the session"));
		let params = [(Strand::from("x"), Value::from(1))];
		let body = ctx
			.entering_call_body(params.clone(), units(), RowAccess::Absent, false, None)
			.unwrap();
		assert_eq!(body.lexical_value("x"), Some(&Value::from(1)));
		assert_eq!(body.lexical_value("limit"), None, "a caller's binding reached a body");
		assert_eq!(body.lexical_value("session"), Some(&Value::from("the session")));
		let nested = body
			.entering_call_body(params.clone(), units(), RowAccess::Absent, false, None)
			.unwrap();
		assert_eq!(nested.lexical_value("limit"), None);
		let frame = ctx.clone().privileged_frame();
		let body =
			frame.entering_call_body(params, units(), RowAccess::Absent, false, None).unwrap();
		assert_eq!(body.lexical_value("x"), Some(&Value::from(1)));
		assert_eq!(
			body.lexical_value("limit"),
			None,
			"a caller's binding reached a privileged frame"
		);
		assert_eq!(body.lexical_value("session"), Some(&Value::from("the session")));
		assert_eq!(ctx.value("limit"), Some(&Value::from(1000)));
		assert_eq!(ctx.lexical_value("limit"), Some(&Value::from(1000)));
	}

	#[test]
	fn a_call_body_binds_made_inside_it_are_read() {
		let ctx = root_ctx_with_options().with_param("limit", Value::from(1000));
		let params = [(Strand::from("x"), Value::from(1))];
		let body = ctx
			.entering_call_body(params, units(), RowAccess::Absent, false, None)
			.unwrap()
			.with_param("inner", Value::from(2));
		assert_eq!(body.lexical_value("inner"), Some(&Value::from(2)));
		assert_eq!(body.lexical_value("x"), Some(&Value::from(1)));
		assert_eq!(body.lexical_value("limit"), None);
	}

	#[test]
	fn a_call_body_without_a_row_keeps_the_enclosing_one() {
		let enclosing = Arc::new(Value::from("enclosing"));
		let ctx = root_ctx_with_options().with_current_row(Arc::clone(&enclosing));
		let params = [(Strand::from("x"), Value::from(1))];
		let body = ctx.entering_call_body(params, units(), RowAccess::Absent, false, None).unwrap();
		assert_eq!(body.value("x"), Some(&Value::from(1)));
		assert_eq!(body.value("this"), None);
		assert_eq!(body.value("parent"), None);
		assert!(Arc::ptr_eq(body.current_row_shared().expect("the row"), &enclosing));
	}

	#[test]
	fn a_call_body_carries_the_frame_of_the_call() {
		let ctx = root_ctx_with_options();
		let no_params = std::iter::empty::<(Strand, Value)>();
		let plain = ctx
			.entering_call_body(no_params.clone(), units(), RowAccess::Absent, false, None)
			.unwrap();
		assert!(!plain.root().skip_fetch_perms);
		assert!(plain.root().computing_record.is_none());
		let skipping =
			ctx.entering_call_body(no_params, units(), RowAccess::Absent, true, None).unwrap();
		assert!(skipping.root().skip_fetch_perms);
		assert!(!ctx.root().skip_fetch_perms);
	}

	#[test]
	fn a_call_body_is_refused_once_the_budget_is_spent() {
		let mut ctx = root_ctx_with_options();
		let ExecutionContext::Root(root) = &mut ctx else {
			unreachable!("a root context")
		};
		root.options.as_mut().expect("options").dive = 1;
		let no_params = std::iter::empty::<(Strand, Value)>();
		let body = ctx
			.entering_call_body(no_params.clone(), units(), RowAccess::Absent, false, None)
			.unwrap();
		assert_eq!(budget(&body), 0);
		assert_eq!(budget(&ctx), 1);
		let refused = body.entering_call_body(no_params, units(), RowAccess::Absent, false, None);
		assert!(refused.is_err());
		assert!(body.check_dive().is_err());
		assert!(ctx.check_dive().is_ok());
	}

	/// Whether `ctx` binds `name` to a KNN query vector's row being scanned.
	fn forbidden(ctx: &ExecutionContext, name: &str) -> bool {
		matches!(ctx.binding(name), Some(Binding::Forbidden(_)))
	}

	#[test]
	fn a_knn_vector_forbids_the_value_in_scope_and_binds_no_parameter() {
		let ctx = root_ctx_with_options().with_current_row(Arc::new(Value::from("enclosing")));
		let vector = ctx.with_forbidden_row(Arc::from("$v"));
		let state = vector.forbidden_row().expect("a forbidden row");
		assert!(state.covers_row());
		// The enclosing row is not the value in scope of the vector.
		assert!(vector.current_row().is_none());
		// `$parent` is not bound by the vector itself: an enclosing statement's
		// binding stays its own, and an unbound one falls back to the row.
		assert_eq!(vector.value("parent"), None);
		assert!(ctx.forbidden_row().is_none());
		let refusal = state.refusal().to_string();
		assert_eq!(
			refusal,
			"Invalid query: The KNN query vector cannot depend on the record being scanned: $v"
		);
	}

	#[test]
	fn a_statement_entered_from_a_knn_vector_binds_the_forbidden_row() {
		let vector = root_ctx_with_options().with_forbidden_row(Arc::from("$v"));
		// A SELECT has rows of its own; a block reads the enclosing row.
		let select = vector.forbidding_row_params(false).expect("a statement of the vector");
		assert!(forbidden(&select, "parent"));
		assert!(forbidden(&select, "this"));
		// A forbidden binding holds no value, and is no parameter collected.
		assert_eq!(select.value("parent"), None);
		assert!(!select.collect_params().contains_key("parent"));
		assert!(!select.forbidden_row().expect("the state").covers_row());
		let block = vector.forbidding_row_params(true).expect("a statement of the vector");
		assert!(block.forbidden_row().expect("the state").covers_row());
		// A statement nested in the SELECT binds that SELECT's row of its own.
		let nested = select.with_param("parent", Value::from("own row"));
		assert_eq!(nested.value("parent"), Some(&Value::from("own row")));
		assert!(forbidden(&nested, "this"));
		// A frame isolated from the statement's parameters does not see it.
		let isolated = select.with_new_ctx(Context::new_isolated(select.ctx()).freeze());
		assert!(isolated.binding("parent").is_none());
		// A copy of the chain carries it.
		let snapshot = select.with_new_ctx(Context::snapshot(select.ctx()).freeze());
		assert!(forbidden(&snapshot, "parent"));
		// Inside the SELECT, a statement entered with no row binds nothing new.
		assert!(select.forbidding_row_params(false).is_none());
		// Outside a vector, nothing changes.
		assert!(root_ctx_with_options().forbidding_row_params(true).is_none());
	}

	#[test]
	fn a_call_body_without_a_row_from_a_knn_vector_binds_the_forbidden_row() {
		let vector = root_ctx_with_options().with_forbidden_row(Arc::from("$v"));
		let params =
			[(Strand::from("x"), Value::from(1)), (Strand::from("parent"), Value::from(2))];
		let body = vector
			.entering_call_body(params, units(), vector.row_access(None), false, None)
			.unwrap();
		assert!(forbidden(&body, "this"));
		// An argument of the name reads as the row, as a body given one reads
		// that row over it.
		assert!(forbidden(&body, "parent"));
		assert_eq!(body.value("x"), Some(&Value::from(1)));
		assert!(body.forbidden_row().expect("the state").covers_row());
		// A body given a row binds that row.
		let row = Arc::new(Value::from("row"));
		let no_params = std::iter::empty::<(Strand, Value)>();
		let body = vector
			.entering_call_body(no_params, units(), RowAccess::Present(&row), false, None)
			.unwrap();
		assert_eq!(body.value("this"), Some(&Value::from("row")));
		assert_eq!(body.value("parent"), Some(&Value::from("row")));
	}

	#[test]
	fn the_enclosing_row_of_a_statement_entered_from_a_knn_vector_is_forbidden() {
		let vector = root_ctx_with_options().with_forbidden_row(Arc::from("$v"));
		let select = vector.forbidding_row_params(false).expect("a statement of the vector");
		let (row, forbidding) = select.enclosing_row();
		assert!(row.is_none());
		let forbidding = forbidding.expect("a context that forbids the value in scope");
		assert!(forbidding.forbidden_row().expect("the state").covers_row());
		let bound = select.with_param("this", Value::from("own row"));
		let (row, forbidding) = bound.enclosing_row();
		assert_eq!(row, Some(&Value::from("own row")));
		assert!(forbidding.is_none());
	}

	#[test]
	fn a_principal_replaces_the_auth_of_the_context_and_its_options() {
		let ctx = root_ctx_with_options();
		let narrowed = Arc::new(Auth::for_root(crate::iam::Role::Viewer));
		let limited = ctx.with_principal(&narrowed);
		assert!(Arc::ptr_eq(&limited.root().auth, &narrowed));
		assert!(Arc::ptr_eq(&limited.options().expect("options").auth, &narrowed));
		assert!(!Arc::ptr_eq(&ctx.root().auth, &narrowed));
	}
	#[test]
	fn a_sink_takes_the_effects_in_the_order_they_were_recorded() {
		let sink = SessionSink::default();
		sink.record(SessionEffect::Option {
			name: Strand::new("IMPORT"),
			what: true,
		});
		sink.record(SessionEffect::Use {
			ns: Some(Strand::new("a")),
			db: None,
		});
		sink.record(SessionEffect::Use {
			ns: None,
			db: Some(Strand::new("b")),
		});
		let taken = sink.take();
		assert_eq!(taken.len(), 3);
		assert!(matches!(
			&taken[0],
			SessionEffect::Option {
				what: true,
				..
			}
		));
		assert_eq!(
			taken[2],
			SessionEffect::Use {
				ns: None,
				db: Some(Strand::new("b")),
			}
		);
		assert!(sink.take().is_empty());
	}

	/// The sink belongs to the context of a top-level statement alone: every context derived to
	/// run other code holds none, so a program that runs there cannot change the caller's session.
	#[test]
	fn only_the_statement_context_holds_the_session_sink() {
		let ctx = root_ctx_with_options().with_session_sink(Arc::default());
		assert!(ctx.session_sink().is_some());
		// Binding a parameter for the rest of the same program keeps it; binding names for other
		// code (a subquery's `$parent`, a function's arguments, an event's `$before`) does not.
		assert!(ctx.with_param("p", Value::None).session_sink().is_some());
		assert!(ctx.with_params([("p", Value::None)]).session_sink().is_none());
		let isolated = Context::new_isolated(ctx.ctx()).freeze();
		assert!(ctx.with_new_ctx(Arc::clone(&isolated)).session_sink().is_none());
		let narrowed = Arc::new(Auth::for_root(crate::iam::Role::Viewer));
		assert!(ctx.with_principal(&narrowed).session_sink().is_none());
		assert!(ctx.with_replaced_principal(isolated, &narrowed).session_sink().is_none());
		let limit = crate::iam::AuthLimit::new(crate::iam::Level::No, None);
		assert!(ctx.with_limited_auth(&limit).session_sink().is_none());
		let body = ctx
			.entering_call_body(
				std::iter::empty::<(Strand, Value)>(),
				units(),
				RowAccess::Absent,
				false,
				None,
			)
			.expect("the call body is entered");
		assert!(body.session_sink().is_none());
		// A permission predicate, a cascade and a computed field run under another contract.
		assert!(ctx.clone().with_skip_fetch_perms(true).session_sink().is_none());
		assert!(ctx.with_perms_suppressed(true).session_sink().is_none());
		let record = Arc::new(ComputingRecord {
			rid: crate::val::RecordId::new(TableName::from("t"), 1_i64),
			stored: None,
		});
		assert!(ctx.clone().with_computing_record(Some(record)).session_sink().is_none());
		// A derivation that changes nothing is still the statement's own context.
		assert!(ctx.clone().with_skip_fetch_perms(false).session_sink().is_some());
		assert!(ctx.with_perms_suppressed(false).session_sink().is_some());
	}
}
