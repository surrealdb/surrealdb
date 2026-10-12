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
//!
//! Note: Parts of this module are work-in-progress for the hierarchical context model.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_strand::Strand;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::catalog::{DatabaseDefinition, IndexDefinition, NamespaceDefinition, TableDefinition};
use crate::ctx::{Context, FrozenContext};
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
		let strand = |path: &[crate::expr::Part]| match session.pick(path) {
			Value::String(s) => Some(s),
			_ => None,
		};
		let value = |path: &[crate::expr::Part]| match session.pick(path) {
			Value::None => None,
			v => Some(v),
		};
		Some(Self {
			ns: strand(NS.as_ref()),
			db: strand(DB.as_ref()),
			id: match session.pick(ID.as_ref()) {
				Value::Uuid(u) => Some(*u.as_ref()),
				_ => None,
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
	/// Root-level operations will need to handle this case.
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
	/// explicit DAG input binding -- operators read it via `CurrentValueSource`,
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
	/// free of observable side effects (GHSA-66r2-5gwj-gxm2), and a `COMPUTED`
	/// body runs on every read of the field and under the definer's auth. Both
	/// are enforced twice, because a write can be reached by either route: the
	/// write path refuses a statement it runs under the frame, and
	/// `frame::options_for_frame` mirrors the frame into `Options::no_write`
	/// for code that reaches the datastore through the options, such as a
	/// Surrealism module call.
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
	/// record in it and fails with `TbNotFound` where the answer is yes.
	///
	/// An absence the execution-level cache holds is read again from the
	/// transaction before it is believed: a statement block that implicitly
	/// defines the table after an earlier read cached its absence leaves the
	/// cache behind the catalog, and a read must not fail for a table the
	/// block itself defined. The repeat is paid only by a read that is about
	/// to fail.
	pub(crate) async fn table_is_undefined(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> anyhow::Result<bool> {
		if self.get_table_def(table, version).await?.is_some() {
			return Ok(false);
		}
		let current = crate::exec::vm::descriptor::definition(self, table, version).await?;
		if version.is_none() {
			self.table_def_cache.write().await.insert(table.clone(), current.clone());
		}
		Ok(current.is_none())
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
			// An index a newer release wrote is one this scan cannot choose.
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
				skip_fetch_perms: r.skip_fetch_perms,
				perms_suppressed: r.perms_suppressed,
				caller_perms_suppressed: r.caller_perms_suppressed,
				computing_record: r.computing_record.clone(),
				no_write: r.no_write,
				version_stamp: r.version_stamp,
				guard_verdicts: Arc::clone(&r.guard_verdicts),
				writing: r.writing,
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
					skip_fetch_perms: n.root.skip_fetch_perms,
					perms_suppressed: n.root.perms_suppressed,
					caller_perms_suppressed: n.root.caller_perms_suppressed,
					computing_record: n.root.computing_record.clone(),
					no_write: n.root.no_write,
					version_stamp: n.root.version_stamp,
					guard_verdicts: Arc::clone(&n.root.guard_verdicts),
					writing: n.root.writing,
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
						skip_fetch_perms: d.ns_ctx.root.skip_fetch_perms,
						perms_suppressed: d.ns_ctx.root.perms_suppressed,
						caller_perms_suppressed: d.ns_ctx.root.caller_perms_suppressed,
						computing_record: d.ns_ctx.root.computing_record.clone(),
						no_write: d.ns_ctx.root.no_write,
						version_stamp: d.ns_ctx.root.version_stamp,
						guard_verdicts: Arc::clone(&d.ns_ctx.root.guard_verdicts),
						writing: d.ns_ctx.root.writing,
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
	/// wins on both sides of the bridge without the flags being coupled.
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
		new
	}

	/// The context a privileged frame evaluates in: code a definer wrote — a
	/// permission, a field's `VALUE` or `ASSERT`, a computed field, an event,
	/// a gate — which runs under the definer's authority rather than the
	/// caller's bindings. It holds the session's own values (`$auth`,
	/// `$access`, `$token`, `$session`, read through the parent chain), the
	/// database's parameters, and whatever the frame binds itself after this;
	/// no query parameter, session `LET`, `LET`, `FOR` variable or closure of
	/// the caller is visible in it, nor in any context derived from it, so a
	/// function called from the frame is isolated too.
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
	/// The scope is a child of `self`'s, so the body reads the caller's
	/// bindings — its `LET`s, `FOR` variables, query parameters and session
	/// `LET`s — unless `self` is a privileged frame ([`Self::privileged_frame`]),
	/// which holds none of them. The scope binds the parameters first, so
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
		row: Option<&Arc<Value>>,
		skip_fetch_perms: bool,
		computing_record: Option<&Arc<ComputingRecord>>,
	) -> anyhow::Result<Self> {
		self.check_dive()?;
		let mut child = Context::new_child(self.ctx());
		for (name, value) in params {
			child.add_value(name, Arc::new(value));
		}
		child.set_vm_units(units);
		if let Some(row) = row {
			child.add_value("parent", Arc::clone(row));
			child.add_value("this", Arc::clone(row));
		}
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
		new
	}

	/// Derive a context in which a write is refused, naming the frame that
	/// refuses it.
	///
	/// A frame already refusing a write is left alone: the two frames refuse
	/// alike, so which of them a nested one names changes nothing, and the
	/// outer one is the one the caller entered. See [`RootContext::no_write`].
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
		new
	}

	/// Derive a context for code a write statement reaches
	/// ([`RootContext::writing`]).
	pub(crate) fn in_write(self) -> Self {
		if self.root().writing {
			return self;
		}
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.writing = true;
		new
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
	pub fn with_param(&self, name: impl Into<Strand>, value: Value) -> Self {
		self.with_params([(name, value)])
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
		let mut child = Context::new_child(self.ctx());
		for (name, value) in params {
			child.add_value(name, Arc::new(value));
		}
		self.with_new_ctx(child.freeze())
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
	/// carried across the bridge from inside this frame must land in the frame
	/// it was entered in. Each caller names its own rather than reconstructing
	/// one from the pair, because `Options` draws the same distinctions under
	/// its own names and those are the definitions of record.
	///
	/// `options_for_frame` rebuilds the permission-predicate and
	/// computed-field frames from scratch and leaves every other frame's
	/// options alone, so what is written here survives to the bridge — and a
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

	/// Get the function registry.
	///
	/// Returns the registry held by the root context, which every level of
	/// the hierarchy shares. Carrying it per-root allows different roots to
	/// have different registries, enabling custom function registration
	/// (e.g., enterprise-only functions).
	/// Registry of built-in scalar, aggregate, projection and index
	/// functions, plus the method-dispatch table.
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

#[cfg(test)]
mod tests {
	use super::*;
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
		let body =
			ctx.entering_call_body(params, Arc::clone(&table), Some(&row), false, None).unwrap();
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
	fn a_call_body_sees_the_callers_bindings_outside_a_privileged_frame() {
		let ctx = root_ctx_with_options()
			.with_param("limit", Value::from(1000))
			.with_param("session", Value::from("the session"));
		let params = [(Strand::from("x"), Value::from(1))];
		let body = ctx.entering_call_body(params.clone(), units(), None, false, None).unwrap();
		assert_eq!(body.value("x"), Some(&Value::from(1)));
		assert_eq!(body.value("limit"), Some(&Value::from(1000)));
		let frame = ctx.clone().privileged_frame();
		let body = frame.entering_call_body(params, units(), None, false, None).unwrap();
		assert_eq!(body.value("x"), Some(&Value::from(1)));
		assert_eq!(body.value("limit"), None, "a caller's binding reached a privileged frame");
		assert_eq!(body.value("session"), Some(&Value::from("the session")));
		assert_eq!(ctx.value("limit"), Some(&Value::from(1000)));
	}

	#[test]
	fn a_call_body_without_a_row_keeps_the_enclosing_one() {
		let enclosing = Arc::new(Value::from("enclosing"));
		let ctx = root_ctx_with_options().with_current_row(Arc::clone(&enclosing));
		let params = [(Strand::from("x"), Value::from(1))];
		let body = ctx.entering_call_body(params, units(), None, false, None).unwrap();
		assert_eq!(body.value("x"), Some(&Value::from(1)));
		assert_eq!(body.value("this"), None);
		assert_eq!(body.value("parent"), None);
		assert!(Arc::ptr_eq(body.current_row_shared().expect("the row"), &enclosing));
	}

	#[test]
	fn a_call_body_carries_the_frame_of_the_call() {
		let ctx = root_ctx_with_options();
		let no_params = std::iter::empty::<(Strand, Value)>();
		let plain = ctx.entering_call_body(no_params.clone(), units(), None, false, None).unwrap();
		assert!(!plain.root().skip_fetch_perms);
		assert!(plain.root().computing_record.is_none());
		let skipping = ctx.entering_call_body(no_params, units(), None, true, None).unwrap();
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
		let body = ctx.entering_call_body(no_params.clone(), units(), None, false, None).unwrap();
		assert_eq!(budget(&body), 0);
		assert_eq!(budget(&ctx), 1);
		let refused = body.entering_call_body(no_params, units(), None, false, None);
		assert!(refused.is_err());
		assert!(body.check_dive().is_err());
		assert!(ctx.check_dive().is_ok());
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
}
