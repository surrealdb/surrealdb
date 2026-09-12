//! Hierarchical execution contexts for the stream executor.
//!
//! The context hierarchy provides type-safe access to resources at different levels:
//! - `RootContext`: Always available, wraps a FrozenContext with auth + session
//! - `NamespaceContext`: Root + namespace definition
//! - `DatabaseContext`: Namespace + database definition
//!
//! `FrozenContext` is the single source of truth for parameters, transactions,
//! capabilities, and all legacy context fields. `RootContext` adds only the fields
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
use crate::exec::function::FunctionRegistry;
use crate::exec::permission::action_is_checked;
use crate::exec::physical_expr::ComputingRecord;
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
/// - Legacy Options (for fallback to compute path)
/// - Function registry (name resolution for every planned function call)
#[derive(Clone)]
pub struct RootContext {
	/// The underlying FrozenContext -- single source of truth for
	/// params, txn, capabilities, and all legacy context fields.
	pub ctx: FrozenContext,
	/// Registry of built-in scalar, aggregate, projection and index
	/// functions, plus the method-dispatch table.
	///
	/// Built once per datastore and shared by `Arc`; every planner and every
	/// function-call physical expression resolves names through it. It lives
	/// on the root so that the whole hierarchy — namespace, database, and
	/// every child derived by `with_new_ctx` or `Clone` — sees the same
	/// registry.
	pub(crate) function_registry: Arc<FunctionRegistry>,
	/// Legacy Options for fallback to compute path when streaming executor
	/// encounters unimplemented expressions.
	/// Remove this when the streaming executor has full coverage.
	pub options: Option<Options>,
	/// The underlying datastore (optional - only needed for root-level operations
	/// like INFO FOR ROOT, DEFINE USER ON ROOT, etc.)
	///
	/// Note: This is None when executing from a borrowed Datastore reference.
	/// Root-level operations will need to handle this case.
	pub datastore: Option<Arc<Datastore>>,
	/// Cancellation token for cooperative cancellation in the streaming executor
	pub cancellation: CancellationToken,
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
	/// a scalar without a row of their own (`ExprPlan`, a `FOR` range).
	/// `RecursePart` sets it too, so the recursion operator can evaluate the
	/// recursed path's expressions against the idiom's document root. A
	/// relational plan never sets it: its scans supply their own rows.
	pub(crate) current_row: Option<Arc<Value>>,
	/// When true, RecordId dereferences bypass permission checks.
	///
	/// Propagated from `EvalContext` at subquery / lookup / recursion boundaries
	/// so that permission predicate evaluation does not re-enter table permissions
	/// and recurse infinitely on cyclic record links.
	pub(crate) skip_fetch_perms: bool,
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
	/// When true, a write reached from here is rejected.
	///
	/// Set while evaluating a `COMPUTED` field body, which runs on every read of
	/// the field and under the definer's auth. The streaming engine plans no
	/// writes of its own, so enforcement is the legacy bridge's
	/// (`operators::legacy_bridge::legacy_options_for_frame`); this flag is
	/// what tells it.
	pub(crate) computing_field: bool,
	/// Evaluated VERSION timestamp for time-travel queries.
	///
	/// Set by the `VersionScope` operator when a SELECT has a VERSION clause.
	/// Read by `fetch_record`, `fetch_record_no_perms`, and `FieldPart` so
	/// that record dereferences and FETCH resolution honour the same version
	/// as the source scan.
	pub(crate) version_stamp: Option<u64>,
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
			.field("computing_field", &self.computing_field)
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
		use crate::catalog::providers::TableProvider;
		if version.is_none() {
			// Check execution-level cache (read lock — concurrent reads allowed)
			let cache = self.table_def_cache.read().await;
			if let Some(cached) = cache.get(table) {
				return Ok(cached.clone());
			}
		}

		// Cache miss or versioned lookup — go to the transaction
		let txn = self.txn();
		let result =
			txn.get_tb_by_name(&self.ns_ctx.ns.name, &self.db.name, table, version).await?;

		if version.is_none() {
			// Populate cache (write lock — brief exclusive access)
			self.table_def_cache.write().await.insert(table.clone(), result.clone());
		}

		Ok(result)
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
		let result = txn
			.all_tb_indexes(self.ns_ctx.ns.namespace_id, self.db.database_id, table, version)
			.await?;

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
	if root.skip_fetch_perms {
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
	/// need direct access to the legacy compute context.
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
	/// The one gate every permission check in the streaming engine consults:
	/// the scan, graph, reference and join operators through
	/// [`DatabaseContext::should_check_perms`], and the reads of a `DEFINE
	/// FUNCTION`, `DEFINE PARAM`, module or model `PERMISSIONS` clause
	/// directly. Returns `false`, bypassing every check, when:
	///
	/// - the context is inside a permission predicate (`skip_fetch_perms`). A definer-authored
	///   predicate reads freely, as the legacy engine evaluates one with `perms = false`; exempting
	///   it here disables the whole-scan `Deny` short-circuits, the per-row table and field
	///   permission filtering, edge and target enforcement, and the function and parameter
	///   permission checks in one place;
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
				function_registry: Arc::clone(&r.function_registry),
				options: r.options.clone(),
				datastore: r.datastore.clone(),
				cancellation: r.cancellation.clone(),
				auth: Arc::clone(&r.auth),
				session: r.session.clone(),
				current_value: r.current_value.clone(),
				current_row: r.current_row.clone(),
				skip_fetch_perms: r.skip_fetch_perms,
				computing_record: r.computing_record.clone(),
				computing_field: r.computing_field,
				version_stamp: r.version_stamp,
			}),
			Self::Namespace(n) => Self::Namespace(NamespaceContext {
				root: RootContext {
					ctx,
					function_registry: Arc::clone(&n.root.function_registry),
					options: n.root.options.clone(),
					datastore: n.root.datastore.clone(),
					cancellation: n.root.cancellation.clone(),
					auth: Arc::clone(&n.root.auth),
					session: n.root.session.clone(),
					current_value: n.root.current_value.clone(),
					current_row: n.root.current_row.clone(),
					skip_fetch_perms: n.root.skip_fetch_perms,
					computing_record: n.root.computing_record.clone(),
					computing_field: n.root.computing_field,
					version_stamp: n.root.version_stamp,
				},
				ns: Arc::clone(&n.ns),
			}),
			Self::Database(d) => Self::Database(DatabaseContext {
				ns_ctx: NamespaceContext {
					root: RootContext {
						ctx,
						function_registry: Arc::clone(&d.ns_ctx.root.function_registry),
						options: d.ns_ctx.root.options.clone(),
						datastore: d.ns_ctx.root.datastore.clone(),
						cancellation: d.ns_ctx.root.cancellation.clone(),
						auth: Arc::clone(&d.ns_ctx.root.auth),
						session: d.ns_ctx.root.session.clone(),
						current_value: d.ns_ctx.root.current_value.clone(),
						current_row: d.ns_ctx.root.current_row.clone(),
						skip_fetch_perms: d.ns_ctx.root.skip_fetch_perms,
						computing_record: d.ns_ctx.root.computing_record.clone(),
						computing_field: d.ns_ctx.root.computing_field,
						version_stamp: d.ns_ctx.root.version_stamp,
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

	/// Derive a context that skips permission checks on RecordId dereferences.
	///
	/// Used at subquery / lookup / recursion boundaries when the parent
	/// `EvalContext` has `skip_fetch_perms` set (i.e., we are inside a
	/// permission predicate evaluation).
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

	/// Enter a compiled body one re-entry deeper, consuming one unit of the
	/// computation-depth budget.
	///
	/// The budget is the dive of the [`Options`] this context carries — the
	/// same count the legacy evaluator decrements per expression — so one
	/// bound, `max_computation_depth`, covers a chain of re-entries whichever
	/// engine each hop runs on: a compiled closure that invokes itself, a body
	/// that hands a sub-computation to the legacy evaluator (which starts from
	/// the reduced budget), or a legacy statement that calls back into a
	/// compiled closure (whose context is rebuilt from the legacy options). The
	/// count lives here rather than on an `EvalContext` because the operators a
	/// body's blocks and subqueries run through rebuild their `EvalContext`
	/// from this context, and a count kept there would restart at each of
	/// them. Fails with [`ExecError::ComputationDepthExceeded`] once the budget
	/// is spent; a context built without options — only a test harness builds
	/// one — has no budget and is returned unchanged.
	pub(crate) fn dive(&self) -> anyhow::Result<Self> {
		let Some(options) = self.options() else {
			return Ok(self.clone());
		};
		let options = options.dive(1).map_err(anyhow::Error::new)?;
		let mut new = self.clone();
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.options = Some(options);
		Ok(new)
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

	/// Derive a context in which a write is rejected.
	///
	/// Used while evaluating a `COMPUTED` field body. See
	/// [`RootContext::computing_field`].
	pub fn computing_field(self) -> Self {
		if self.root().computing_field {
			return self;
		}
		let mut new = self;
		let root = match &mut new {
			Self::Root(r) => r,
			Self::Namespace(n) => &mut n.root,
			Self::Database(d) => &mut d.ns_ctx.root,
		};
		root.computing_field = true;
		new
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
		let mut child = Context::new_child(self.ctx());
		child.add_value(name, Arc::new(value));
		self.with_new_ctx(child.freeze())
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
		// Also update the legacy Options if present, so fallback compute
		// sees the limited auth.
		if let Some(ref opts) = root.options {
			root.options = Some(opts.clone().with_auth(Arc::clone(&root.auth)));
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
	pub fn function_registry(&self) -> &Arc<FunctionRegistry> {
		&self.root().function_registry
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

	/// Get the legacy Options (if available).
	///
	/// This is used for fallback to the legacy compute path when the streaming
	/// executor encounters unimplemented expressions.
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
