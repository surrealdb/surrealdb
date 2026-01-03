//! Hierarchical execution contexts for the stream executor.
//!
//! The context hierarchy provides type-safe access to resources at different levels:
//! - `RootContext`: Always available, contains datastore, params, cancellation
//! - `NamespaceContext`: Root + namespace definition + transaction
//! - `DatabaseContext`: Namespace + database definition
//!
//! Operators declare their minimum required context level via `ExecutionPlan::required_context()`,
//! and the executor validates requirements before execution begins.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
use crate::err::Error;
use crate::iam::{Action, Auth};
use crate::kvs::{Datastore, Transaction};
use crate::val::Value;

/// Parameters passed to queries (e.g., `$param` values).
pub(crate) type Parameters = HashMap<Cow<'static, str>, Arc<Value>>;

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
	pub fn short_name(&self) -> &'static str {
		match self {
			Self::Root => "Rt",
			Self::Namespace => "Ns",
			Self::Database => "Db",
		}
	}
}

/// Root-level context - always available.
///
/// Contains resources that don't require a namespace or database selection:
/// - Datastore access (optional, for root-level operations)
/// - Query parameters
/// - Cancellation token
/// - Authentication context
#[derive(Clone)]
pub struct RootContext {
	/// The underlying datastore (optional - only needed for root-level operations
	/// like INFO FOR ROOT, DEFINE USER ON ROOT, etc.)
	///
	/// Note: This is None when executing from a borrowed Datastore reference.
	/// Root-level operations will need to handle this case.
	pub datastore: Option<Arc<Datastore>>,
	/// Query parameters ($param values)
	pub params: Arc<Parameters>,
	/// Cancellation token for cooperative cancellation
	pub cancellation: CancellationToken,
	/// Authentication context for the current session
	pub auth: Arc<Auth>,
	/// Whether authentication is enabled on the datastore
	pub auth_enabled: bool,
	/// The transaction for this execution
	pub txn: Arc<Transaction>,
}

impl std::fmt::Debug for RootContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("RootContext")
			.field("datastore", &self.datastore.as_ref().map(|_| "<Datastore>"))
			.field("params", &self.params)
			.field("cancellation", &self.cancellation)
			.field("auth", &self.auth)
			.field("auth_enabled", &self.auth_enabled)
			.field("txn", &"<Transaction>")
			.finish()
	}
}

/// Namespace-level context - root + namespace.
///
/// Contains everything from RootContext plus:
/// - Namespace definition
/// - Transaction (created at namespace level)
#[derive(Clone)]
pub struct NamespaceContext {
	/// Root context (datastore, params, cancellation)
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

	/// Get the transaction
	pub fn txn(&self) -> &Arc<Transaction> {
		&self.root.txn
	}

	/// Get the parameters
	pub fn params(&self) -> &Parameters {
		&self.root.params
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
}

impl std::fmt::Debug for DatabaseContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DatabaseContext")
			.field("ns_ctx", &self.ns_ctx)
			.field("db", &self.db)
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

	/// Get the namespace definition
	pub fn ns(&self) -> &NamespaceDefinition {
		&self.ns_ctx.ns
	}

	/// Get the transaction
	pub fn txn(&self) -> &Transaction {
		self.ns_ctx.txn()
	}

	/// Get the parameters
	pub fn params(&self) -> &Parameters {
		self.ns_ctx.params()
	}

	/// Get the datastore (if available)
	pub fn datastore(&self) -> Option<&Datastore> {
		self.ns_ctx.datastore()
	}
}

/// Unified execution context - a discriminated union of all context levels.
///
/// Operators receive this enum and use typed accessor methods to get the
/// appropriate context level. The accessors return `Result` for levels that
/// may not be available, providing runtime safety in addition to the
/// compile-time safety from the typed context structs.
#[derive(Clone)]
pub enum ExecutionContext {
	/// Root-level context (no ns/db selected)
	Root(RootContext),
	/// Namespace-level context (ns selected, no db)
	Namespace(NamespaceContext),
	/// Database-level context (both ns and db selected)
	Database(DatabaseContext),
}

impl std::fmt::Debug for ExecutionContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Root(r) => f.debug_tuple("Root").field(r).finish(),
			Self::Namespace(n) => f.debug_tuple("Namespace").field(n).finish(),
			Self::Database(d) => f.debug_tuple("Database").field(d).finish(),
		}
	}
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
			Self::Root(_) => Err(Error::NsEmpty),
			Self::Namespace(n) => Ok(n),
			Self::Database(d) => Ok(&d.ns_ctx),
		}
	}

	/// Get database-level access (may fail if root or namespace only).
	///
	/// Returns an error if no database has been selected.
	pub fn database(&self) -> Result<&DatabaseContext, Error> {
		match self {
			Self::Root(_) | Self::Namespace(_) => Err(Error::DbEmpty),
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

	/// Get the transaction.
	pub fn txn(&self) -> &Arc<Transaction> {
		match self {
			Self::Root(r) => &r.txn,
			Self::Namespace(n) => &n.root.txn,
			Self::Database(d) => &d.ns_ctx.root.txn,
		}
	}

	/// Get the parameters (always available).
	pub fn params(&self) -> &Parameters {
		&self.root().params
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
		self.root().auth_enabled
	}

	/// Check if permissions should be checked for the given action.
	///
	/// This mirrors the logic in `Options::check_perms()` but adapted for
	/// the execution context. Returns `true` if permission checks should
	/// be performed, `false` if they should be bypassed.
	///
	/// Permission checks are bypassed when:
	/// - Auth is disabled and user is anonymous
	/// - User has sufficient role (Editor for Edit, Viewer for View) AND the target database is
	///   within the user's auth level
	pub fn should_check_perms(&self, action: Action) -> Result<bool, Error> {
		let root = self.root();

		// Check if server auth is disabled
		if !root.auth_enabled && root.auth.is_anon() {
			return Ok(false);
		}

		// For database-level operations, check if we can bypass based on role and level
		if let Ok(db_ctx) = self.database() {
			let ns = db_ctx.ns_name();
			let db = db_ctx.db_name();

			match action {
				Action::Edit => {
					let allowed = root.auth.has_editor_role();
					let db_in_actor_level = root.auth.is_root()
						|| root.auth.is_ns_check(ns)
						|| root.auth.is_db_check(ns, db);
					Ok(!allowed || !db_in_actor_level)
				}
				Action::View => {
					let allowed = root.auth.has_viewer_role();
					let db_in_actor_level = root.auth.is_root()
						|| root.auth.is_ns_check(ns)
						|| root.auth.is_db_check(ns, db);
					Ok(!allowed || !db_in_actor_level)
				}
			}
		} else {
			// Without database context, we can't do the full check
			// Default to requiring permission checks
			Ok(true)
		}
	}
}
