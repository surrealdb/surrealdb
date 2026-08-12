//! Per-MCP-session state wrapping the SurrealDB Datastore and Session.

use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::{QueryResult, QueryResultBuilder};
use surrealdb_types::Variables;
use tokio::sync::RwLock;

use crate::cnf::McpConfig;
use crate::error::Error;

/// Per-MCP-session state holding the shared datastore and mutable session context.
pub struct McpSession {
	ds: Arc<Datastore>,
	session: RwLock<Session>,
	/// MCP-side runtime configuration (caps, timeouts, ...). Cloned from
	/// the parent [`crate::service::McpService`] at session-init time so
	/// every handler reads its caps off the running configuration rather
	/// than from a process-global static.
	config: Arc<McpConfig>,
}

impl McpSession {
	/// Construct a session backed by [`McpConfig::default`]. Used by
	/// in-crate tests that don't care about caps; production callers go
	/// through [`Self::with_config`] via the service layer.
	pub fn new(ds: Arc<Datastore>, session: Session) -> Self {
		Self::with_config(ds, session, Arc::new(McpConfig::default()))
	}

	/// Construct a session bound to an explicit [`McpConfig`]. The
	/// service layer always uses this so caps are taken from the
	/// configuration loaded at service construction.
	pub fn with_config(ds: Arc<Datastore>, session: Session, config: Arc<McpConfig>) -> Self {
		Self {
			ds,
			session: RwLock::new(session),
			config,
		}
	}

	/// Borrow the MCP configuration this session was constructed with.
	pub(crate) fn config(&self) -> &McpConfig {
		&self.config
	}

	/// Execute a SurrealQL query with optional typed variable bindings.
	///
	/// Both top-level failures (parse errors, capability denials, transaction
	/// cancellation) and statement-level failures are returned in-band as
	/// [`QueryResult`]s with an `Err` result. This keeps the tool-layer
	/// formatting uniform: every outcome the LLM cares about surfaces as a
	/// [`rmcp::model::CallToolResult`] with `is_error = true`, not as a
	/// JSON-RPC error. JSON-RPC errors are reserved for protocol-level faults
	/// (unknown tool, malformed params, MCP session not initialized).
	pub async fn execute(
		&self,
		query: &str,
		vars: Option<Variables>,
	) -> Result<Vec<QueryResult>, Error> {
		let session = self.session.read().await;
		self.execute_with_session(&session, query, vars).await
	}

	/// Execute a GQL (ISO/IEC 39075) query with optional typed variable
	/// bindings, against the namespace and database selected on the session.
	///
	/// Mirrors [`Self::execute`]: the query capability is checked for the
	/// subject and top-level failures are returned in-band as error
	/// [`QueryResult`]s. GQL is an experimental capability; when it is not
	/// enabled the datastore returns that as the (in-band) error here, so the
	/// `gql` tool degrades to a clear message rather than disappearing.
	pub async fn execute_gql(
		&self,
		query: &str,
		vars: Option<Variables>,
	) -> Result<Vec<QueryResult>, Error> {
		let session = self.session.read().await;
		if !self.ds.allows_query_by_subject(session.au.as_ref()) {
			let err = surrealdb_types::Error::query(
				"Capabilities denied this query for the current subject".to_string(),
				None,
			);
			return Ok(vec![QueryResultBuilder::started_now().finish_with_result(Err(err))]);
		}
		self.run_to_results(self.ds.execute_gql(query, &session, vars)).await
	}

	/// Execute a GraphQL request against the namespace and database selected on
	/// the session, returning the GraphQL response envelope (`{ data, errors }`)
	/// as plain JSON.
	///
	/// `variables` is a plain-JSON object (or `Null`). Capability denial,
	/// schema-generation failures (no namespace/database, GraphQL not
	/// configured), and timeouts are returned as the `Err` message for the
	/// tool layer to surface in-band; GraphQL execution errors ride back inside
	/// the returned envelope.
	pub async fn execute_graphql(
		&self,
		query: &str,
		variables: serde_json::Value,
		operation: Option<String>,
	) -> Result<serde_json::Value, String> {
		// Clone the session so the read lock is not held across schema
		// generation and request execution.
		let session = self.session.read().await.clone();
		if !self.ds.allows_query_by_subject(session.au.as_ref()) {
			return Err("Capabilities denied this query for the current subject".to_string());
		}
		let fut = surrealdb_core::graphql::execute_request(
			&self.ds,
			&session,
			query.to_string(),
			variables,
			operation,
		);
		let outcome = match self.config.query_timeout {
			Some(dur) => tokio::time::timeout(dur, fut).await.map_err(|_elapsed| {
				format!(
					"MCP GraphQL request exceeded the {}s timeout (set SURREAL_MCP_QUERY_TIMEOUT_SECS=0 to disable)",
					dur.as_secs()
				)
			})?,
			None => fut.await,
		};
		outcome.map_err(|e| e.to_string())
	}

	/// Execute `query` scoped to an explicit `(namespace, database)` without
	/// mutating the MCP session's own `use` state.
	///
	/// Used by resource handlers whose URIs embed a fully qualified target
	/// (e.g. `surrealdb://schema/ns/{ns}/db/{db}/table/{table}`). Resolving
	/// these through the caller's live session would mean a client that
	/// caches by URI could get schema from the wrong namespace after a
	/// `use` switch, so we clone the session and override the context for
	/// just this call. Auth, variables, and other session state are
	/// preserved.
	pub async fn execute_in(
		&self,
		ns: &str,
		db: &str,
		query: &str,
		vars: Option<Variables>,
	) -> Result<Vec<QueryResult>, Error> {
		let scoped = {
			let base = self.session.read().await;
			let mut scoped = base.clone();
			scoped.ns = Some(ns.to_string());
			scoped.db = Some(db.to_string());
			scoped
		};
		self.execute_with_session(&scoped, query, vars).await
	}

	/// Shared core for [`execute`] and [`execute_in`]: applies the outer
	/// [`McpConfig::query_timeout`] and normalises top-level failures into
	/// an in-band error [`QueryResult`].
	async fn execute_with_session(
		&self,
		session: &Session,
		query: &str,
		vars: Option<Variables>,
	) -> Result<Vec<QueryResult>, Error> {
		if !self.ds.allows_query_by_subject(session.au.as_ref()) {
			let err = surrealdb_types::Error::query(
				"Capabilities denied this query for the current subject".to_string(),
				None,
			);
			return Ok(vec![QueryResultBuilder::started_now().finish_with_result(Err(err))]);
		}

		self.run_to_results(self.ds.execute(query, session, vars)).await
	}

	/// Run a datastore query future under the configured
	/// [`McpConfig::query_timeout`], normalising both a timeout and a top-level
	/// execution failure into an in-band error [`QueryResult`]. Shared by the
	/// SurrealQL ([`Self::execute`]) and GQL ([`Self::execute_gql`])
	/// paths, which differ only in which datastore entrypoint produces `fut`.
	async fn run_to_results<F>(&self, fut: F) -> Result<Vec<QueryResult>, Error>
	where
		F: std::future::Future<
				Output = std::result::Result<Vec<QueryResult>, surrealdb_types::Error>,
			>,
	{
		let outcome = match self.config.query_timeout {
			Some(dur) => match tokio::time::timeout(dur, fut).await {
				Ok(inner) => inner,
				Err(_elapsed) => {
					tracing::warn!(
						target: "surrealdb::mcp",
						timeout_secs = dur.as_secs(),
						"MCP query exceeded the configured timeout"
					);
					let err = surrealdb_types::Error::query(
						format!(
							"MCP query exceeded the {}s timeout (set SURREAL_MCP_QUERY_TIMEOUT_SECS=0 to disable)",
							dur.as_secs()
						),
						surrealdb_types::QueryError::TimedOut {
							duration: dur,
						},
					);
					return Ok(vec![
						QueryResultBuilder::started_now().finish_with_result(Err(err)),
					]);
				}
			},
			None => fut.await,
		};

		match outcome {
			Ok(results) => Ok(results),
			Err(err) => {
				tracing::warn!(
					target: "surrealdb::mcp",
					kind = err.kind_str(),
					error = %err.message(),
					"top-level query execution failed"
				);
				Ok(vec![QueryResultBuilder::started_now().finish_with_result(Err(err))])
			}
		}
	}

	/// Derive a session pinned to an explicit `(namespace, database)`,
	/// sharing this session's datastore, authentication and configuration
	/// but owning its own `use` state.
	///
	/// A `None` component leaves that half of the scope as it is on the
	/// source session, so a caller may override the database while
	/// inheriting the namespace. Mutating the returned session's scope does
	/// not affect the session it was derived from, which is what makes it
	/// safe to hand to a single stateless request.
	pub(crate) async fn derive_scoped(&self, ns: Option<String>, db: Option<String>) -> Self {
		let mut scoped = self.session.read().await.clone();
		if ns.is_some() {
			scoped.ns = ns;
		}
		if db.is_some() {
			scoped.db = db;
		}
		Self {
			ds: Arc::clone(&self.ds),
			session: RwLock::new(scoped),
			config: Arc::clone(&self.config),
		}
	}

	/// Build a session directly from an authenticated `Session`, pinned to
	/// an explicit scope. Used to serve a request that arrives without any
	/// prior handshake, where the caller's credentials and scope both come
	/// from the request itself.
	pub(crate) fn from_request(
		ds: Arc<Datastore>,
		mut session: Session,
		ns: Option<String>,
		db: Option<String>,
		config: Arc<McpConfig>,
	) -> Self {
		if ns.is_some() {
			session.ns = ns;
		}
		if db.is_some() {
			session.db = db;
		}
		Self::with_config(ds, session, config)
	}

	/// Read-only access to the underlying datastore for permission checks
	/// and existence probes that shouldn't go through the SQL layer.
	pub(crate) fn datastore(&self) -> &Datastore {
		&self.ds
	}

	/// Execute a closure against the current session for callers that need
	/// to inspect auth state without mutating anything.
	pub async fn with_session<R>(&self, f: impl FnOnce(&Session) -> R) -> R {
		let session = self.session.read().await;
		f(&session)
	}

	pub async fn use_ns(&self, ns: &str) -> Result<(), Error> {
		let mut session = self.session.write().await;
		session.ns = Some(ns.to_string());
		Ok(())
	}

	pub async fn use_db(&self, db: &str) -> Result<(), Error> {
		let mut session = self.session.write().await;
		session.db = Some(db.to_string());
		Ok(())
	}

	pub async fn current_ns(&self) -> Option<String> {
		self.session.read().await.ns.clone()
	}

	pub async fn current_db(&self) -> Option<String> {
		self.session.read().await.db.clone()
	}

	/// Snapshot `(ns, db)` under a single read lock. Cheaper than two
	/// separate calls and atomic with respect to a concurrent `use`
	/// switch - useful for the audit log path where we want both fields
	/// from the same session state.
	pub(crate) async fn current_ns_db(&self) -> (Option<String>, Option<String>) {
		let session = self.session.read().await;
		(session.ns.clone(), session.db.clone())
	}
}
