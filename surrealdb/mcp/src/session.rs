//! Per-MCP-session state wrapping the SurrealDB Datastore and Session.

use std::sync::Arc;

use surrealdb_core::dbs::{QueryResult, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_types::Variables;
use tokio::sync::RwLock;

use crate::error::Error;

/// Per-MCP-session state holding the shared datastore and mutable session context.
pub struct McpSession {
	ds: Arc<Datastore>,
	session: RwLock<Session>,
}

impl McpSession {
	pub fn new(ds: Arc<Datastore>, session: Session) -> Self {
		Self {
			ds,
			session: RwLock::new(session),
		}
	}

	/// Execute a SurrealQL query with optional typed variable bindings.
	pub async fn execute(
		&self,
		query: &str,
		vars: Option<Variables>,
	) -> Result<Vec<QueryResult>, Error> {
		let session = self.session.read().await;
		self.ds.execute(query, &session, vars).await.map_err(|e| Error::QueryFailed(e.into()))
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
}
