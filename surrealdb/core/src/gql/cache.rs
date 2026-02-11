//! GraphQL schema cache.
//!
//! Generated `async_graphql::dynamic::Schema` instances are expensive to build
//! (they require reading all table/field/access definitions from the datastore).
//! This module provides [`GraphQLSchemaCache`] which caches schemas keyed by
//! `(namespace, database, GraphQLConfig)` so that repeated requests to the same
//! database reuse the previously generated schema.
//!
//! Cache entries are transparently invalidated when schema generation fails with
//! a database or schema error, ensuring stale entries do not persist after DDL
//! changes.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_graphql::dynamic::Schema;
use tokio::sync::RwLock;

use super::error::GqlError;
use super::schema::generate_schema;
use crate::catalog::GraphQLConfig;
use crate::dbs::Session;
use crate::kvs::Datastore;

/// Cache key: (namespace name, database name, GraphQL configuration).
///
/// The configuration is included so that a `DEFINE CONFIG GRAPHQL` change
/// naturally produces a different key and triggers regeneration.
type CacheKey = (String, String, GraphQLConfig);

/// In-memory cache of generated GraphQL schemas.
///
/// Thread-safe via `Arc<RwLock<...>>` -- multiple readers can share a cached
/// schema concurrently, and writes (insert/remove) acquire exclusive access.
#[derive(Clone, Default)]
pub struct GraphQLSchemaCache {
	ns_db_schema_cache: Arc<RwLock<HashMap<CacheKey, Schema>>>,
}

impl Debug for GraphQLSchemaCache {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SchemaCache").field("ns_db_schema_cache", &self.ns_db_schema_cache).finish()
	}
}

impl GraphQLSchemaCache {
	/// Retrieve a cached schema or generate a new one.
	///
	/// 1. Reads the current `GraphQLConfig` from the datastore to form the cache key.
	/// 2. Returns the cached schema if one exists for this key.
	/// 3. Otherwise, generates the schema via [`generate_schema`], caches it, and returns it.
	///
	/// On generation failure, stale cache entries are removed if the error
	/// indicates a database or schema-level problem.
	pub async fn get_schema(
		&self,
		datastore: &Arc<Datastore>,
		session: &Session,
	) -> Result<Schema, GqlError> {
		use crate::catalog::providers::DatabaseProvider;
		use crate::kvs::{LockType, TransactionType};

		let ns = session.ns.as_ref().ok_or(GqlError::UnspecifiedNamespace)?;
		let db = session.db.as_ref().ok_or(GqlError::UnspecifiedDatabase)?;

		// Get the current config to use as part of cache key
		let kvs = datastore;
		let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;

		let db_def = match tx.get_db_by_name(ns, db).await? {
			Some(db) => db,
			None => return Err(GqlError::NotConfigured),
		};

		let cg = tx
			.expect_db_config(db_def.namespace_id, db_def.database_id, "graphql")
			.await
			.map_err(|e| {
				if matches!(e.downcast_ref(), Some(crate::err::Error::CgNotFound { .. })) {
					GqlError::NotConfigured
				} else {
					GqlError::DbError(e)
				}
			})?;
		let gql_config = (*cg).clone().try_into_graphql()?;

		let cache_key = (ns.to_owned(), db.to_owned(), gql_config.clone());

		{
			let guard = self.ns_db_schema_cache.read().await;
			if let Some(cand) = guard.get(&cache_key) {
				return Ok(cand.clone());
			}
		};

		// Try to generate the schema
		let schema = match generate_schema(datastore, session, gql_config).await {
			Ok(s) => s,
			Err(e) => {
				// If we get an error that could indicate stale cache (database not found,
				// schema errors from missing tables, etc.), clear the cache entry
				if matches!(e, GqlError::DbError(_) | GqlError::SchemaError(_)) {
					let mut guard = self.ns_db_schema_cache.write().await;
					guard.remove(&cache_key);
				}
				return Err(e);
			}
		};

		{
			let mut guard = self.ns_db_schema_cache.write().await;
			guard.insert(cache_key, schema.clone());
		}

		Ok(schema)
	}
}
