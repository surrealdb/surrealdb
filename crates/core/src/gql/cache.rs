use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use async_graphql::dynamic::Schema;
use tokio::sync::RwLock;

use super::error::GqlError;
use super::schema::generate_schema;
use crate::dbs::Session;
use crate::kvs::Datastore;

#[derive(Clone)]
pub struct GraphQLSchemaCache {
	ns_db_schema_cache: Arc<RwLock<BTreeMap<(String, String), Schema>>>,
	pub datastore: Arc<Datastore>,
}

impl Debug for GraphQLSchemaCache {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SchemaCache").field("ns_db_schema_cache", &self.ns_db_schema_cache).finish()
	}
}

impl GraphQLSchemaCache {
	pub fn new(datastore: Arc<Datastore>) -> Self {
		GraphQLSchemaCache {
			ns_db_schema_cache: Default::default(),
			datastore,
		}
	}
	pub async fn get_schema(&self, session: &Session) -> Result<Schema, GqlError> {
		let ns = session.ns.as_ref().ok_or(GqlError::UnspecifiedNamespace)?;
		let db = session.db.as_ref().ok_or(GqlError::UnspecifiedDatabase)?;
		{
			let guard = self.ns_db_schema_cache.read().await;
			if let Some(cand) = guard.get(&(ns.to_owned(), db.to_owned())) {
				return Ok(cand.clone());
			}
		};

		let schema = generate_schema(&self.datastore, session).await?;

		{
			let mut guard = self.ns_db_schema_cache.write().await;
			guard.insert((ns.to_owned(), db.to_owned()), schema.clone());
		}

		Ok(schema)
	}
}
