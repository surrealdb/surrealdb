use async_graphql_axum::{self, GraphQLBatchRequest, GraphQLRequest, GraphQLResponse};
use tokio::sync::RwLock;

use std::{
	collections::BTreeMap,
	convert::Infallible,
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};

use async_graphql::{
	dynamic::Schema,
	http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
	Executor,
};
use axum::{
	body::{BoxBody, HttpBody, StreamBody},
	extract::FromRequest,
	http::{Request as HttpRequest, Response as HttpResponse},
	response::IntoResponse,
	BoxError,
};
use bytes::Bytes;
use futures_util::{future::BoxFuture, StreamExt};
use tower_service::Service;

use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;

use crate::net::AppState;

use super::{
	error::{resolver_error, GqlError},
	schema::generate_schema,
};

pub trait Invalidator: Debug + Clone + Send + Sync + 'static {
	type MetaData: Debug + Clone + Send + Sync + Hash;

	fn is_valid(datastore: &Datastore, session: &Session, meta: &Self::MetaData) -> bool;

	fn generate(
		datastore: &Arc<Datastore>,
		session: &Session,
	) -> impl std::future::Future<Output = Result<(Schema, Self::MetaData), GqlError>> + std::marker::Send;
}

#[derive(Debug, Clone, Copy)]
pub struct Pessimistic;
impl Invalidator for Pessimistic {
	type MetaData = ();

	fn is_valid(_datastore: &Datastore, _session: &Session, _meta: &Self::MetaData) -> bool {
		false
	}

	async fn generate(
		datastore: &Arc<Datastore>,
		session: &Session,
	) -> Result<(Schema, Self::MetaData), GqlError> {
		let schema = generate_schema(datastore, session).await?;
		Ok((schema, ()))
	}
}

#[derive(Clone)]
pub struct SchemaCache<I: Invalidator = Pessimistic> {
	inner: Arc<RwLock<BTreeMap<(String, String), (Schema, I::MetaData)>>>,
	_invalidator: PhantomData<I>,
}

impl<I: Invalidator + Debug> Debug for SchemaCache<I> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SchemaCache")
			.field("inner", &self.inner)
			.field("_invalidator", &self._invalidator)
			.finish()
	}
}

impl<I: Invalidator> SchemaCache<I> {
	pub fn new() -> Self {
		SchemaCache {
			inner: BTreeMap::new(),
			_invalidator: PhantomData,
		}
	}
	pub async fn get_schema(&self, session: &Session) -> Result<Schema, GqlError> {
		let ns = session.ns.as_ref().expect("missing ns should have been caught");
		let db = session.db.as_ref().expect("missing db should have been caught");
		{
			let guard = self.inner.read().await;
			if let Some(cand) = guard.get(&(ns.to_owned(), db.to_owned())) {
				if I::is_valid(&self.datastore, session, &cand.1) {
					return Ok(cand.0.clone());
				}
			}
		};

		let (schema, meta) = I::generate(&self.datastore, session).await?;

		{
			let mut guard = self.inner.write().await;
			guard.insert((ns.to_owned(), db.to_owned()), (schema.clone(), meta));
		}

		return Ok(schema);
	}

	// fn get(&self, ns: String, db: String) -> Option<&(Schema, I::MetaData)> {
	// 	self.inner.get(&(ns, db))
	// }

	// fn insert(&mut self, ns: String, db: String, schema: Schema, meta: I::MetaData) {
	// 	self.inner.insert((ns, db), (schema, meta));
	// }
}
