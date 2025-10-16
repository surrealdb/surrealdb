use std::sync::Arc;

use axum::Router;
use axum::routing::post_service;
use surrealdb_core::kvs::Datastore;

use crate::gql::GraphQLService;

pub(super) fn router<S>(ds: Arc<Datastore>) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	let service = GraphQLService::new(ds);
	Router::new().route("/graphql", post_service(service))
}
