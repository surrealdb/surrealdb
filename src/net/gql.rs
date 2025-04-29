use std::sync::Arc;

use axum::routing::post_service;
use axum::Router;
use surrealdb::gql::cache::Pessimistic;
use surrealdb::kvs::Datastore;

use crate::gql::GraphQL;

pub(super) fn router<S>(ds: Arc<Datastore>) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	let service = GraphQL::new(Pessimistic, ds);
	Router::new().route("/graphql", post_service(service))
}
