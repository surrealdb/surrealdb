use axum::Router;
use std::sync::Arc;

use axum::routing::post_service;

use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::gql::cache::Pessimistic;
use surrealdb::kvs::Datastore;

use crate::err::Error;
use crate::gql::GraphQL;

pub(super) async fn router<S>(ds: Arc<Datastore>) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	// Check if capabilities allow querying the requested HTTP route
	if !ds.allows_http_route(&RouteTarget::GraphQL) {
		return Err(Error::OperationForbidden);
	}
	let service = GraphQL::new(Pessimistic, ds);
	Router::new().route("/graphql", post_service(service))
}
