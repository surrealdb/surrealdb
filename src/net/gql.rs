use axum::Router;
use axum::routing::post_service;

use crate::gql::GraphQLService;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	let service = GraphQLService::new();
	Router::new().route("/graphql", post_service(service))
}
