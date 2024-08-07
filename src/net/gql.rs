use std::{env, sync::Arc};

use axum::response;
use axum::response::IntoResponse;
use axum::Router;

use async_graphql::http::GraphiQLSource;
use axum::routing::{get, post_service};

use surrealdb::gql::cache::Pessimistic;
use surrealdb::kvs::Datastore;

use crate::gql::GraphQL;

pub(super) async fn router<S>(ds: Arc<Datastore>) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	let service = GraphQL::new(Pessimistic, ds);
	let var = env::var("SURREALDB_ENABLE_GRAPHQL_DASHBOARD");
	match var.as_ref().map(|s| s.as_str()) {
		Ok("true") => {
			warn!("IMPORTANT: GraphQL Dashboard is a pre-release feature. This is not recomended for production use.");
			Router::new().route("/graphql", get(graphiql).post_service(service))
		}
		_ => Router::new().route("/graphql", post_service(service)),
	}
}

pub async fn graphiql() -> impl IntoResponse {
	response::Html(GraphiQLSource::build().endpoint("/graphql").finish())
}
