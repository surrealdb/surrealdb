use std::env;

use axum::response;
use axum::response::IntoResponse;
use axum::Router;
use http_body::Body as HttpBody;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::GraphQL;
use axum::routing::{get, post_service};

use crate::gql::schema::get_schema;

pub(super) async fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + Sync + 'static,
	B::Data: Send + Sync,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
	bytes::Bytes: From<<B as HttpBody>::Data>,
{
	let service = GraphQL::new(get_schema().await.unwrap());
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
