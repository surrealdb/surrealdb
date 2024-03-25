use axum::response;
use axum::response::IntoResponse;
use axum::Router;
use http_body::Body as HttpBody;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::GraphQL;
use axum::routing::get;

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
	Router::new().route("/graphql", get(graphiql).post_service(service))
}

pub async fn graphiql() -> impl IntoResponse {
	response::Html(GraphiQLSource::build().endpoint("/graphql").finish())
}
