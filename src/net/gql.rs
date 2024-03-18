use std::convert::Infallible;

use async_graphql::{dynamic::*, BatchResponse, Executor, Value};
use axum::response::IntoResponse;
use axum::Router;
use axum::{response, Extension};
use http_body::Body as HttpBody;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLBatchRequest, GraphQLRequest, GraphQLResponse};
use axum::routing::get;

use surrealdb::dbs::Session;

use crate::gql::schema::get_schema;

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/graphql", get(graphiql).post(post_handler))
}

pub async fn graphiql() -> impl IntoResponse {
	response::Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn post_handler(
	Extension(session): Extension<Session>,
	body: String,
) -> Result<impl IntoResponse, Infallible> {
	let schema = get_schema();
	let res = schema.execute(body).await;
	let res = serde_json::to_string(&res).unwrap();

	Ok(res)
}
