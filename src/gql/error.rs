use async_graphql::ParseRequestError;
use axum::{
	body::{boxed, BoxBody},
	response::IntoResponse,
};
use http::StatusCode;
use hyper::Body;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GqlError {
	#[error("Database error: {0}")]
	DbError(surrealdb::err::Error),
	#[error("Error generating schema: {0}")]
	SchemaError(String),
	#[error("Error resolving request: {0}")]
	ResolverError(String),
}

pub fn schema_error(msg: impl Into<String>) -> GqlError {
	GqlError::SchemaError(msg.into())
}
pub fn resolver_error(msg: impl Into<String>) -> GqlError {
	GqlError::ResolverError(msg.into())
}

impl From<surrealdb::err::Error> for GqlError {
	fn from(value: surrealdb::err::Error) -> Self {
		GqlError::DbError(value)
	}
}

impl IntoResponse for GqlError {
	fn into_response(self) -> http::Response<BoxBody> {
		http::Response::builder()
			.status(StatusCode::BAD_REQUEST)
			.body(boxed(Body::from(format!("{:?}", self))))
			.unwrap()
	}
}
