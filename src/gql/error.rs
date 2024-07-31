use std::fmt::Debug;

use async_graphql::{InputType, InputValueError};
use axum::{
	body::{boxed, BoxBody},
	response::IntoResponse,
};
use http::StatusCode;
use hyper::Body;
use surrealdb::sql::Kind;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GqlError {
	#[error("Database error: {0}")]
	DbError(surrealdb::err::Error),
	#[error("Error generating schema: {0}")]
	SchemaError(String),
	#[error("Error resolving request: {0}")]
	ResolverError(String),
	#[error("Internal Error: {0}")]
	InternalError(String),
	#[error("Error converting value: {val} to type: {target}")]
	TypeError {
		target: Kind,
		val: async_graphql::Value,
	},
}

pub fn schema_error(msg: impl Into<String>) -> GqlError {
	GqlError::SchemaError(msg.into())
}

pub fn resolver_error(msg: impl Into<String>) -> GqlError {
	GqlError::ResolverError(msg.into())
}
pub fn internal_error(msg: impl Into<String>) -> GqlError {
	let msg = msg.into();
	error!("{}", msg);
	GqlError::InternalError(msg.into())
}

pub fn type_error(kind: Kind, val: &async_graphql::Value) -> GqlError {
	GqlError::TypeError {
		target: kind,
		val: val.to_owned(),
	}
}

impl From<surrealdb::err::Error> for GqlError {
	fn from(value: surrealdb::err::Error) -> Self {
		GqlError::DbError(value)
	}
}

impl<T> From<InputValueError<T>> for GqlError
where
	T: InputType + Debug,
{
	fn from(value: InputValueError<T>) -> Self {
		GqlError::ResolverError(format!("{value:?}"))
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
