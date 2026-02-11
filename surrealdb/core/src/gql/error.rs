//! GraphQL error types and helper constructors.
//!
//! [`GqlError`] is the central error type for the GraphQL subsystem. It covers
//! schema generation failures, resolver runtime errors, authentication issues,
//! type conversion mismatches, and internal bugs.
//!
//! Each variant has a corresponding helper constructor (e.g. [`schema_error`],
//! [`resolver_error`]) that creates the error from a message string.  A blanket
//! `From<GqlError> for async_graphql::Error` conversion allows `?` propagation
//! from resolvers.

use std::backtrace;
use std::fmt::Debug;

use async_graphql::{InputType, InputValueError};
use surrealdb_types::ToSql;
use thiserror::Error;

use crate::expr::Kind;

/// Domain error type for the GraphQL subsystem.
///
/// Variants fall into three categories:
/// - **User-facing** -- `SchemaError`, `ResolverError`, `AuthError`, `TypeError`, `NotConfigured`,
///   `UnspecifiedNamespace`, `UnspecifiedDatabase`.
/// - **Propagated** -- `DbError` wraps lower-level datastore errors via `#[from]`.
/// - **Internal** -- `InternalError` indicates a bug; the constructor logs a backtrace at `error!`
///   level before returning.
#[derive(Error, Debug)]
pub enum GqlError {
	/// A lower-level datastore or transaction error.
	#[error("Database error: {0}")]
	DbError(#[from] anyhow::Error),

	/// The schema could not be generated (e.g. unsupported types, no tables).
	#[error("Error generating schema: {0}")]
	SchemaError(String),

	/// A resolver encountered an error at query execution time.
	#[error("Error resolving request: {0}")]
	ResolverError(String),

	/// The session did not specify a namespace.
	#[error("No Namespace specified")]
	UnspecifiedNamespace,

	/// The session did not specify a database.
	#[error("No Database specified")]
	UnspecifiedDatabase,

	/// No `DEFINE CONFIG GRAPHQL` statement has been executed for this database.
	#[error("GraphQL has not been configured for this database")]
	NotConfigured,

	/// An unexpected internal error (bug). The constructor logs a backtrace.
	#[error("Internal Error: {0}")]
	InternalError(String),

	/// An authentication operation (signIn/signUp) failed.
	#[error("Authentication error: {0}")]
	AuthError(String),

	/// A GraphQL value could not be converted to the expected SurrealDB type.
	#[error("Error converting value: {val} to type: {}", target.to_sql())]
	TypeError {
		target: Kind,
		val: async_graphql::Value,
	},
}

/// Create a [`GqlError::SchemaError`].
pub fn schema_error(msg: impl Into<String>) -> GqlError {
	GqlError::SchemaError(msg.into())
}

/// Create a [`GqlError::ResolverError`].
pub fn resolver_error(msg: impl Into<String>) -> GqlError {
	GqlError::ResolverError(msg.into())
}

/// Create a [`GqlError::AuthError`].
pub fn auth_error(msg: impl Into<String>) -> GqlError {
	GqlError::AuthError(msg.into())
}

/// Create a [`GqlError::InternalError`], logging the message and a captured
/// backtrace at `error!` level.
pub fn internal_error(msg: impl Into<String>) -> GqlError {
	let msg = msg.into();
	let bt = backtrace::Backtrace::capture();

	error!("{}\n{bt}", msg);
	GqlError::InternalError(msg)
}

/// Create a [`GqlError::TypeError`] from a target kind and the offending value.
pub fn type_error(kind: Kind, val: &async_graphql::Value) -> GqlError {
	GqlError::TypeError {
		target: kind,
		val: val.to_owned(),
	}
}

/// Convert an `async_graphql` input-value validation error into a resolver error.
impl<T> From<InputValueError<T>> for GqlError
where
	T: InputType + Debug,
{
	fn from(value: InputValueError<T>) -> Self {
		GqlError::ResolverError(format!("{value:?}"))
	}
}

/// Allow `GqlError` to be used with `?` inside `async_graphql` resolvers by
/// converting it to `async_graphql::Error`.
impl From<GqlError> for async_graphql::Error {
	fn from(value: GqlError) -> Self {
		async_graphql::Error::new(value.to_string())
	}
}
