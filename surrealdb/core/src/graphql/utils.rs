//! Shared utility functions and traits for the GraphQL module.
//!
//! - [`GraphqlValueUtils`] -- convenience accessors for `async_graphql::Value` variants (number,
//!   string, list, object) that mirror the JSON scalar types.
//! - [`execute_statement`] -- runs one top-level statement against the datastore and extracts the
//!   first result value.

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::{Name, Value as GraphqlValue};

use super::error::GraphqlError;
use crate::dbs::Session;
use crate::kvs::{Datastore, QueryRequest};
use crate::val::Value as SqlValue;

/// Convenience accessors for extracting typed data from an `async_graphql::Value`.
///
/// These mirror the JSON scalar types and are used throughout the resolver and
/// type-conversion code to avoid repetitive pattern matching.
pub(crate) trait GraphqlValueUtils {
	/// Extract the value as an `i64`, if it is a `Number` with an integer representation.
	fn as_i64(&self) -> Option<i64>;
	/// Extract the value as a `String`, if it is a `String` variant.
	fn as_string(&self) -> Option<String>;
	/// Extract the value as a list (slice of values), if it is a `List` variant.
	fn as_list(&self) -> Option<&Vec<GraphqlValue>>;
	/// Extract the value as an object (ordered map), if it is an `Object` variant.
	fn as_object(&self) -> Option<&IndexMap<Name, GraphqlValue>>;
}

impl GraphqlValueUtils for GraphqlValue {
	fn as_i64(&self) -> Option<i64> {
		if let GraphqlValue::Number(n) = self {
			n.as_i64()
		} else {
			None
		}
	}

	fn as_string(&self) -> Option<String> {
		if let GraphqlValue::String(s) = self {
			Some(s.to_owned())
		} else {
			None
		}
	}

	fn as_list(&self) -> Option<&Vec<GraphqlValue>> {
		if let GraphqlValue::List(a) = self {
			Some(a)
		} else {
			None
		}
	}

	fn as_object(&self) -> Option<&IndexMap<Name, GraphqlValue>> {
		if let GraphqlValue::Object(o) = self {
			Some(o)
		} else {
			None
		}
	}
}

/// Execute one top-level statement against the datastore and return its result
/// value.
///
/// This is the lowest-level execution helper. The statement is submitted in
/// the form the parser produces, so the executor chooses its engine exactly as
/// it would for the same statement typed by a user; the first response is
/// taken and its public value converted back to an internal
/// [`Value`](SqlValue). Most resolvers call higher-level wrappers (e.g.
/// `execute_select` in the tables module) that build a `SelectStatement` first.
pub(crate) async fn execute_statement(
	ds: &Datastore,
	sess: &Session,
	stmt: surrealdb_expr::parser::TopLevelExpr,
) -> Result<SqlValue, GraphqlError> {
	let ast = surrealdb_expr::parser::Query {
		expressions: vec![stmt],
	};
	let results = ds
		.run(QueryRequest::new(ast, sess))
		.await
		.map_err(|e| GraphqlError::InternalError(format!("Failed to execute query plan: {}", e)))?;

	// Take the first result from the response list
	let first_result = results
		.into_iter()
		.next()
		.ok_or_else(|| GraphqlError::InternalError("No results returned from query".to_string()))?;

	// Convert from PublicValue to internal Value
	first_result
		.result
		.map(|v| v.into())
		.map_err(|e| GraphqlError::InternalError(format!("Query execution failed: {}", e)))
}
