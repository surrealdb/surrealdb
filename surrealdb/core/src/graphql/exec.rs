//! Transport-agnostic GraphQL request execution.
//!
//! [`execute_request`] runs a single GraphQL operation against the namespace
//! and database selected on a [`Session`] and returns the response envelope
//! (`{ data, errors }`) as plain JSON. It is shared by every non-HTTP
//! transport that speaks GraphQL -- the `graphql` RPC method and the MCP
//! `graphql` tool -- so request construction and schema resolution stay in one
//! place. The HTTP `/graphql` route builds its request through
//! `async-graphql-axum` (to support batching and `multipart/mixed`) but
//! resolves its schema through the same datastore cache
//! ([`Datastore::graphql_schema`]).

use std::sync::Arc;

use serde_json::Value as JsonValue;

use super::GraphqlError;
use crate::dbs::Session;
use crate::kvs::Datastore;

/// Execute a GraphQL request against the namespace and database selected on
/// `session`, returning the GraphQL response envelope (`{ data, errors }`) as
/// plain JSON.
///
/// `variables` is a plain-JSON object (or `Null` for none); `operation`
/// selects a named operation when the document defines more than one. Schema
/// generation surfaces missing-namespace / missing-database / not-configured as
/// [`GraphqlError`]s; GraphQL execution errors are reported in-band in the returned
/// envelope's `errors` array, exactly as over HTTP.
pub async fn execute_request(
	ds: &Arc<Datastore>,
	session: &Session,
	query: String,
	variables: JsonValue,
	operation: Option<String>,
) -> Result<JsonValue, GraphqlError> {
	if !ds.allows_query_by_subject(session.au.as_ref()) {
		return Err(GraphqlError::AuthError(
			"GraphQL query execution is not allowed by subject capabilities".to_string(),
		));
	}

	let schema = ds.graphql_schema(session).await?;

	// Build the request from its standard JSON envelope so parsing matches the
	// HTTP `/graphql` transport exactly.
	let mut envelope = serde_json::Map::new();
	envelope.insert("query".to_string(), JsonValue::String(query));
	if !variables.is_null() {
		envelope.insert("variables".to_string(), variables);
	}
	if let Some(operation) = operation {
		envelope.insert("operationName".to_string(), JsonValue::String(operation));
	}
	let request: async_graphql::Request = serde_json::from_value(JsonValue::Object(envelope))
		.map_err(|e| GraphqlError::ResolverError(format!("Invalid GraphQL request: {e}")))?;

	// Resolvers read the datastore and session out of the request context.
	let request = request.data(Arc::clone(ds)).data(Arc::new(session.clone()));

	let response = schema.execute(request).await;
	serde_json::to_value(&response).map_err(|e| {
		GraphqlError::InternalError(format!("Failed to serialise GraphQL response: {e}"))
	})
}
