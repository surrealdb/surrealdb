use std::collections::BTreeMap;

use anyhow::Context;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{DefaultBodyLimit, Query, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use surrealdb_core::dbs::Session;
use surrealdb_core::rpc::format::json::json_to_value;
use surrealdb_rpc::RowFormat;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_types::{SurrealValue, Value, Variables};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::{
	HTTP_MAX_SQL_BODY_SIZE, WEBSOCKET_MAX_MESSAGE_SIZE, WEBSOCKET_MAX_WRITE_BUFFER_SIZE,
	WEBSOCKET_READ_BUFFER_SIZE, WEBSOCKET_WRITE_BUFFER_SIZE,
};
use crate::ntw::error::Error as NetError;
use crate::ntw::input::bytes_to_utf8;
use crate::ntw::timeout::with_query_timeout;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/sql", options(|| async {}).get(get_handler).post(post_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_SQL_BODY_SIZE))
}

/// The variables a request's query string binds.
///
/// A value that is JSON is bound as the value it encodes, so `?Data={"a":1}`
/// binds an object; any other value is bound as the text it arrived as. Only
/// JSON is read: a record id, datetime or uuid spelled in the query string
/// stays text, as it would in a JSON body.
fn query_variables(params: BTreeMap<String, String>) -> Variables {
	params
		.into_iter()
		.map(|(name, text)| {
			// Only text that can begin a JSON value is worth handing to the
			// deserializer; most query parameters are plain words.
			let value = match text.trim_start().chars().next() {
				Some('{' | '[' | '"' | 't' | 'f' | 'n' | '-' | '0'..='9') => {
					match serde_json::from_str(&text) {
						Ok(json) => json_to_value(json),
						Err(_) => Value::String(text),
					}
				}
				_ => Value::String(text),
			};
			(name, value)
		})
		.collect()
}

async fn post_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	output: Option<TypedHeader<Accept>>,
	Query(mut params): Query<BTreeMap<String, String>>,
	sql: Bytes,
) -> Result<Output, ResponseError> {
	let read_only = take_read_only(&mut params).map_err(|e| ResponseError(e.into()))?;
	let vars = query_variables(params);
	// Get a database reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Sql) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Sql);
		return Err(NetError::ForbiddenRoute(RouteTarget::Sql.to_string()).into());
	}
	// Check if the user is allowed to query
	if !db.allows_query_by_subject(session.au.as_ref()) {
		return Err(NetError::ForbiddenRoute(RouteTarget::Sql.to_string()).into());
	}
	// Convert the received sql query
	let sql = bytes_to_utf8(&sql).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Execute the received sql query under the configured wall-clock query
	// timeout (default off), reusing `--query-timeout` as the guard duration.
	// JSON and CBOR answers are written around rows the executor encoded in
	// that format, so no public value is built for a row.
	let encoded = match output.as_deref() {
		None | Some(Accept::ApplicationJson) => Some(RowFormat::Json),
		Some(Accept::ApplicationCbor) => Some(RowFormat::Cbor),
		_ => None,
	};
	if let Some(format) = encoded {
		let run = surrealdb_core::rpc::format::encoded::execute_encoded(
			db,
			sql,
			&session,
			Some(vars),
			format,
			read_only,
		);
		return match with_query_timeout(db.query_timeout(), run).await {
			Ok(res) => {
				let mut bytes = Vec::new();
				match surrealdb_core::rpc::format::encoded::encode_results(format, &res, &mut bytes)
				{
					Ok(()) => Ok(match format {
						RowFormat::Json => Output::Json(bytes),
						RowFormat::Cbor => Output::Cbor(bytes),
					}),
					Err(_) => Ok(Output::Fail),
				}
			}
			Err(err) => Err(ResponseError(err.into())),
		};
	}
	let run = db.run(
		surrealdb_core::kvs::QueryRequest::new(sql, &session)
			.with_variables(Some(vars))
			.with_read_only(read_only),
	);
	match with_query_timeout(db.query_timeout(), run).await {
		Ok(res) => match output.as_deref() {
			// Internal serialization
			Some(Accept::ApplicationFlatbuffers) => {
				let v = res.into_value();
				Ok(Output::flatbuffers(&v))
			}
			// An unsupported content-type was requested; JSON and CBOR are
			// answered above.
			_ => Err(NetError::InvalidType.into()),
		},
		// There was an error when executing the query
		Err(err) => Err(ResponseError(err.into())),
	}
}

/// The query-string parameter that asks for the request to run as one read-only
/// block, and the bound it carries: `read_only_max_staleness=<duration>`.
const READ_ONLY_MAX_STALENESS: &str = "read_only_max_staleness";
/// The query-string parameter that makes the bound strict. See
/// [`READ_ONLY_MAX_STALENESS`].
const READ_ONLY_STRICT: &str = "read_only_strict";

/// Takes the read-only parameters out of the query string, so they are not also
/// bound as variables. The request then runs exactly as if its statements were
/// wrapped in `BEGIN READ ONLY MAX STALENESS <duration> [STRICT]; …; COMMIT;`.
/// `read_only_strict` without `read_only_max_staleness` is refused rather than
/// ignored.
fn take_read_only(
	params: &mut BTreeMap<String, String>,
) -> Result<Option<surrealdb_core::kvs::ReadOnlyOptions>, surrealdb_types::Error> {
	let bound = params.remove(READ_ONLY_MAX_STALENESS);
	let strict = params.remove(READ_ONLY_STRICT);
	let Some(bound) = bound else {
		return match strict {
			Some(_) => Err(surrealdb_types::Error::validation(
				format!("`{READ_ONLY_STRICT}` requires `{READ_ONLY_MAX_STALENESS}`"),
				None,
			)),
			None => Ok(None),
		};
	};
	let max_staleness = bound
		.parse::<surrealdb_types::Duration>()
		.map(std::time::Duration::from)
		.map_err(|_| {
		surrealdb_types::Error::validation(
			format!("`{READ_ONLY_MAX_STALENESS}` must be a duration, got `{bound}`"),
			None,
		)
	})?;
	let strict = match strict.as_deref() {
		None => false,
		Some(v) if v.eq_ignore_ascii_case("true") || v == "1" => true,
		Some(v) if v.eq_ignore_ascii_case("false") || v == "0" => false,
		Some(v) => {
			return Err(surrealdb_types::Error::validation(
				format!("`{READ_ONLY_STRICT}` must be a boolean, got `{v}`"),
				None,
			));
		}
	};
	Ok(Some(surrealdb_core::kvs::ReadOnlyOptions {
		max_staleness,
		strict,
	}))
}

async fn get_handler(
	ws: WebSocketUpgrade,
	Extension(state): Extension<AppState>,
	Extension(sess): Extension<Session>,
) -> impl IntoResponse {
	// Get a database reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Sql) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Sql);
		return NetError::ForbiddenRoute(RouteTarget::Sql.to_string()).into_response();
	}
	// Check if the user is allowed to query
	if !db.allows_query_by_subject(sess.au.as_ref()) {
		return NetError::ForbiddenRoute(RouteTarget::Sql.to_string()).into_response();
	}
	// Now upgrade the WebSocket connection with comprehensive buffer configuration
	ws
		// Set the maximum WebSocket frame size to prevent oversized frames
		.max_frame_size(*WEBSOCKET_MAX_MESSAGE_SIZE)
		// Set the maximum WebSocket message size to prevent memory exhaustion
		.max_message_size(*WEBSOCKET_MAX_MESSAGE_SIZE)
		// Configure read buffer size for incoming data optimization
		.read_buffer_size(*WEBSOCKET_READ_BUFFER_SIZE)
		// Configure write buffer size for outgoing data optimization
		.write_buffer_size(*WEBSOCKET_WRITE_BUFFER_SIZE)
		// Set maximum write buffer size to apply backpressure when needed
		.max_write_buffer_size(*WEBSOCKET_MAX_WRITE_BUFFER_SIZE)
		// Handle WebSocket upgrade failures with appropriate logging
		.on_failed_upgrade(|err| {
			warn!("Failed to upgrade WebSocket connection: {err}");
		})
		// Handle the WebSocket upgrade and process messages
		.on_upgrade(move |socket| handle_socket(state, socket, sess))
}

async fn handle_socket(state: AppState, ws: WebSocket, session: Session) {
	// Split the WebSocket connection
	let (mut tx, mut rx) = ws.split();
	// Wait to receive the next message
	while let Some(res) = rx.next().await {
		if let Ok(msg) = res
			&& let Ok(sql) = msg.to_text()
		{
			// Get a database reference
			let db = &state.datastore;
			// Execute the received sql query under the configured wall-clock
			// query timeout (default off).
			let run = surrealdb_core::rpc::format::encoded::execute_encoded(
				db,
				sql,
				&session,
				None,
				RowFormat::Json,
				None,
			);
			let _ = match with_query_timeout(db.query_timeout(), run).await {
				// Write the JSON response around the encoded rows
				Ok(v) => {
					let mut bytes = Vec::new();
					match surrealdb_core::rpc::format::encoded::encode_results(
						RowFormat::Json,
						&v,
						&mut bytes,
					)
					.and_then(|()| Ok(String::from_utf8(bytes)?))
					{
						// Send the JSON response to the client
						Ok(v) => tx.send(Message::Text(v.into())).await,
						// There was an error converting to JSON
						Err(e) => {
							tx.send(Message::Text(format!("Failed to parse JSON: {e}",).into()))
								.await
						}
					}
				}
				// There was an error when executing the query
				Err(e) => tx.send(Message::Text(e.to_string().into())).await,
			};
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	fn bound(text: &str) -> Value {
		let params = BTreeMap::from([("p".to_owned(), text.to_owned())]);
		query_variables(params).get("p").cloned().expect("the variable is bound")
	}

	#[test]
	fn json_in_a_query_string_binds_as_the_value_it_encodes() {
		assert!(matches!(bound(r#"{"Settings":{"testing":"hello"}}"#), Value::Object(_)));
		assert!(matches!(bound("[1,2,3]"), Value::Array(_)));
		assert!(matches!(bound("true"), Value::Bool(true)));
		assert!(matches!(bound("42"), Value::Number(_)));
		assert!(matches!(bound(r#""quoted""#), Value::String(_)));
	}

	#[test]
	fn anything_else_in_a_query_string_binds_as_its_text() {
		for text in ["hello", "person:1", "2024-01-01T00:00:00Z", "{not json", "", "a b"] {
			assert_eq!(bound(text), Value::String(text.to_owned()), "{text:?}");
		}
	}

	fn params(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
		entries.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
	}

	#[test]
	fn the_read_only_parameters_are_taken_out_of_the_variables() {
		let mut p = params(&[
			("read_only_max_staleness", "5s"),
			("read_only_strict", "true"),
			("other", "1"),
		]);
		let read_only = take_read_only(&mut p).unwrap().unwrap();
		assert_eq!(read_only.max_staleness, Duration::from_secs(5));
		assert!(read_only.strict);
		assert_eq!(p, params(&[("other", "1")]));

		let mut p = params(&[("read_only_max_staleness", "250ms")]);
		let read_only = take_read_only(&mut p).unwrap().unwrap();
		assert_eq!(read_only.max_staleness, Duration::from_millis(250));
		assert!(!read_only.strict);

		let mut p = params(&[("other", "1")]);
		assert!(take_read_only(&mut p).unwrap().is_none());
	}

	#[test]
	fn a_malformed_read_only_parameter_is_refused() {
		for p in [
			params(&[("read_only_strict", "true")]),
			params(&[("read_only_max_staleness", "soon")]),
			params(&[("read_only_max_staleness", "5s"), ("read_only_strict", "maybe")]),
		] {
			assert!(take_read_only(&mut p.clone()).is_err(), "{p:?}");
		}
	}
}
