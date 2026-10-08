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
use surrealdb_rpc::RowFormat;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_types::{SurrealValue, Variables};
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

async fn post_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	output: Option<TypedHeader<Accept>>,
	Query(params): Query<BTreeMap<String, String>>,
	sql: Bytes,
) -> Result<Output, ResponseError> {
	let vars = Variables::from(params);
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
	match with_query_timeout(db.query_timeout(), db.execute(sql, &session, Some(vars))).await {
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
