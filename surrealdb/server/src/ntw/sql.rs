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
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_types::{Array, SurrealValue, Value, Variables};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::ntw::error::Error as NetError;
use crate::ntw::input::bytes_to_utf8;

pub fn router<S>(max_body_size: usize) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/sql", options(|| async {}).get(get_handler).post(post_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(max_body_size))
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
	// Execute the received sql query
	match db.execute(sql, &session, Some(vars)).await {
		Ok(res) => match output.as_deref() {
			// Simple serialization
			None | Some(Accept::ApplicationJson) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::json_value(&v))
			}
			Some(Accept::ApplicationCbor) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::cbor(v))
			}
			// Internal serialization
			Some(Accept::ApplicationFlatbuffers) => {
				let v = res.into_value();
				Ok(Output::flatbuffers(&v))
			}
			// An unsupported content-type was requested
			Some(_) => Err(NetError::InvalidType.into()),
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
	ws.on_upgrade(move |socket| handle_socket(state, socket, sess))
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
			// Execute the received sql query
			let _ = match db.execute(sql, &session, None).await {
				// Convert the response to JSON
				Ok(v) => match surrealdb_core::rpc::format::json::encode_str(Value::Array(
					Array::from(v.into_iter().map(|x| x.into_value()).collect::<Vec<_>>()),
				)) {
					// Send the JSON response to the client
					Ok(v) => tx.send(Message::Text(v.into())).await,
					// There was an error converting to JSON
					Err(e) => {
						tx.send(Message::Text(format!("Failed to parse JSON: {e}",).into())).await
					}
				},
				// There was an error when executing the query
				Err(e) => tx.send(Message::Text(e.to_string().into())).await,
			};
		}
	}
}
