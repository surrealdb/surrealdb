use anyhow::Context;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{DefaultBodyLimit, Query, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::HTTP_MAX_SQL_BODY_SIZE;
use crate::core::dbs::capabilities::RouteTarget;
use crate::core::dbs::{Session, Variables};
use crate::core::val::Value;
use crate::net::error::Error as NetError;
use crate::net::input::bytes_to_utf8;
use crate::net::params::Params;

pub(super) fn router<S>() -> Router<S>
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
	params: Query<Params>,
	sql: Bytes,
) -> Result<Output, ResponseError> {
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
	match db.execute(sql, &session, Some(Variables::from(params.0.parse()))).await {
		Ok(res) => match output.as_deref() {
			// Simple serialization
			Some(Accept::ApplicationJson) => {
				let v = res.into_iter().map(|x| x.into_value()).collect::<Value>();
				Ok(Output::json_value(&v))
			}
			Some(Accept::ApplicationCbor) => {
				let v = res.into_iter().map(|x| x.into_value()).collect::<Value>();
				Ok(Output::cbor(&v))
			}
			// Internal serialization
			Some(Accept::Surrealdb) => Ok(Output::bincode(&res)),
			// An incorrect content-type was requested
			_ => Err(NetError::InvalidType.into()),
		},
		// There was an error when executing the query
		Err(err) => Err(ResponseError(err)),
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
		if let Ok(msg) = res {
			if let Ok(sql) = msg.to_text() {
				// Get a database reference
				let db = &state.datastore;
				// Execute the received sql query
				let _ = match db.execute(sql, &session, None).await {
					// Convert the response to JSON
					Ok(v) => match crate::core::rpc::format::json::encode_str(Value::from(
						v.into_iter().map(|x| x.into_value()).collect::<Vec<_>>(),
					)) {
						// Send the JSON response to the client
						Ok(v) => tx.send(Message::Text(v)).await,
						// There was an error converting to JSON
						Err(e) => {
							tx.send(Message::Text(format!("Failed to parse JSON: {e}",))).await
						}
					},
					// There was an error when executing the query
					Err(e) => tx.send(Message::Text(e.to_string())).await,
				};
			}
		}
	}
}
