use std::str::FromStr;
use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use axum_extra::headers::Header;
use bytes::Bytes;
use http::HeaderMap;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::mem::ALLOC;
use surrealdb_core::rpc::format::{Format, PROTOCOLS};
use surrealdb_core::rpc::{DbResponse, RpcProtocol};
use tokio::sync::RwLock;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::RequestId;
use uuid::Uuid;

use super::AppState;
use super::error::ResponseError;
use super::headers::{Accept, ContentType, SurrealId};
use crate::ntw::error::Error as NetError;
use crate::rpc::RpcState;
use crate::rpc::format::HttpFormat;
use crate::rpc::websocket::Websocket;

pub fn router(max_body_size: usize) -> Router<Arc<RpcState>> {
	Router::new()
		.route("/rpc", options(|| async {}).get(get_handler).post(post_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(max_body_size))
}

async fn get_handler(
	ws: WebSocketUpgrade,
	Extension(state): Extension<AppState>,
	Extension(id): Extension<RequestId>,
	Extension(mut session): Extension<Session>,
	State(rpc_state): State<Arc<RpcState>>,
	headers: HeaderMap,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Rpc) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Rpc);
		return Err(NetError::ForbiddenRoute(RouteTarget::Rpc.to_string()));
	}
	// Check that a valid header has been specified
	if headers.get(SEC_WEBSOCKET_PROTOCOL).is_none() {
		warn!("A connection was made without a specified protocol.");
		warn!(
			"Automatic inference of the protocol format is deprecated in SurrealDB 2.0 and will be removed in SurrealDB 3.0."
		);
		warn!("Please upgrade any client to ensure that the connection format is specified.");
	}
	// Check if there is a connection id header specified
	let id = match headers.get(SurrealId::name()) {
		// Use the specific SurrealDB id header when provided
		Some(id) => {
			match id.to_str() {
				Ok(id) => {
					// Attempt to parse the request id as a UUID
					match Uuid::from_str(id) {
						// The specified request id was a valid UUID
						Ok(id) => id,
						// The specified request id was not a UUID
						Err(_) => return Err(NetError::Request),
					}
				}
				Err(_) => return Err(NetError::Request),
			}
		}
		// Otherwise, use the generic WebSocket connection id header
		None => match id.header_value().is_empty() {
			// No request id was specified so create a new id
			true => Uuid::new_v4(),
			// A request id was specified to try to parse it
			false => match id.header_value().to_str() {
				// Attempt to parse the request id as a UUID
				Ok(id) => match Uuid::from_str(id) {
					// The specified request id was a valid UUID
					Ok(id) => id,
					// The specified request id was not a UUID
					Err(_) => return Err(NetError::Request),
				},
				// The request id contained invalid characters
				Err(_) => return Err(NetError::Request),
			},
		},
	};
	// This session supports live queries
	session.rt = true;
	// Store the connection id in session
	session.id = Some(id);
	// Check if a connection with this id already exists
	if rpc_state.web_sockets.read().await.contains_key(&id) {
		return Err(NetError::Request);
	}
	let ws_config = &rpc_state.websocket_config;
	// Now let's upgrade the WebSocket connection with comprehensive buffer configuration
	Ok(ws
		// Set the potential WebSocket protocols (JSON, CBOR, etc.)
		.protocols(PROTOCOLS)
		// Set the maximum WebSocket frame size to prevent oversized frames
		.max_frame_size(ws_config.max_message_size)
		// Set the maximum WebSocket message size to prevent memory exhaustion
		.max_message_size(ws_config.max_message_size)
		// Configure read buffer size for incoming data optimization
		.read_buffer_size(ws_config.read_buffer_size)
		// Configure write buffer size for outgoing data optimization
		.write_buffer_size(ws_config.write_buffer_size)
		// Set maximum write buffer size to apply backpressure when needed
		.max_write_buffer_size(ws_config.max_write_buffer_size)
		// Handle WebSocket upgrade failures with appropriate logging
		.on_failed_upgrade(|err| {
			warn!("Failed to upgrade WebSocket connection: {err}");
		})
		// Handle the WebSocket upgrade and process messages
		.on_upgrade(move |socket| {
			handle_socket(state.datastore.clone(), rpc_state, socket, session, id)
		}))
}

async fn handle_socket(
	datastore: Arc<Datastore>,
	state: Arc<RpcState>,
	ws: WebSocket,
	session: Session,
	id: Uuid,
) {
	// Check if there is a WebSocket protocol specified
	let format = match ws.protocol().and_then(|h| h.to_str().ok()) {
		// Any selected protocol will always be a valid value
		Some(protocol) => protocol.into(),
		// No protocol format was specified
		_ => Format::Json,
	};
	// Serve the socket connection requests
	Websocket::serve(id, ws, format, session, datastore, state).await;
}

async fn post_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	State(rpc_state): State<Arc<RpcState>>,
	accept: Option<TypedHeader<Accept>>,
	TypedHeader(content_type): TypedHeader<ContentType>,
	body: Bytes,
) -> Result<impl IntoResponse, ResponseError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Rpc) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Rpc);
		return Err(NetError::ForbiddenRoute(RouteTarget::Rpc.to_string()).into());
	}
	// Get the input format from the Content-Type header
	let fmt: Format = (&content_type).into();
	// Check that the input format is a valid format
	if matches!(fmt, Format::Unsupported) {
		return Err(NetError::InvalidType.into());
	}
	// Get the output format from the Accept header
	let out: Option<Format> = accept.as_deref().map(Into::into);
	// Check that the input format and the output format match
	if let Some(out) = out
		&& fmt != out
	{
		return Err(NetError::InvalidType.into());
	}
	// Use the shared HTTP instance with persistent sessions
	let rpc = &*rpc_state.http;
	// Update the default session (None key) with the session from middleware
	// This is used for requests that don't specify a session_id
	rpc.set_session(None, Arc::new(RwLock::new(session)));
	// Check to see available memory
	if ALLOC.is_beyond_threshold() {
		return Err(NetError::ServerOverloaded.into());
	}
	// Parse the HTTP request body
	match fmt.req_http(body) {
		Ok(req) => {
			// Execute the specified method
			let res = RpcProtocol::execute(
				rpc,
				req.txn.map(Into::into),
				req.session_id.map(Into::into),
				req.method,
				req.params,
			)
			.await;
			// Return the HTTP response
			Ok(fmt.res_http(match res {
				Ok(result) => DbResponse::success(None, None, result),
				Err(err) => DbResponse::failure(None, None, err),
			})?)
		}
		Err(err) => Err(err.into()),
	}
}
