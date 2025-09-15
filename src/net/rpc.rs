use std::ops::Deref;
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
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::RequestId;
use uuid::Uuid;

use super::AppState;
use super::error::ResponseError;
use super::headers::{Accept, ContentType, SurrealId};
use crate::cnf;
use crate::cnf::HTTP_MAX_RPC_BODY_SIZE;
use crate::core::dbs::Session;
use crate::core::dbs::capabilities::RouteTarget;
use crate::core::kvs::Datastore;
use crate::core::mem::ALLOC;
use crate::core::rpc::RpcContext;
use crate::core::rpc::format::{Format, PROTOCOLS};
use crate::net::error::Error as NetError;
use crate::rpc::RpcState;
use crate::rpc::format::HttpFormat;
use crate::rpc::http::Http;
use crate::rpc::response::IntoRpcResponse;
use crate::rpc::websocket::Websocket;

pub(super) fn router() -> Router<Arc<RpcState>> {
	Router::new()
		.route("/rpc", options(|| async {}).get(get_handler).post(post_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_RPC_BODY_SIZE))
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
					match Uuid::try_parse(id) {
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
				Ok(id) => match Uuid::try_parse(id) {
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
	session.id = Some(id.to_string());
	// Check if a connection with this id already exists
	if rpc_state.web_sockets.read().await.contains_key(&id) {
		return Err(NetError::Request);
	}
	// Now let's upgrade the WebSocket connection
	Ok(ws
		// Set the potential WebSocket protocols
		.protocols(PROTOCOLS)
		// Set the maximum WebSocket frame size
		.max_frame_size(*cnf::WEBSOCKET_MAX_FRAME_SIZE)
		// Set the maximum WebSocket message size
		.max_message_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
		// Set an error
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
	accept: Option<TypedHeader<Accept>>,
	content_type: TypedHeader<ContentType>,
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
	let fmt: Format = content_type.deref().into();
	// Check that the input format is a valid format
	if matches!(fmt, Format::Unsupported) {
		return Err(NetError::InvalidType.into());
	}
	// Get the output format from the Accept header
	let out: Option<Format> = accept.as_deref().map(Into::into);
	// Check that the input format and the output format match
	if let Some(out) = out {
		if fmt != out {
			return Err(NetError::InvalidType.into());
		}
	}
	// Create a new HTTP instance
	let rpc = Http::new(&state.datastore, session);
	// Check to see available memory
	if ALLOC.is_beyond_threshold() {
		return Err(NetError::ServerOverloaded.into());
	}
	// Parse the HTTP request body
	match fmt.req_http(body) {
		Ok(req) => {
			// Execute the specified method
			let res = RpcContext::execute(&rpc, req.version, req.txn, req.method, req.params).await;
			// Return the HTTP response
			Ok(fmt.res_http(res.into_response(None))?)
		}
		Err(err) => Err(err.into()),
	}
}
