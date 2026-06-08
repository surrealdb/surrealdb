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
use surrealdb_core::iam::Auth;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::mem::ALLOC;
use surrealdb_core::rpc::format::{Format, PROTOCOLS};
use surrealdb_core::rpc::{DbResponse, Method, RpcProtocol};
use tokio::sync::RwLock;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::RequestId;
use uuid::Uuid;

use super::AppState;
use super::error::ResponseError;
use super::headers::{Accept, ContentType, SurrealId};
use crate::cnf;
use crate::cnf::HTTP_MAX_RPC_BODY_SIZE;
use crate::ntw::error::Error as NetError;
use crate::rpc::RpcState;
use crate::rpc::format::HttpFormat;
use crate::rpc::websocket::Websocket;

pub fn router() -> Router<Arc<RpcState>> {
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
	// Now let's upgrade the WebSocket connection with comprehensive buffer configuration
	Ok(ws
		// Set the potential WebSocket protocols (JSON, CBOR, etc.)
		.protocols(PROTOCOLS)
		// Set the maximum WebSocket frame size to prevent oversized frames
		.max_frame_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
		// Set the maximum WebSocket message size to prevent memory exhaustion
		.max_message_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
		// Configure read buffer size for incoming data optimization
		.read_buffer_size(*cnf::WEBSOCKET_READ_BUFFER_SIZE)
		// Configure write buffer size for outgoing data optimization
		.write_buffer_size(*cnf::WEBSOCKET_WRITE_BUFFER_SIZE)
		// Set maximum write buffer size to apply backpressure when needed
		.max_write_buffer_size(*cnf::WEBSOCKET_MAX_WRITE_BUFFER_SIZE)
		// Handle WebSocket upgrade failures with appropriate logging
		.on_failed_upgrade(|err| {
			warn!("Failed to upgrade WebSocket connection: {err}");
		})
		// Handle the WebSocket upgrade and process messages
		.on_upgrade(move |socket| {
			handle_socket(Arc::clone(&state.datastore), rpc_state, socket, session, id)
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

	let rec_limit = db.config().max_object_parsing_depth as usize;
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
	let rpc = &*rpc_state.http;
	// Snapshot the caller's request-level auth principal BEFORE moving the
	// session into the ephemeral slot. This principal (derived by the
	// `SurrealAuth` middleware from Basic/Bearer headers on THIS request)
	// is compared to the target session's stored principal to prevent
	// session hijack across callers - see `Http::verify_caller_for_session`.
	let caller_au: Arc<Auth> = Arc::clone(&session.au);
	// Isolate this request's session under a unique key to prevent
	// concurrent requests from racing on a shared session slot.
	let request_session_id = Uuid::new_v4();
	rpc.register_ephemeral_session(request_session_id, Arc::new(RwLock::new(session)));
	// Check to see available memory
	if ALLOC.is_beyond_threshold() {
		rpc.remove_ephemeral_session(&request_session_id);
		return Err(NetError::ServerOverloaded.into());
	}
	// Parse the HTTP request body
	let result = match fmt.req_http(body, rec_limit) {
		Ok(req) => {
			// Preserve the raw client-provided session_id for methods that
			// require an explicit ID (attach/detach).
			let client_session: Option<Uuid> = req.session_id.map(Into::into);
			let session_id = client_session.unwrap_or(request_session_id);
			// Echo back the request id and client-supplied session id
			// (if any) so HTTP responses match the WebSocket convention.
			let req_id = req.id;
			let method = req.method;
			// Ownership gate: if the client supplied a session id that targets an existing attached
			// session, the caller's request-level auth principal must match the
			// session's stored principal. `Method::Attach` is the only
			// exception - it creates a new session and has no prior
			// principal to match against (the trait-level `attach` then
			// enforces the global cap and UUID uniqueness). All other
			// methods, including `Method::Detach`, go through the gate.
			//
			// When `client_session == Some(request_session_id)` we
			// deliberately skip verification: the client happened to
			// specify the ephemeral id, which matches the caller's own
			// auth by construction. This also avoids a collision oracle.
			let gate_result: Result<(), surrealdb_types::Error> = if method == Method::Attach {
				Ok(())
			} else if let Some(cid) = client_session
				&& cid != request_session_id
			{
				rpc.verify_caller_for_session(&cid, caller_au.as_ref()).await
			} else {
				Ok(())
			};
			// Execute the specified method only if the gate allows.
			let res = match gate_result {
				Ok(()) => {
					RpcProtocol::execute(
						rpc,
						req.txn.map(Into::into),
						session_id,
						client_session,
						method,
						req.params,
					)
					.await
				}
				Err(err) => Err(err),
			};
			// Build the HTTP response. Do not use `?` here: a failure from
			// `res_http` would short-circuit the function and bypass the
			// ephemeral-session cleanup below, leaking an entry per failed
			// serialization for the server lifetime.
			let db_response = match res {
				Ok(result) => DbResponse::success(req_id, client_session, result),
				Err(err) => DbResponse::failure(req_id, client_session, err),
			};
			fmt.res_http(db_response).map_err(Into::into)
		}
		Err(err) => Err(err.into()),
	};
	// Clean up the per-request session
	rpc.remove_ephemeral_session(&request_session_id);
	result
}
