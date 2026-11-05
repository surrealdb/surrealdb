use std::str::FromStr;
use std::sync::Arc;

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
use surrealdb_core::kvs::Datastore;
use surrealdb_core::mem::RefusalSite;
use surrealdb_core::rpc::format::{Format, PROTOCOLS};
use surrealdb_rpc::DbResponse;
use surrealdb_rpc::capabilities::RouteTarget;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
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
use crate::rpc::socket::{RpcSocket, RpcUpgrade};
use crate::rpc::websocket::Websocket;

pub fn router() -> Router<Arc<RpcState>> {
	Router::new()
		.route("/rpc", options(|| async {}).get(get_handler).post(post_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_RPC_BODY_SIZE))
}

async fn get_handler(
	ws: RpcUpgrade,
	Extension(state): Extension<AppState>,
	Extension(id): Extension<RequestId>,
	Extension(mut session): Extension<Session>,
	cookie_policy: Option<Extension<super::auth::CookiePolicy>>,
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
	// Enforce the configured origin allow-list on WebSocket upgrades from
	// browsers or requests carrying an explicit Origin header.
	if let Some(Extension(cookie_policy)) = cookie_policy
		&& !cookie_policy.origins.is_empty()
		&& !cookie_policy.origins.iter().any(|o| o.as_bytes() == b"*")
	{
		let gate = super::auth::browser_origin_allowed(&headers, &cookie_policy.origins);
		if gate == super::auth::Gate::Denied
			|| (gate == super::auth::Gate::NotBrowser && headers.contains_key(http::header::ORIGIN))
		{
			return Err(NetError::ForbiddenOrigin);
		}
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
	// The WebSocket's limits and buffers
	let config = WebSocketConfig::default()
		// The largest frame accepted, to prevent oversized frames
		.max_frame_size(Some(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE))
		// The largest message accepted, to prevent memory exhaustion
		.max_message_size(Some(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE))
		// The read buffer for incoming data
		.read_buffer_size(*cnf::WEBSOCKET_READ_BUFFER_SIZE)
		// The write buffer for outgoing data
		.write_buffer_size(*cnf::WEBSOCKET_WRITE_BUFFER_SIZE)
		// The write buffer past which backpressure applies
		.max_write_buffer_size(*cnf::WEBSOCKET_MAX_WRITE_BUFFER_SIZE);
	// Now let's upgrade the WebSocket connection
	Ok(ws
		// Set the potential WebSocket protocols (JSON, CBOR, etc.)
		.protocols(PROTOCOLS)
		// Handle the WebSocket upgrade and process messages
		.on_upgrade(
			config,
			// Handle WebSocket upgrade failures with appropriate logging
			|err| warn!("Failed to upgrade WebSocket connection: {err}"),
			move |socket| {
				handle_socket(Arc::clone(&state.datastore), rpc_state, socket, session, id)
			},
		))
}

async fn handle_socket(
	datastore: Arc<Datastore>,
	state: Arc<RpcState>,
	ws: RpcSocket,
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

	let rec_limit = db.parser_config().max_object_parsing_depth as usize;
	// Get the input format from the Content-Type header
	let in_fmt: Format = (&content_type).into();
	// Check that the input format is a valid format
	if matches!(in_fmt, Format::Unsupported) {
		return Err(NetError::InvalidType.into());
	}
	// Get the output format from the Accept header
	let out: Option<Format> = accept.as_deref().map(Into::into);
	// Check that the input format and the output format match. A shaped response answers a
	// CBOR request: shaped is CBOR with result rows as shaped blocks.
	if let Some(out) = out
		&& in_fmt != out
		&& !matches!(
			(in_fmt, out),
			(Format::Cbor, Format::Cbor2 | Format::Shaped)
				| (Format::Cbor2, Format::Cbor2 | Format::Shaped)
		) {
		return Err(NetError::InvalidType.into());
	}
	let out_fmt = out.unwrap_or(in_fmt);
	// Check to see available memory
	if crate::ntw::memory_guard::admit(RefusalSite::Http).is_some() {
		return Err(NetError::ServerOverloaded.into());
	}
	// Parse the HTTP request body
	let req = in_fmt.req_http(body, rec_limit)?;
	// Echo back the request id and client-supplied session id (if any) so
	// HTTP responses match the WebSocket convention.
	let req_id = req.id;
	let client_session: Option<Uuid> = req.session_id.map(Into::into);
	// The shared transport answers this request in `out_fmt`.
	let res = surrealdb_core::rpc::with_response_format(
		out_fmt.row_format(),
		rpc_state.http.handle(
			session,
			req.txn.map(Into::into),
			client_session,
			req.method,
			req.params,
		),
	)
	.await;
	let db_response = match res {
		Ok(result) => DbResponse::success(req_id, client_session, result),
		Err(err) => DbResponse::failure(req_id, client_session, err),
	};
	Ok(out_fmt.res_http(db_response)?)
}
