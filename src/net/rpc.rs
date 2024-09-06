use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use super::headers::SurrealId;
use crate::cnf;
use crate::cnf::HTTP_MAX_RPC_BODY_SIZE;
use crate::err::Error;
use crate::rpc::connection::Connection;
use crate::rpc::format::HttpFormat;
use crate::rpc::post_context::PostRpcContext;
use crate::rpc::response::IntoRpcResponse;
use crate::rpc::RpcState;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::routing::options;
use axum::{
	extract::ws::{WebSocket, WebSocketUpgrade},
	response::IntoResponse,
	Extension, Router,
};
use axum_extra::headers::Header;
use axum_extra::TypedHeader;
use bytes::Bytes;
use http::HeaderMap;
use http::HeaderValue;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb::rpc::format::Format;
use surrealdb::rpc::format::PROTOCOLS;
use surrealdb::rpc::method::Method;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::RequestId;
use uuid::Uuid;

use super::headers::Accept;
use super::headers::ContentType;
use super::AppState;

use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::rpc::rpc_context::RpcContext;

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
	Extension(mut sess): Extension<Session>,
	State(rpc_state): State<Arc<RpcState>>,
	headers: HeaderMap,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Rpc) {
		return Err(Error::OperationForbidden);
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
						Err(_) => return Err(Error::Request),
					}
				}
				Err(_) => return Err(Error::Request),
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
					Err(_) => return Err(Error::Request),
				},
				// The request id contained invalid characters
				Err(_) => return Err(Error::Request),
			},
		},
	};

	// Store connection id in session
	sess.id = Some(id.to_string());

	// Check if a connection with this id already exists
	if rpc_state.web_sockets.read().await.contains_key(&id) {
		return Err(Error::Request);
	}
	// Now let's upgrade the WebSocket connection
	Ok(ws
		// Set the potential WebSocket protocols
		.protocols(PROTOCOLS)
		// Set the maximum WebSocket frame size
		.max_frame_size(*cnf::WEBSOCKET_MAX_FRAME_SIZE)
		// Set the maximum WebSocket message size
		.max_message_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
		// Handle the WebSocket upgrade and process messages
		.on_upgrade(move |socket| {
			handle_socket(state.datastore.clone(), rpc_state, socket, sess, id)
		}))
}

async fn handle_socket(
	datastore: Arc<Datastore>,
	state: Arc<RpcState>,
	ws: WebSocket,
	sess: Session,
	id: Uuid,
) {
	// Check if there is a WebSocket protocol specified
	let format = match ws.protocol().map(HeaderValue::to_str) {
		// Any selected protocol will always be a valid value
		Some(protocol) => protocol.unwrap().into(),
		// No protocol format was specified
		_ => Format::None,
	};
	// Format::Unsupported is not in the PROTOCOLS list so cannot be the value of format here
	// Create a new connection instance
	let rpc = Connection::new(datastore, state, id, sess, format);
	// Serve the socket connection requests
	Connection::serve(rpc, ws).await;
}

async fn post_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	output: Option<TypedHeader<Accept>>,
	content_type: TypedHeader<ContentType>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Rpc) {
		return Err(Error::OperationForbidden);
	}
	let fmt: Format = content_type.deref().into();
	let out_fmt: Option<Format> = output.as_deref().map(Into::into);
	if let Some(out_fmt) = out_fmt {
		if fmt != out_fmt {
			return Err(Error::InvalidType);
		}
	}
	if fmt == Format::Unsupported || fmt == Format::None {
		return Err(Error::InvalidType);
	}

	let mut rpc_ctx = PostRpcContext::new(&state.datastore, session, BTreeMap::new());

	match fmt.req_http(body) {
		Ok(req) => {
			let res = rpc_ctx.execute(Method::parse(req.method), req.params).await;
			fmt.res_http(res.into_response(None)).map_err(Error::from)
		}
		Err(err) => Err(Error::from(err)),
	}
}
