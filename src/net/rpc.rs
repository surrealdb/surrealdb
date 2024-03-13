use std::collections::BTreeMap;

use crate::cnf;
use crate::dbs::DB;
use crate::err::Error;
use crate::rpc::connection::Connection;
use crate::rpc::failure::Failure;
use crate::rpc::format::Format;
use crate::rpc::format::PROTOCOLS;
use crate::rpc::request::Request;
use crate::rpc::WEBSOCKETS;
use axum::routing::get;
use axum::routing::post;
use axum::TypedHeader;
use axum::{
	extract::ws::{WebSocket, WebSocketUpgrade},
	response::IntoResponse,
	Extension, Router,
};
use bytes::Bytes;
use http::HeaderValue;
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
use surrealdb::rpc::method::Method;
use surrealdb::sql::Array;
use tower_http::request_id::RequestId;
use uuid::Uuid;

use super::headers::Accept;
use super::headers::ContentType;
use super::output;

use surrealdb::rpc::context::RpcContext;

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/rpc", get(get_handler)).route("/rpc", post(post_handler))
}

async fn get_handler(
	ws: WebSocketUpgrade,
	Extension(id): Extension<RequestId>,
	Extension(sess): Extension<Session>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Check if there is a request id header specified
	let id = match id.header_value().is_empty() {
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
	};
	// Check if a connection with this id already exists
	if WEBSOCKETS.read().await.contains_key(&id) {
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
		.on_upgrade(move |socket| handle_socket(socket, sess, id)))
}

async fn handle_socket(ws: WebSocket, sess: Session, id: Uuid) {
	// Check if there is a WebSocket protocol specified
	let format = match ws.protocol().map(HeaderValue::to_str) {
		// Any selected protocol will always be a valie value
		Some(protocol) => protocol.unwrap().into(),
		// No protocol format was specified
		_ => Format::None,
	};
	//
	// Create a new connection instance
	let rpc = Connection::new(id, sess, format);
	// Serve the socket connection requests
	Connection::serve(rpc, ws).await;
}

async fn post_handler(
	Extension(session): Extension<Session>,
	output: Option<TypedHeader<Accept>>,
	content_type: TypedHeader<ContentType>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	let kvs = DB.get().unwrap();

	let mut rpc_ctx = RpcContext {
		session,
		kvs,
		vars: BTreeMap::new(),
		lq_handler: None,
	};

	let req: Request = match *content_type {
		ContentType::ApplicationJson => surrealdb::sql::value(
			std::str::from_utf8(&body).map_err(|e| Error::Other(e.to_string()))?,
		)?
		.try_into()
		.or(Err(Error::Other("Error:".to_string())))?,
		ContentType::ApplicationCbor => todo!(),
		ContentType::ApplicationPack => todo!(),
		_ => return Err(Error::InvalidType),
	};

	// let method = Method::parse("info");
	let res = rpc_ctx.execute(Method::parse(req.method), req.params).await;

	match res {
		Ok(res) => match output.as_deref() {
			// Simple serialization
			Some(Accept::ApplicationJson) => Ok(output::json(&output::simplify(res))),
			Some(Accept::ApplicationCbor) => Ok(output::cbor(&output::simplify(res))),
			Some(Accept::ApplicationPack) => Ok(output::pack(&output::simplify(res))),
			// Internal serialization
			Some(Accept::Surrealdb) => Ok(output::full(&res)),
			// An incorrect content-type was requested
			_ => Err(Error::InvalidType),
		},
		// There was an error when executing the query
		Err(err) => Err(Error::from(err)),
	}
}
