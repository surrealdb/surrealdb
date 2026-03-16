use std::str::FromStr;
use std::sync::Arc;

use async_graphql::Data;
use async_graphql::http::{ALL_WEBSOCKET_PROTOCOLS, WebSocketProtocols, WsMessage};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::http::header::SEC_WEBSOCKET_PROTOCOL;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use futures_util::{SinkExt, StreamExt, future};
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::gql::cache::GraphQLSchemaCache;
use tracing::instrument;

use crate::gql::GraphQLService;
use crate::ntw::AppState;
use crate::ntw::error::Error as NetError;
use crate::rpc::RpcState;

pub fn router() -> Router<Arc<RpcState>> {
	let service = GraphQLService::new();
	let cache = service.cache();
	Router::new().route("/graphql", get(ws_handler).post_service(service)).layer(Extension(cache))
}

#[instrument(skip_all)]
async fn ws_handler(
	ws: WebSocketUpgrade,
	headers: HeaderMap,
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Extension(cache): Extension<GraphQLSchemaCache>,
	State(rpc_state): State<Arc<RpcState>>,
) -> impl IntoResponse {
	let datastore = &state.datastore;

	if !datastore.allows_http_route(&RouteTarget::GraphQL) {
		warn!(
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::GraphQL
		);
		return NetError::ForbiddenRoute(RouteTarget::GraphQL.to_string()).into_response();
	}

	if session.ns.is_none() {
		info!("GraphQL WebSocket rejected: no namespace specified");
		return NetError::Request.into_response();
	}
	if session.db.is_none() {
		info!("GraphQL WebSocket rejected: no database specified");
		return NetError::Request.into_response();
	}

	let schema = match cache.get_schema(datastore, &session).await {
		Ok(schema) => schema,
		Err(err) => {
			info!(?err, "error generating GraphQL schema for websocket");
			return NetError::Request.into_response();
		}
	};

	let mut data = Data::default();
	data.insert(datastore.clone());
	data.insert(Arc::new(session));
	data.insert(rpc_state.notification_router.clone());

	let protocol = select_ws_protocol(&headers);

	ws.protocols(ALL_WEBSOCKET_PROTOCOLS).on_upgrade(move |socket| async move {
		serve_graphql_ws(socket, schema, protocol, data).await;
	})
}

fn select_ws_protocol(headers: &HeaderMap) -> WebSocketProtocols {
	headers
		.get(SEC_WEBSOCKET_PROTOCOL)
		.and_then(|value| value.to_str().ok())
		.and_then(|protocols| {
			protocols
				.split(',')
				.find_map(|protocol| WebSocketProtocols::from_str(protocol.trim()).ok())
		})
		// Default to graphql-transport-ws when no explicit protocol is supplied.
		.unwrap_or(WebSocketProtocols::GraphQLWS)
}

#[instrument(skip_all)]
async fn serve_graphql_ws(
	socket: WebSocket,
	schema: async_graphql::dynamic::Schema,
	protocol: WebSocketProtocols,
	data: Data,
) {
	let (mut sink, stream) = socket.split();

	let input = stream
		.take_while(|res| future::ready(res.is_ok()))
		.map(Result::unwrap)
		.filter_map(|msg| {
			if let Message::Text(_) | Message::Binary(_) = msg {
				future::ready(Some(msg))
			} else {
				future::ready(None)
			}
		})
		.map(Message::into_data);

	let mut stream = async_graphql::http::WebSocket::new(schema, input, protocol)
		.connection_data(data)
		.map(|msg| match msg {
			WsMessage::Text(text) => Message::Text(text.into()),
			WsMessage::Close(code, status) => Message::Close(Some(CloseFrame {
				code,
				reason: status.into(),
			})),
		});

	while let Some(item) = stream.next().await {
		if sink.send(item).await.is_err() {
			break;
		}
	}
}
