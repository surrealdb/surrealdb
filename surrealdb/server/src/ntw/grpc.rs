//! The gRPC route.
//!
//! `SurrealDBService` is mounted on the same router, and therefore the same
//! listener, as the HTTP and WebSocket transports: a client picks a transport
//! by scheme (`http://`, `ws://`, `grpc://`) and nothing else about the
//! deployment changes. gRPC is HTTP/2, which the server negotiates the same
//! way it always has -- by the connection preface on a plaintext port, and by
//! ALPN on a TLS one -- so no second port and no separate listener is
//! involved.
//!
//! Mounting it as a route rather than beside the router is what puts gRPC
//! behind the same middleware stack as everything else: the readiness gate,
//! the `Authorization` header handling that the RPC handler's ownership gate
//! depends on, request tracing, and the per-request metrics all apply
//! unchanged.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::any;
use axum::{Extension, Router};
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SurrealDbServiceServer;
use tower_service::Service;

use super::AppState;
use crate::cnf::HTTP_MAX_RPC_BODY_SIZE;
use crate::ntw::error::Error as NetError;
use crate::rpc::RpcState;
use crate::rpc::grpc::GrpcService;

/// Every method on the service, matched as one route.
///
/// The wildcard hands unknown method names to the generated service, which
/// answers them with gRPC's own `UNIMPLEMENTED` status rather than an HTTP 404
/// a gRPC client would have to guess at.
const SERVICE_ROUTE: &str = "/surrealdb.protocol.rpc.v1.SurrealDBService/{*method}";

pub fn router() -> Router<Arc<RpcState>> {
	Router::new().route(SERVICE_ROUTE, any(handler))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	State(rpc_state): State<Arc<RpcState>>,
	request: Request,
) -> Result<Response, NetError> {
	// gRPC is an RPC transport, so it answers to the same route capability the
	// other two do: an operator denying `rpc` denies all three.
	if !state.datastore.allows_http_route(&RouteTarget::Rpc) {
		warn!("Capabilities denied gRPC route request attempt, target: '{}'", &RouteTarget::Rpc);
		return Err(NetError::ForbiddenRoute(RouteTarget::Rpc.to_string()));
	}
	// The service is built per request so it can carry that request's
	// authenticated session, which is the principal the handler's ownership
	// gate compares against the session a request names.
	let mut service = SurrealDbServiceServer::new(GrpcService::new(rpc_state, session))
		// Bound what a client may send us, matching the HTTP RPC body limit.
		// The encoding limit is deliberately left at its default: a query
		// result or an export chunk is bounded by the data, not by what a
		// client may push at us.
		.max_decoding_message_size(*HTTP_MAX_RPC_BODY_SIZE);
	let response = match service.call(request).await {
		Ok(response) => response,
		// The generated service reports failures as gRPC statuses, so its
		// error type is uninhabited.
		Err(error) => match error {},
	};
	Ok(response.map(axum::body::Body::new))
}

#[cfg(test)]
mod tests {
	use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SERVICE_NAME;

	use super::SERVICE_ROUTE;

	/// The route is written out so it reads as a path, but it has to stay in
	/// step with the service the protocol generates: a rename there would
	/// otherwise leave every method unroutable.
	#[test]
	fn the_route_matches_the_generated_service_name() {
		assert_eq!(SERVICE_ROUTE, format!("/{SERVICE_NAME}/{{*method}}"));
	}
}
