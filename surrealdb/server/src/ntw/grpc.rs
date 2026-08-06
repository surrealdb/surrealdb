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
use tonic::codec::CompressionEncoding;
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

/// The compressed message codecs the service accepts and will answer with, most
/// preferred first, paired with the `grpc-accept-encoding` token that names each
/// one on the wire.
///
/// One list drives both the service's configuration and what `GetCapabilities`
/// reports, because a client that is told about a codec the route does not
/// enable spends a rejected call discovering otherwise.
const COMPRESSED_ENCODINGS: &[(&str, CompressionEncoding)] =
	&[("zstd", CompressionEncoding::Zstd), ("gzip", CompressionEncoding::Gzip)];

/// The codec list as the capability handshake reports it.
///
/// `identity` comes last and is always accepted whether or not it is named;
/// naming it is what makes the answer read as "these and uncompressed" rather
/// than as the silence of a server too old to carry the field.
pub fn accepted_message_encodings() -> Vec<String> {
	COMPRESSED_ENCODINGS
		.iter()
		.map(|(token, _)| (*token).to_string())
		.chain(std::iter::once("identity".to_string()))
		.collect()
}

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
	// Accept a compressed request in any of these, and answer in one of them.
	// tonic picks the response encoding per request from the client's
	// `grpc-accept-encoding`, so a client advertising none — including one built
	// before this was enabled — still receives identity-encoded frames.
	for (_, encoding) in COMPRESSED_ENCODINGS {
		service = service.accept_compressed(*encoding).send_compressed(*encoding);
	}
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

	use super::{COMPRESSED_ENCODINGS, SERVICE_ROUTE, accepted_message_encodings};

	/// What the handshake advertises has to be what the route enables, plus
	/// `identity`. A codec named here but not enabled costs a client a rejected
	/// call; one enabled but not named costs it the compression it could have
	/// had.
	#[test]
	fn the_advertised_codecs_are_the_enabled_ones() {
		let advertised = accepted_message_encodings();
		let (named, identity) = advertised.split_at(advertised.len() - 1);
		assert_eq!(identity, ["identity"], "identity is always accepted and is named last");
		let enabled: Vec<&str> = COMPRESSED_ENCODINGS.iter().map(|(token, _)| *token).collect();
		assert_eq!(named, enabled.as_slice());
	}

	/// The route is written out so it reads as a path, but it has to stay in
	/// step with the service the protocol generates: a rename there would
	/// otherwise leave every method unroutable.
	#[test]
	fn the_route_matches_the_generated_service_name() {
		assert_eq!(SERVICE_ROUTE, format!("/{SERVICE_NAME}/{{*method}}"));
	}
}
