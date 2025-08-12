mod api;
mod auth;
pub mod client_ip;
pub mod error;
mod export;
mod gql;
pub(crate) mod headers;
mod health;
mod import;
mod input;
mod key;
mod ml;
pub(crate) mod output;
mod params;
mod rpc;
mod signals;
mod signin;
mod signup;
mod sql;
mod sync;
mod tracer;
mod version;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::response::Redirect;
use axum::routing::get;
use axum::{Router, middleware};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use http::header;
use surrealdb::headers::{AUTH_DB, AUTH_NS, DB, ID, NS};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::ServiceBuilderExt;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::auth::AsyncRequireAuthorizationLayer;
#[cfg(feature = "http-compression")]
use tower_http::compression::CompressionLayer;
#[cfg(feature = "http-compression")]
use tower_http::compression::predicate::{NotForContentType, Predicate, SizeAbove};
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::MakeRequestUuid;
use tower_http::sensitive_headers::{
	SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer,
};
use tower_http::trace::TraceLayer;

use crate::cli::CF;
use crate::cnf;
use crate::core::dbs::capabilities::ExperimentalTarget;
use crate::core::kvs::Datastore;
use crate::net::signals::graceful_shutdown;
use crate::rpc::{RpcState, notifications};
use crate::telemetry::metrics::HttpMetricsLayer;

const LOG: &str = "surrealdb::net";

///
/// AppState is used to share data between routes.
#[derive(Clone)]
pub struct AppState {
	pub client_ip: client_ip::ClientIp,
	pub datastore: Arc<Datastore>,
}

pub async fn init(ds: Arc<Datastore>, ct: CancellationToken) -> Result<()> {
	// Get local copy of options
	let opt = CF.get().unwrap();

	let app_state = AppState {
		client_ip: opt.client_ip,
		datastore: ds.clone(),
	};

	// Specify headers to be obfuscated from all requests/responses
	let headers: Arc<[_]> = Arc::new([
		header::AUTHORIZATION,
		header::PROXY_AUTHORIZATION,
		header::COOKIE,
		header::SET_COOKIE,
	]);

	// Build the middleware to our service.
	let service = ServiceBuilder::new()
		// Ensure any panics are caught and handled
		.catch_panic()
		// Ensure a X-Request-Id header is specified
		.set_x_request_id(MakeRequestUuid)
		// Ensure the Request-Id is sent in the response
		.propagate_x_request_id()
		// Limit the number of requests handled at once
		.concurrency_limit(*cnf::NET_MAX_CONCURRENT_REQUESTS);

	#[cfg(feature = "http-compression")]
	let service = service.layer(
		CompressionLayer::new().compress_when(
			// Don't compress below 512 bytes
			SizeAbove::new(512)
				// Don't compress gRPC
				.and(NotForContentType::GRPC)
				// Don't compress images
				.and(NotForContentType::IMAGES),
		),
	);

	#[cfg(feature = "http-compression")]
	let allow_header = [
		http::header::ACCEPT,
		http::header::ACCEPT_ENCODING,
		http::header::AUTHORIZATION,
		http::header::CONTENT_TYPE,
		http::header::ORIGIN,
		NS.clone(),
		DB.clone(),
		ID.clone(),
		AUTH_NS.clone(),
		AUTH_DB.clone(),
	];

	#[cfg(not(feature = "http-compression"))]
	let allow_header = [
		http::header::ACCEPT,
		http::header::AUTHORIZATION,
		http::header::CONTENT_TYPE,
		http::header::ORIGIN,
		NS.clone(),
		DB.clone(),
		ID.clone(),
		AUTH_NS.clone(),
		AUTH_DB.clone(),
	];

	let service = service
		.layer(AddExtensionLayer::new(app_state))
		.layer(middleware::from_fn(client_ip::client_ip_middleware))
		.layer(SetSensitiveRequestHeadersLayer::from_shared(Arc::clone(&headers)))
		.layer(
			TraceLayer::new_for_http()
				.make_span_with(tracer::HttpTraceLayerHooks)
				.on_request(tracer::HttpTraceLayerHooks)
				.on_response(tracer::HttpTraceLayerHooks)
				.on_failure(tracer::HttpTraceLayerHooks),
		)
		.layer(HttpMetricsLayer)
		.layer(SetSensitiveResponseHeadersLayer::from_shared(headers))
		.layer(AsyncRequireAuthorizationLayer::new(auth::SurrealAuth))
		.layer(headers::add_server_header(!opt.no_identification_headers))
		.layer(headers::add_version_header(!opt.no_identification_headers))
		// Apply CORS headers to relevant responses
		.layer(
			CorsLayer::new()
				.allow_methods([
					http::Method::GET,
					http::Method::PUT,
					http::Method::POST,
					http::Method::PATCH,
					http::Method::DELETE,
					http::Method::OPTIONS,
				])
				.allow_headers(allow_header)
				// allow requests from any origin
				.allow_origin(Any)
				.max_age(Duration::from_secs(86400)),
		);

	let axum_app = Router::<Arc<RpcState>>::new()
		// Redirect until we provide a UI
		.route("/", get(|| async { Redirect::temporary(cnf::APP_ENDPOINT) }))
		.route("/status", get(|| async {}))
		.merge(health::router())
		.merge(export::router())
		.merge(import::router())
		.merge(rpc::router())
		.merge(version::router())
		.merge(sync::router())
		.merge(sql::router())
		.merge(signin::router())
		.merge(signup::router())
		.merge(key::router())
		.merge(ml::router())
		.merge(api::router());
	//.merge(gql::router(ds.clone()));

	if ds.get_capabilities().allows_experimental(&ExperimentalTarget::GraphQL) {
		warn!(
			"❌🔒IMPORTANT: GraphQL is a pre-release feature with known security flaws. This is not recommended for production use.🔒❌"
		);
	}

	let axum_app = axum_app.layer(service);

	// Get a new server handler
	let handle = Handle::new();

	let rpc_state = Arc::new(RpcState::new());

	// Setup the graceful shutdown handler
	let shutdown_handler = graceful_shutdown(rpc_state.clone(), ct.clone(), handle.clone());

	let axum_app = axum_app.with_state(rpc_state.clone());

	// Spawn a task to handle notifications
	tokio::spawn(async move { notifications(ds, rpc_state, ct.clone()).await });
	// If a certificate and key are specified, then setup TLS
	let res = if let (Some(cert), Some(key)) = (&opt.crt, &opt.key) {
		// Configure certificate and private key used by https
		let tls = RustlsConfig::from_pem_file(cert, key).await?;
		// Setup the Axum server with TLS
		let server = axum_server::bind_rustls(opt.bind, tls);
		// Log the server startup to the CLI
		info!(target: LOG, "Started web server on {}", &opt.bind);
		// Start the server and listen for connections
		server
			.handle(handle)
			.serve(axum_app.into_make_service_with_connect_info::<SocketAddr>())
			.await
	} else {
		// Setup the Axum server
		let server = axum_server::bind(opt.bind);
		// Log the server startup to the CLI
		info!(target: LOG, "Started web server on {}", &opt.bind);
		// Start the server and listen for connections
		server
			.handle(handle)
			.serve(axum_app.into_make_service_with_connect_info::<SocketAddr>())
			.await
	};
	// Catch the error and try to provide some guidance
	if let Err(e) = res {
		if opt.bind.port() < 1024 {
			if let io::ErrorKind::PermissionDenied = e.kind() {
				error!(target: LOG, "Binding to ports below 1024 requires privileged access or special permissions.");
			}
		}
		return Err(e.into());
	}
	// Wait for the shutdown to finish
	let _ = shutdown_handler.await;
	// Log the server shutdown to the CLI
	info!(target: LOG, "Web server stopped. Bye!");

	Ok(())
}
