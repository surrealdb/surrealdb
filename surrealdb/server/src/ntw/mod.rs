pub mod api;
mod auth;
pub mod client_ip;
pub mod error;
pub mod export;
#[cfg(feature = "graphql")]
pub mod gql;
pub(crate) mod headers;
pub mod health;
pub mod import;
mod input;
pub mod key;
pub mod ml;
pub(crate) mod output;
mod params;
pub mod rpc;
mod signals;
pub mod signin;
pub mod signup;
pub mod sql;
pub mod sync;
mod tracer;
pub mod version;

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
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::ServiceBuilderExt;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use tower_http::compression::CompressionLayer;
use tower_http::compression::predicate::{NotForContentType, Predicate, SizeAbove};
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::MakeRequestUuid;
use tower_http::sensitive_headers::{
	SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer,
};
use tower_http::trace::TraceLayer;

use crate::cli::Config;
use crate::cnf::{self, HttpServerConfig, ServerConfig};
use crate::ntw::signals::graceful_shutdown;
use crate::rpc::{RpcState, notifications};
use crate::telemetry::metrics::HttpMetricsLayer;

const LOG: &str = "surrealdb::net";

/// Factory for constructing the top-level Axum Router used by the HTTP server.
///
/// Embedders can provide their own implementation to add or remove routes, or wrap
/// additional middleware. The default binary uses [`CommunityComposer`].
///
/// # Examples
///
/// Extend the default community router with additional custom routes:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use axum::{Router, routing::get};
/// use surreal::RouterFactory;
/// use surreal::rpc::RpcState;
/// use surreal::core::CommunityComposer;
///
/// struct MyComposer;
///
/// impl RouterFactory for MyComposer {
///     fn configure_router() -> Router<Arc<RpcState>> {
///         let router = CommunityComposer::configure_router();
///         router.merge(
///             Router::new()
///                 .route("/custom", get(|| async { "Hello from custom route" }))
///         )
///     }
/// }
/// ```
///
/// Build a minimal router from individual endpoint routers:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use axum::Router;
/// use surreal::RouterFactory;
/// use surreal::rpc::RpcState;
/// use surreal::ntw::{health, sql, rpc};
///
/// struct MinimalComposer;
///
/// impl RouterFactory for MinimalComposer {
///     fn configure_router() -> Router<Arc<RpcState>> {
///         Router::new()
///             .merge(health::router())
///             .merge(sql::router())
///             .merge(rpc::router())
///     }
/// }
/// ```
///
/// [`CommunityComposer`]: surrealdb_core::CommunityComposer
pub trait RouterFactory {
	/// Build and return the base Router. The server will attach shared state and layers.
	fn configure_router(http: &HttpServerConfig) -> Router<Arc<RpcState>>;
}

/// Default router implementation for the community edition.
///
/// Provides the standard set of HTTP routes shipped with the `surreal` binary.
/// Consumers embedding SurrealDB can implement `RouterFactory` on their own
/// composer to customize routes.
impl RouterFactory for CommunityComposer {
	fn configure_router(http: &HttpServerConfig) -> Router<Arc<RpcState>> {
		let router = Router::<Arc<RpcState>>::new()
			// Redirect until we provide a UI
			.route("/", get(|| async { Redirect::temporary(cnf::APP_ENDPOINT) }))
			.route("/status", get(|| async {}))
			.merge(health::router())
			.merge(export::router())
			.merge(import::router(http.max_import_body_size))
			.merge(rpc::router(http.max_rpc_body_size))
			.merge(version::router())
			.merge(sync::router())
			.merge(sql::router(http.max_sql_body_size))
			.merge(signin::router(http.max_signin_body_size))
			.merge(signup::router(http.max_signup_body_size))
			.merge(key::router(http.max_key_body_size))
			.merge(ml::router(http.max_ml_body_size))
			.merge(api::router(http.max_api_body_size));

		#[cfg(feature = "graphql")]
		let router = router.merge(gql::router());

		router
	}
}

///
/// AppState is used to share data between routes.
#[derive(Clone)]
pub struct AppState {
	pub client_ip: client_ip::ClientIp,
	pub datastore: Arc<Datastore>,
}

/// Initialize and start the HTTP server.
///
/// Sets up the Axum HTTP server with middleware, routing, and TLS configuration.
///
/// # Parameters
/// - `opt`: Server configuration including bind address and TLS settings
/// - `ds`: The datastore instance to serve
/// - `ct`: Cancellation token for graceful shutdown
///
/// # Generic parameters
/// - `F`: Router factory type implementing `RouterFactory`
pub async fn init<F: RouterFactory>(
	opt: &Config,
	ds: Arc<Datastore>,
	ct: CancellationToken,
	server_config: &ServerConfig,
) -> Result<()> {
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
		.concurrency_limit(server_config.http.max_concurrent_requests);

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
		.layer(headers::add_server_header(!opt.no_identification_headers)?)
		.layer(headers::add_version_header(!opt.no_identification_headers)?)
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

	let axum_app = F::configure_router(&server_config.http);

	let axum_app = axum_app.layer(service);

	// Get a new server handler
	let handle = Handle::new();

	// Create RpcState with persistent HTTP handler
	let rpc_state = Arc::new(RpcState::new(
		ds.clone(),
		surrealdb_core::dbs::Session::default(),
		server_config.websocket.clone(),
	));

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
		if opt.bind.port() < 1024
			&& let io::ErrorKind::PermissionDenied = e.kind()
		{
			error!(target: LOG, "Binding to ports below 1024 requires privileged access or special permissions.");
		}
		return Err(e.into());
	}
	// Wait for the shutdown to finish
	let _ = shutdown_handler.await;
	// Log the server shutdown to the CLI
	info!(target: LOG, "Web server stopped. Bye!");

	Ok(())
}
