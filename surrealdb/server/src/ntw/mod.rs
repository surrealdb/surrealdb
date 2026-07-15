pub mod api;
mod auth;
pub mod client_ip;
pub mod error;
pub mod export;
#[cfg(feature = "gql")]
pub mod gql;
#[cfg(feature = "graphql")]
pub mod graphql;
pub(crate) mod headers;
pub mod health;
pub mod import;
mod input;
pub mod key;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod ml;
pub(crate) mod output;
mod params;
mod ready;
pub mod rpc;
mod signals;
pub mod signin;
pub mod signup;
pub mod sql;
pub mod sync;
pub(crate) mod timeout;
mod tracer;
pub mod version;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::Result;
use axum::response::Redirect;
use axum::routing::get;
use axum::{Extension, Router, middleware};
use axum_server::Handle;
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use http::header;
use surrealdb::headers::{AUTH_DB, AUTH_NS, DB, ID, NS};
use surrealdb_core::CommunityComposer;
use surrealdb_core::channel::Receiver;
use surrealdb_core::kvs::{Datastore, TransactionBuilderFactory};
use surrealdb_types::Notification;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::ServiceBuilderExt;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::compression::predicate::{NotForContentType, Predicate, SizeAbove};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::request_id::MakeRequestUuid;
use tower_http::sensitive_headers::{
	SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer,
};
use tower_http::trace::TraceLayer;

/// Re-exported so an edition overriding `/ready` can reuse the community
/// readiness decision; pair it with [`community_router`] to omit the built-in
/// `/ready` route.
pub use self::ready::community_readiness;
use crate::cli::Config;
use crate::cnf;
use crate::ntw::signals::graceful_shutdown;
use crate::observe::{HttpMetricsLayer, MetricsState};
use crate::rpc::{RpcState, notifications};

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
/// use surrealdb_server::RouterFactory;
/// use surrealdb_server::rpc::RpcState;
/// use surrealdb_server::core::CommunityComposer;
///
/// struct MyComposer;
///
/// impl RouterFactory for MyComposer {
///     fn configure_router(router_state: Self::RouterState) -> Router<Arc<RpcState>> {
///         let router = CommunityComposer::configure_router(router_state);
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
/// use surrealdb_server::RouterFactory;
/// use surrealdb_server::rpc::RpcState;
/// use surrealdb_server::ntw::{health, sql, rpc};
///
/// struct MinimalComposer;
///
/// impl RouterFactory for MinimalComposer {
///     fn configure_router(_router_state: Self::RouterState) -> Router<Arc<RpcState>> {
///         Router::new()
///             .merge(health::router())
///             .merge(sql::router())
///             .merge(rpc::router())
///     }
/// }
/// ```
///
/// [`CommunityComposer`]: surrealdb_core::CommunityComposer
pub trait RouterFactory: TransactionBuilderFactory {
	/// Build and return the base Router. The server will attach shared state and layers.
	fn configure_router(router_state: Self::RouterState) -> Router<Arc<RpcState>>;
}

/// Default router implementation for the community edition.
///
/// Provides the standard set of HTTP routes shipped with the `surreal` binary.
/// Consumers embedding SurrealDB can implement `RouterFactory` on their own
/// composer to customize routes.
impl RouterFactory for CommunityComposer {
	fn configure_router(_router_state: Self::RouterState) -> Router<Arc<RpcState>> {
		community_router(&[])
	}
}

/// Build the community HTTP router, omitting any path in `exclude` so an edition
/// can register its own handler for that path without an axum merge collision
/// (axum panics on a duplicate `(path, method)`). Pass `&[]` for the standard
/// community router — [`CommunityComposer::configure_router`] does exactly that.
///
/// `/ready` is the route intended to be overridable this way: an edition that
/// layers an extra serve-readiness condition builds `community_router(&["/ready"])`,
/// then merges its own `/ready` that calls [`community_readiness`] and ANDs in
/// its condition.
pub fn community_router(exclude: &[&str]) -> Router<Arc<RpcState>> {
	let mut router = Router::<Arc<RpcState>>::new()
		// Redirect until we provide a UI
		.route("/", get(|| async { Redirect::temporary(cnf::APP_ENDPOINT) }))
		.route("/status", get(|| async {}))
		.merge(health::router());
	if !exclude.contains(&"/ready") {
		router = router.merge(ready::router());
	}
	router = router
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

	#[cfg(feature = "graphql")]
	{
		router = router.merge(graphql::router());
	}

	#[cfg(feature = "gql")]
	{
		router = router.merge(gql::router());
	}

	#[cfg(feature = "mcp")]
	{
		router = router.merge(mcp::router());
	}

	router
}

///
/// AppState is used to share data between routes.
#[derive(Clone)]
pub struct AppState {
	pub client_ip: client_ip::ClientIp,
	pub datastore: Arc<Datastore>,
	/// Shared metrics observer for non-tower-instrumented protocols
	/// (GraphQL, MCP). `None` when `SURREAL_METRICS_ENABLED=false` or no
	/// metrics reader is attached, in which case the recording sites
	/// short-circuit.
	pub metrics_observer: Option<Arc<crate::observe::metrics::MetricsObserver>>,
	/// Readiness signals (the startup-complete flag and heartbeat staleness
	/// limit) read by the `/ready` handler.
	pub readiness: Readiness,
}

/// Readiness signals shared with the HTTP layer to gate traffic during startup.
#[derive(Clone)]
pub struct Readiness {
	/// Whether the instance has finished starting up and is ready to serve
	/// user-facing queries. Flipped to `true` once the deferred startup work
	/// (import + credentials) has completed; read by the readiness gate and the
	/// `/ready` handler.
	pub ready: Arc<AtomicBool>,
	/// Maximum age of the current node's cluster heartbeat for `/ready` to
	/// consider the node healthy, derived from the node-membership refresh
	/// interval (see `start::init`). `None` disables the heartbeat check — used
	/// by embedder entrypoints that do not run the membership refresh task, so
	/// `/ready` reflects only the `ready` flag.
	pub max_heartbeat_age: Option<Duration>,
}

/// Configuration options for building a [`SurrealRouter`].
///
/// # Defaults
///
/// [`RouterOptions::default()`] provides sensible defaults:
/// - `client_ip`: [`ClientIp::Socket`] (extract the client IP from the raw socket)
/// - `no_identification_headers`: `false` (include `Server` and version headers)
/// - `allow_origin`: empty (allow all origins)
/// - `durable_session_ttl`: `None` (HTTP RPC sessions are in-memory only)
///
/// # Example
///
/// ```rust,ignore
/// use surrealdb_server::ntw::{RouterOptions, client_ip::ClientIp};
///
/// let opts = RouterOptions::default();
///
/// // Or customise:
/// let opts = RouterOptions {
///     client_ip: ClientIp::None,
///     no_identification_headers: true,
///     allow_origin: vec!["https://example.com".to_string()],
///     durable_session_ttl: None,
/// };
/// ```
#[derive(Clone, Debug)]
pub struct RouterOptions {
	/// Strategy for extracting the client IP address from incoming requests.
	pub client_ip: client_ip::ClientIp,
	/// When `true`, suppresses the `Server` and SurrealDB version response headers.
	pub no_identification_headers: bool,
	/// Allowed CORS origins. When empty (the default), all origins are allowed.
	/// Each entry should be a valid origin string (e.g. `"https://example.com"`).
	pub allow_origin: Vec<String>,
	/// When set, client-attached HTTP RPC sessions are persisted in the
	/// datastore — surviving server restarts and shared across cluster nodes
	/// — and expire after this idle TTL. `None` (the default) keeps sessions
	/// in-memory only.
	pub durable_session_ttl: Option<Duration>,
}

impl Default for RouterOptions {
	fn default() -> Self {
		Self {
			client_ip: client_ip::ClientIp::Socket,
			no_identification_headers: false,
			allow_origin: Vec::new(),
			durable_session_ttl: None,
		}
	}
}

impl From<&Config> for RouterOptions {
	fn from(cfg: &Config) -> Self {
		Self {
			client_ip: cfg.client_ip,
			no_identification_headers: cfg.no_identification_headers,
			allow_origin: cfg.allow_origin.clone(),
			durable_session_ttl: cfg.durable_session_ttl,
		}
	}
}

/// A fully-configured SurrealDB router ready to be served.
///
/// This struct is the result of [`SurrealRouter::build`] and contains the Axum
/// [`Router`] with all SurrealDB middleware and state applied, along with the
/// handles needed to run notification delivery and manage shutdown.
///
/// # For embedders
///
/// Use [`into_router`](SurrealRouter::into_router) to extract the `Router` and merge it
/// into your own Axum application. Then call
/// [`spawn_notifications`](SurrealRouter::spawn_notifications) to start LIVE query notification
/// delivery whenever you are ready.
///
/// # Shutdown
///
/// When the server is stopping, the embedder is responsible for:
/// 1. Cancelling the [`CancellationToken`] passed to [`build`](SurrealRouter::build) to stop
///    background tasks and notification delivery.
/// 2. Calling [`Datastore::shutdown`] on the shared datastore to deregister the node from the
///    cluster and release resources. You can use the convenience method
///    [`shutdown`](SurrealRouter::shutdown) which does this for you.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use surrealdb_server::ntw::{SurrealRouter, RouterOptions};
/// use surrealdb_server::core::{CommunityComposer, kvs::Datastore};
/// use tokio_util::sync::CancellationToken;
///
/// let ds = Arc::new(Datastore::new("memory").await?.with_notifications());
/// let ct = CancellationToken::new();
///
/// let opts = RouterOptions::default();
/// let surreal = SurrealRouter::build::<CommunityComposer>(opts, ds.clone(), ct.clone()).await?;
///
/// // Start notification delivery (returns a JoinHandle you can await or abort)
/// let notifications = surreal.spawn_notifications();
///
/// // Extract the router and merge with your own routes
/// let app = axum::Router::new()
///     .route("/custom", axum::routing::get(|| async { "hello" }))
///     .merge(surreal.into_router());
///
/// // Serve `app` with your own server setup...
///
/// // When shutting down, cancel the token and deregister the node from the cluster:
/// // ct.cancel();
/// // ds.shutdown().await.ok();
/// ```
pub struct SurrealRouter {
	router: Router,
	rpc_state: Arc<RpcState>,
	datastore: Arc<Datastore>,
	notifications: Receiver<Notification>,
	canceller: CancellationToken,
}

impl SurrealRouter {
	/// Build a fully-configured SurrealDB [`Router`] with all middleware and state.
	///
	/// This performs all the setup that [`init`] does (middleware layers, CORS,
	/// compression, auth, tracing, etc.) **without** binding to a socket or
	/// starting background tasks. The returned [`SurrealRouter`] can be merged
	/// into an embedder's own Axum application.
	///
	/// # Parameters
	/// - `opt`: Router-specific configuration (see [`RouterOptions`])
	/// - `ds`:  The [`Datastore`] instance to serve
	/// - `ct`:  A [`CancellationToken`] for cooperative shutdown
	///
	/// # Generic parameters
	/// - `F`: A [`RouterFactory`] that determines which routes are included
	pub async fn build<F: RouterFactory>(
		opt: impl Into<RouterOptions>,
		ds: Arc<Datastore>,
		notifications: Receiver<Notification>,
		ct: CancellationToken,
		router_state: F::RouterState,
	) -> Result<Self> {
		// Embedders that build the router directly are responsible for their own
		// startup sequencing, so the readiness gate is open from the start. The
		// heartbeat check is disabled (`None`): this path does not start the
		// node-membership refresh task, so the heartbeat would never refresh.
		let readiness = Readiness {
			ready: Arc::new(AtomicBool::new(true)),
			max_heartbeat_age: None,
		};
		Self::build_with_metrics::<F>(opt, ds, notifications, ct, router_state, None, readiness)
			.await
	}

	/// Build a fully-configured [`SurrealRouter`], optionally mounting the
	/// Prometheus `/metrics` endpoint.
	///
	/// When `metrics` is `Some(..)` the `/metrics` route is merged into the
	/// router tree before HTTP middleware is applied, so it participates in
	/// the standard auth/trace stack. Unauthenticated scrapers still succeed
	/// (the auth middleware allows anonymous sessions); the `/metrics`
	/// handler itself is responsible for filtering the response based on
	/// whether the session is authenticated as a root operator.
	pub async fn build_with_metrics<F: RouterFactory>(
		opt: impl Into<RouterOptions>,
		ds: Arc<Datastore>,
		notifications: Receiver<Notification>,
		ct: CancellationToken,
		router_state: F::RouterState,
		metrics: Option<MetricsState>,
		readiness: Readiness,
	) -> Result<Self> {
		let opt = opt.into();
		// Clone the readiness flag for the gate layer before moving `readiness`
		// into the shared app state.
		let gate_ready = Arc::clone(&readiness.ready);
		let app_state = AppState {
			client_ip: opt.client_ip,
			datastore: Arc::clone(&ds),
			metrics_observer: metrics.as_ref().map(|m| Arc::clone(&m.observer)),
			readiness,
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

		let allow_origin: AllowOrigin = if opt.allow_origin.is_empty() {
			Any.into()
		} else {
			let origins: Vec<http::HeaderValue> = opt
				.allow_origin
				.iter()
				.map(|o| o.parse().map_err(|_| anyhow::anyhow!("invalid CORS origin: {o}")))
				.collect::<Result<Vec<_>, _>>()?;
			AllowOrigin::list(origins)
		};

		let allow_header = vec![
			header::ACCEPT,
			header::ACCEPT_ENCODING,
			header::AUTHORIZATION,
			header::CONTENT_TYPE,
			header::ORIGIN,
			NS.clone(),
			DB.clone(),
			ID.clone(),
			AUTH_NS.clone(),
			AUTH_DB.clone(),
		];

		// MCP protocol headers for cross-origin browser clients
		#[cfg(feature = "mcp")]
		let (allow_header, mcp_expose_headers) = {
			let mut allow_header = allow_header;
			allow_header.push(http::HeaderName::from_static("mcp-session-id"));
			allow_header.push(http::HeaderName::from_static("mcp-protocol-version"));
			allow_header.push(http::HeaderName::from_static("last-event-id"));
			allow_header.push(http::HeaderName::from_static("x-custom-auth-headers"));
			(
				allow_header,
				vec![
					http::HeaderName::from_static("mcp-session-id"),
					http::HeaderName::from_static("mcp-protocol-version"),
				],
			)
		};

		// Clone the community observer once up-front so the RPC state can
		// adjust the LIVE-query gauge / notification counter without going
		// through the fan-out (those are community-side only).
		let prometheus_observer = metrics.as_ref().map(|m| Arc::clone(&m.observer));
		// Source the HTTP tower layer's observer from the datastore so
		// `NetworkBytesEvent`s reach whichever observer the embedder
		// installed -- audit composers in particular contribute a
		// non-noop observer regardless of whether `/metrics` is mounted.
		// Sourcing from `metrics.events_observer` would cause HTTP byte
		// events to be dropped when `SURREAL_METRICS_ENABLED=false`,
		// while WebSocket bytes (which already read
		// `datastore.observer()`) continue to flow -- producing an
		// asymmetric audit trail.
		let events_observer = Some(Arc::clone(ds.observer()));

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
			.layer(HttpMetricsLayer::new(events_observer))
			.layer(SetSensitiveResponseHeadersLayer::from_shared(headers))
			// Gate user-facing endpoints with 503 until startup completes. Sits
			// ahead of auth so gated requests short-circuit before authentication
			// touches the (still-initialising) datastore.
			.layer(middleware::from_fn_with_state(gate_ready, ready::readiness_gate))
			.layer(auth::SurrealAuthLayer)
			.layer(headers::add_server_header(!opt.no_identification_headers)?)
			.layer(headers::add_version_header(!opt.no_identification_headers)?)
			// Apply CORS headers to relevant responses
			.layer({
				let cors = CorsLayer::new()
					.allow_methods([
						http::Method::GET,
						http::Method::PUT,
						http::Method::POST,
						http::Method::PATCH,
						http::Method::DELETE,
						http::Method::OPTIONS,
					])
					.allow_headers(allow_header)
					.allow_origin(allow_origin)
					.max_age(Duration::from_secs(86400));

				#[cfg(feature = "mcp")]
				let cors = cors.expose_headers(mcp_expose_headers);

				cors
			});

		// Build the route tree from the RouterFactory
		let axum_app = F::configure_router(router_state);

		// Optionally merge the `/metrics` endpoint before applying middleware
		// so it inherits the standard auth/trace layers. The `MetricsState`
		// is exposed via an `Extension` layer applied to the full merged
		// router (other handlers simply ignore it).
		let axum_app = if let Some(ms) = metrics {
			axum_app.merge(crate::observe::router::router()).layer(Extension(ms))
		} else {
			axum_app
		};

		// Apply middleware
		let axum_app = axum_app.layer(service);

		// Create RpcState with persistent HTTP handler. The Prometheus observer
		// (if metrics are enabled) is threaded through so the WebSocket I/O
		// paths can increment per-protocol byte counters without grabbing any
		// global state.
		let rpc_state = Arc::new(RpcState::new_with_options(
			Arc::clone(&ds),
			prometheus_observer,
			opt.durable_session_ttl,
		));

		// Apply state
		let axum_app = axum_app.with_state(Arc::clone(&rpc_state));

		Ok(Self {
			router: axum_app,
			rpc_state,
			datastore: ds,
			notifications,
			canceller: ct,
		})
	}

	/// Consume this [`SurrealRouter`] and return the inner Axum [`Router`].
	///
	/// The returned router is fully configured with all SurrealDB middleware,
	/// authentication, CORS, compression, and state. It can be merged into
	/// another Axum application or served directly.
	pub fn into_router(self) -> Router {
		self.router
	}

	/// Return a reference to the inner Axum [`Router`].
	///
	/// Useful when you need to inspect the router without consuming the
	/// [`SurrealRouter`]. To extract the router for merging, prefer
	/// [`into_router`](SurrealRouter::into_router).
	pub fn router(&self) -> &Router {
		&self.router
	}

	/// Return a reference to the [`RpcState`] that backs this router.
	///
	/// The `RpcState` tracks WebSocket connections and LIVE queries. It is
	/// shared between the router and the notification delivery task.
	pub fn rpc_state(&self) -> &Arc<RpcState> {
		&self.rpc_state
	}

	/// Return a reference to the [`Datastore`] backing this router.
	pub fn datastore(&self) -> &Arc<Datastore> {
		&self.datastore
	}

	/// Return a reference to the [`CancellationToken`] for this router.
	pub fn canceller(&self) -> &CancellationToken {
		&self.canceller
	}

	/// Shut down the datastore, deregistering this node from the cluster.
	///
	/// This calls [`Datastore::shutdown`] on the underlying datastore. You should
	/// call this when your server is stopping, **after** cancelling the
	/// [`CancellationToken`] and waiting for background tasks to finish.
	///
	/// For the CLI server path (`surreal start`) this is handled automatically.
	/// Embedders using [`SurrealRouter::build`] directly must call this (or
	/// call `datastore.shutdown()` themselves).
	pub async fn shutdown(&self) {
		self.datastore.shutdown().await.ok();
	}

	/// Spawn the LIVE query notification delivery task.
	///
	/// This starts a background tokio task that listens for datastore
	/// notifications and forwards them to connected WebSocket clients.
	/// The task runs until the [`CancellationToken`] passed to
	/// [`build`](SurrealRouter::build) is cancelled.
	///
	/// Returns a [`JoinHandle`] that resolves when the notification loop exits.
	/// You can `.await` it during shutdown, or simply drop it if you don't need
	/// to wait.
	///
	/// # When to call this
	///
	/// Call this **after** you have set up your server and are ready to begin
	/// processing requests.
	pub fn spawn_notifications(&self) -> JoinHandle<()> {
		let notify = self.notifications.clone();
		let state = Arc::clone(&self.rpc_state);
		let ct = self.canceller.clone();
		tokio::spawn(async move { notifications(notify, state, ct).await })
	}
}

/// Initialize and start the HTTP server.
///
/// Sets up the Axum HTTP server with middleware, routing, and TLS configuration.
/// This is the all-in-one entrypoint used by the `surreal start` CLI command.
///
/// If you are embedding SurrealDB into your own application and want to merge the
/// SurrealDB routes with your own, use [`SurrealRouter::build`] instead.
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
	recv: Receiver<Notification>,
	ct: CancellationToken,
	router_state: F::RouterState,
) -> Result<()> {
	// No separate startup phase via this entrypoint: serve immediately, with the
	// heartbeat check disabled (no membership refresh task runs here).
	let readiness = Readiness {
		ready: Arc::new(AtomicBool::new(true)),
		max_heartbeat_age: None,
	};
	init_with_metrics::<F>(opt, ds, recv, ct, router_state, None, readiness).await
}

/// Initialize and run the SurrealDB HTTP server with optional Prometheus
/// `/metrics` support.
///
/// When `metrics` is `Some(..)`, the `/metrics` route is mounted on the main
/// HTTP listener with the provided [`MetricsState`] available as an
/// `Extension`. Pass `None` to keep the endpoint disabled.
///
/// See [`init`] for the full startup flow and parameter semantics.
pub async fn init_with_metrics<F: RouterFactory>(
	opt: &Config,
	ds: Arc<Datastore>,
	recv: Receiver<Notification>,
	ct: CancellationToken,
	router_state: F::RouterState,
	metrics: Option<MetricsState>,
	readiness: Readiness,
) -> Result<()> {
	// Build the fully-configured router
	let surreal =
		SurrealRouter::build_with_metrics::<F>(opt, ds, recv, ct, router_state, metrics, readiness)
			.await?;

	// Get a new server handler
	let handle = Handle::new();

	// Setup the graceful shutdown handler
	let shutdown_handler = graceful_shutdown(
		Arc::clone(surreal.rpc_state()),
		surreal.canceller().clone(),
		handle.clone(),
	);

	// Spawn the notification delivery task
	surreal.spawn_notifications();

	// Extract the router for serving
	let axum_app = surreal.into_router();

	// If a certificate and key are specified, then setup TLS
	let res = if let (Some(cert), Some(key)) = (&opt.crt, &opt.key) {
		// Configure certificate and private key used by https
		let tls = RustlsConfig::from_pem_file(cert, key).await?;
		// Disable Nagle's algorithm on the raw TCP stream before the TLS handshake
		let acceptor = RustlsAcceptor::new(tls).acceptor(NoDelayAcceptor::new());
		// Setup the Axum server with TLS
		let server = axum_server::bind(opt.bind).acceptor(acceptor);
		// Log the server startup to the CLI
		info!(target: LOG, "Started web server on {}", &opt.bind);
		// Start the server and listen for connections
		server
			.handle(handle)
			.serve(axum_app.into_make_service_with_connect_info::<SocketAddr>())
			.await
	} else {
		// Setup the Axum server, disabling Nagle's algorithm on accepted connections
		let server = axum_server::bind(opt.bind).acceptor(NoDelayAcceptor::new());
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
