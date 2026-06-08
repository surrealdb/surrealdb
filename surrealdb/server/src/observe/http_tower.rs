//! Tower layer that turns every inbound HTTP request into matching
//! [`HttpRequestStartEvent`] / [`HttpRequestEvent`] dispatches on the
//! installed [`ExecutionObserver`].
//!
//! The layer is the single recording site for HTTP-level observability:
//! community Prometheus, OTLP push, and enterprise dimensional / audit
//! observers all consume the same event by attaching themselves to the
//! datastore's fan-out observer. Network byte counters are emitted as
//! [`NetworkBytesEvent`]s alongside the request event so byte-only
//! consumers (the unlabelled `surrealdb_network_bytes_*_total` counters
//! and tenant-attributed dimensional counters) keep working unchanged.
//!
//! # Bounded route attribute
//!
//! Axum's [`MatchedPath`] hands back the matched route template as an
//! `Arc<str>` whose lifetime is tied to the response. We need a
//! `&'static str` so the value can ride on metric attribute slices
//! without allocating per request. The set of route templates is bounded
//! and compile-time-known by the [`crate::ntw::RouterFactory`], so we
//! intern each new template once on first sighting and reuse the
//! `&'static str` for every subsequent request that matches it. This
//! costs at most one leaked string per declared route.

use std::cell::Cell;
use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::extract::MatchedPath;
use futures::Future;
use http::{Request, Response, StatusCode, Version};
use pin_project_lite::pin_project;
use surrealdb_core::observe::{
	ExecutionObserver, HttpMethod, HttpRequestEvent, HttpRequestEventCtx, HttpRequestEventSafe,
	HttpRequestStartEvent, HttpRequestStartEventSafe, HttpVersion, NetworkBytesEvent,
	NetworkBytesEventSafe, NetworkDirection, Outcome, SessionProtocol,
};
use tower::{Layer, Service};

/// Interner for matched-route templates. The set is bounded by the
/// statically-declared router so the leak is constant-sized for the
/// lifetime of the process.
static ROUTE_INTERNER: LazyLock<Mutex<HashSet<&'static str>>> =
	LazyLock::new(|| Mutex::new(HashSet::new()));

/// Look up a previously-interned route template, or intern a new one
/// and return the `&'static str` reference.
///
/// Hot path: hits a `Mutex<HashSet>` once per request. The router has at
/// most a couple of dozen routes so contention is negligible. We keep a
/// `Mutex` rather than a `RwLock` because the steady-state path is a
/// straight read-then-no-mutate that the std library mutex services in
/// nanoseconds.
fn intern_route(s: &str) -> &'static str {
	let mut set = ROUTE_INTERNER.lock().expect("ROUTE_INTERNER poisoned");
	if let Some(existing) = set.get(s) {
		return existing;
	}
	let leaked: &'static str = Box::leak(s.to_owned().into_boxed_str());
	set.insert(leaked);
	leaked
}

#[derive(Clone, Default)]
pub struct HttpMetricsLayer {
	/// Observer used to dispatch [`HttpRequestEvent`] / [`NetworkBytesEvent`].
	///
	/// Sourced from [`surrealdb_core::kvs::Datastore::observer`] -- the
	/// same handle the WebSocket path reads -- so HTTP events reach
	/// whichever observer the embedder installed (audit composer,
	/// dimensional metrics, OTLP bridge, or `NoopObserver` on community
	/// builds with no audit pipeline). Sourcing from the datastore keeps
	/// HTTP / WS event delivery symmetric and decouples it from
	/// `SURREAL_METRICS_ENABLED`.
	observer: Option<Arc<dyn ExecutionObserver>>,
}

impl HttpMetricsLayer {
	pub fn new(observer: Option<Arc<dyn ExecutionObserver>>) -> Self {
		Self {
			observer,
		}
	}
}

impl<S> Layer<S> for HttpMetricsLayer {
	type Service = HttpMetrics<S>;

	fn layer(&self, inner: S) -> Self::Service {
		HttpMetrics {
			inner,
			observer: self.observer.clone(),
		}
	}
}

#[derive(Clone)]
pub struct HttpMetrics<S> {
	inner: S,
	observer: Option<Arc<dyn ExecutionObserver>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for HttpMetrics<S>
where
	S: Service<Request<ReqBody>, Response = Response<ResBody>>,
	ReqBody: http_body::Body,
	ResBody: http_body::Body,
	S::Error: fmt::Display + 'static,
{
	type Response = Response<ResBody>;
	type Error = S::Error;
	type Future = HttpCallMetricsFuture<S::Future>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
		let tracker = HttpCallMetricTracker::new(&request, self.observer.clone());
		HttpCallMetricsFuture::new(self.inner.call(request), tracker)
	}
}

pin_project! {
	pub struct HttpCallMetricsFuture<F> {
		#[pin]
		inner: F,
		tracker: HttpCallMetricTracker,
	}
}

impl<F> HttpCallMetricsFuture<F> {
	fn new(inner: F, tracker: HttpCallMetricTracker) -> Self {
		Self {
			inner,
			tracker,
		}
	}
}

impl<Fut, ResBody, E> Future for HttpCallMetricsFuture<Fut>
where
	Fut: Future<Output = Result<Response<ResBody>, E>>,
	ResBody: http_body::Body,
	E: std::fmt::Display + 'static,
{
	type Output = Result<Response<ResBody>, E>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();

		// Initialize the metrics if not already done.
		if this.tracker.state.get_mut() == &ResultState::None {
			this.tracker.set_state(ResultState::Started);
			on_request_start(this.tracker);
		}

		let response = futures_util::ready!(this.inner.poll(cx));

		let result = match response {
			Ok(reply) => {
				this.tracker.set_state(ResultState::Result(
					reply.status(),
					reply.version(),
					reply.body().size_hint().exact(),
				));
				// Inner→outer hand-off: the auth service stamps the
				// per-tenant ctx onto the response extensions on
				// successful authentication. We snapshot it here so the
				// tracker can dispatch a populated [`HttpRequestEvent`]
				// from `Drop`. Anonymous and auth-failed responses
				// leave the extension empty and we fall back to the
				// default ctx.
				if let Some(ctx) = reply.extensions().get::<HttpRequestEventCtx>() {
					this.tracker.ctx.set(Some(ctx.clone()));
				}
				Ok(reply)
			}
			Err(e) => {
				this.tracker.set_state(ResultState::Failed);
				Err(e)
			}
		};
		Poll::Ready(result)
	}
}

/// Holds the request-lifetime state needed to construct
/// [`HttpRequestStartEvent`] and [`HttpRequestEvent`] dispatches.
///
/// All fields are populated up-front in [`Self::new`] except for the
/// status / version / response size, which the future fills in when the
/// inner stack returns.
pub struct HttpCallMetricTracker {
	method: HttpMethod,
	version: HttpVersion,
	route: Option<&'static str>,
	state: Cell<ResultState>,
	status_code: Option<StatusCode>,
	request_size: Option<u64>,
	response_size: Option<u64>,
	start: Instant,
	finish: Option<Instant>,
	/// Observer used to dispatch [`HttpRequestEvent`] /
	/// [`NetworkBytesEvent`] on every request finish. Sourced from
	/// [`surrealdb_core::kvs::Datastore::observer`] so the dispatch
	/// keeps working even when `SURREAL_METRICS_ENABLED=false` and no
	/// `/metrics` endpoint is mounted.
	observer: Option<Arc<dyn ExecutionObserver>>,
	/// Per-request tenant ctx, snapshotted from the response extensions
	/// in [`HttpCallMetricsFuture::poll`] when the inner stack returns
	/// successfully. Populated by [`crate::ntw::auth::SurrealAuthService`]
	/// after authentication succeeds; left empty for anonymous and
	/// auth-failed responses, in which case `Drop` falls back to
	/// [`HttpRequestEventCtx::default`].
	ctx: Cell<Option<HttpRequestEventCtx>>,
}

#[derive(PartialEq, Eq)]
pub enum ResultState {
	/// The result was already processed.
	None,
	/// Request was started.
	Started,
	/// The result failed with an error.
	Failed,
	/// The result is an actual HTTP response.
	Result(StatusCode, Version, Option<u64>),
}

impl HttpCallMetricTracker {
	fn new<B>(request: &Request<B>, observer: Option<Arc<dyn ExecutionObserver>>) -> Self
	where
		B: http_body::Body,
	{
		let method = HttpMethod::from_method_str(request.method().as_str());
		let version = http_version_from_axum(request.version());
		let route =
			request.extensions().get::<MatchedPath>().map(|matched| intern_route(matched.as_str()));
		Self {
			method,
			version,
			route,
			state: Cell::new(ResultState::None),
			status_code: None,
			request_size: request.body().size_hint().exact(),
			response_size: None,
			start: Instant::now(),
			finish: None,
			observer,
			ctx: Cell::new(None),
		}
	}

	fn set_state(&self, state: ResultState) {
		self.state.set(state)
	}

	pub fn duration(&self) -> Duration {
		self.finish.unwrap_or(Instant::now()) - self.start
	}

	/// Snapshot the per-request tenant ctx for event dispatch. Always
	/// returns a value (defaulting when the auth layer left the slot
	/// empty), so the caller never has to handle a missing ctx.
	fn snapshot_ctx(&self) -> HttpRequestEventCtx {
		// `Cell<Option<T>>::take` is the only no-clone way out for a
		// non-`Copy` value; the tracker is single-use so taking the
		// inner value is safe.
		self.ctx.take().unwrap_or_default()
	}

	fn outcome(&self) -> Outcome {
		match self.state.replace(ResultState::None) {
			ResultState::Failed => Outcome::Error,
			_ => match self.status_code {
				Some(s) if s.is_server_error() || s.is_client_error() => Outcome::Error,
				_ => Outcome::Success,
			},
		}
	}
}

fn http_version_from_axum(version: Version) -> HttpVersion {
	if version == Version::HTTP_10 {
		HttpVersion::Http10
	} else if version == Version::HTTP_11 {
		HttpVersion::Http11
	} else if version == Version::HTTP_2 {
		HttpVersion::Http2
	} else if version == Version::HTTP_3 {
		HttpVersion::Http3
	} else {
		HttpVersion::Other
	}
}

impl Drop for HttpCallMetricTracker {
	fn drop(&mut self) {
		match self.state.replace(ResultState::None) {
			ResultState::None => {
				// Request was not tracked, so no need to fire a
				// completion event.
				return;
			}
			ResultState::Started => {
				// If the response was never processed, we can't get a
				// valid status code; leave the slot empty and the
				// `outcome()` helper will collapse to Error below.
				self.state.set(ResultState::Failed);
			}
			ResultState::Failed => {
				self.state.set(ResultState::Failed);
			}
			ResultState::Result(s, v, size) => {
				self.status_code = Some(s);
				self.version = http_version_from_axum(v);
				self.response_size = size;
				self.state.set(ResultState::Result(s, v, size));
			}
		}

		self.finish = Some(Instant::now());

		on_request_finish(self);
	}
}

/// Dispatch [`HttpRequestStartEvent`] when the future is first polled.
///
/// Recording sites that pivot on the active-request count
/// (`surrealdb.http.active_requests`, rendered as
/// `surrealdb_http_active_requests` by the Prometheus exporter) get their
/// `+1` here so the gauge reflects requests that are in flight even when
/// the inner stack stalls.
pub fn on_request_start(tracker: &HttpCallMetricTracker) {
	let Some(observer) = tracker.observer.as_ref() else {
		return;
	};
	if observer.is_noop() {
		return;
	}
	let event = HttpRequestStartEvent {
		safe: HttpRequestStartEventSafe {
			method: tracker.method,
			route: tracker.route,
			version: tracker.version,
		},
		// Auth runs inside the inner service so we do not yet have a
		// resolved tenant ctx at start time. The decrement on completion
		// uses the same default-ctx attribute set, so the gauge stays
		// balanced.
		ctx: HttpRequestEventCtx::default(),
	};
	observer.on_http_request_started(&event);
}

/// Dispatch [`HttpRequestEvent`] and the corresponding
/// [`NetworkBytesEvent`] pair when the request finishes.
///
/// Network byte events stay separate from the request event because
/// byte-only consumers (`surrealdb.network.received` /
/// `surrealdb.network.sent`) record on direction alone and do not need
/// the full request context.
pub fn on_request_finish(tracker: &HttpCallMetricTracker) {
	let Some(observer) = tracker.observer.as_ref() else {
		return;
	};
	if observer.is_noop() {
		return;
	}

	let ctx = tracker.snapshot_ctx();
	let outcome = tracker.outcome();
	let status_code = tracker.status_code.map(|s| s.as_u16());
	// Map the HTTP status code into the bounded `error_class` taxonomy
	// so dashboards keying on `surrealdb_http_request_total{error_class=...}`
	// distinguish 401 / 403 / 408 from generic 4xx/5xx without scraping
	// the per-status-code label. `None` for 1xx / 2xx / 3xx responses.
	let error_class =
		status_code.and_then(surrealdb_core::observe::error_class::classify_http_status);
	let event = HttpRequestEvent {
		safe: HttpRequestEventSafe {
			method: tracker.method,
			route: tracker.route,
			status_code,
			version: tracker.version,
			outcome,
			duration: tracker.duration(),
			request_size: tracker.request_size,
			response_size: tracker.response_size,
			error_class,
		},
		ctx: ctx.clone(),
	};
	observer.on_http_request_complete(&event);

	// Network byte events: keep the size signal flowing into the
	// byte-only consumers (community aggregate counters, enterprise
	// dimensional byte counters). HTTP path attribution carries the
	// same tenant ctx as the request event, projected into the
	// narrower `NetworkBytesEventCtx` shape.
	let net_ctx = ctx.to_network_bytes();
	if let Some(size) = tracker.request_size {
		observer.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Received,
				protocol: SessionProtocol::Http,
				bytes: size,
			},
			ctx: net_ctx.clone(),
		});
	}
	if let Some(size) = tracker.response_size {
		observer.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Sent,
				protocol: SessionProtocol::Http,
				bytes: size,
			},
			ctx: net_ctx,
		});
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Mutex;

	use axum::Router;
	use axum::body::{Body, to_bytes};
	use axum::response::Response;
	use axum::routing::get;
	use http::Request;
	use surrealdb_core::observe::{
		AuthEvent, HttpRequestEvent, HttpRequestStartEvent, NetworkBytesEvent, QueryEvent,
		RpcEvent, SessionEvent, StatementEvent, TransactionEvent,
	};
	use tower::ServiceExt;

	use super::*;

	/// Captures every dispatched HTTP / network event for inspection.
	#[derive(Default)]
	struct CapturingObserver {
		http_started: Mutex<Vec<HttpRequestStartEvent>>,
		http_complete: Mutex<Vec<HttpRequestEvent>>,
		net_bytes: Mutex<Vec<NetworkBytesEvent>>,
	}

	impl CapturingObserver {
		fn started(&self) -> Vec<HttpRequestStartEvent> {
			self.http_started.lock().unwrap().clone()
		}

		fn complete(&self) -> Vec<HttpRequestEvent> {
			self.http_complete.lock().unwrap().clone()
		}

		fn bytes(&self) -> Vec<NetworkBytesEvent> {
			self.net_bytes.lock().unwrap().clone()
		}
	}

	impl ExecutionObserver for CapturingObserver {
		fn on_statement_complete(&self, _e: &StatementEvent) {}
		fn on_query_complete(&self, _e: &QueryEvent) {}
		fn on_transaction_complete(&self, _e: &TransactionEvent) {}
		fn on_rpc_complete(&self, _e: &RpcEvent) {}
		fn on_auth_event(&self, _e: &AuthEvent) {}
		fn on_session_event(&self, _e: &SessionEvent) {}

		fn on_network_bytes(&self, event: &NetworkBytesEvent) {
			self.net_bytes.lock().unwrap().push(event.clone());
		}

		fn on_http_request_started(&self, event: &HttpRequestStartEvent) {
			self.http_started.lock().unwrap().push(event.clone());
		}

		fn on_http_request_complete(&self, event: &HttpRequestEvent) {
			self.http_complete.lock().unwrap().push(event.clone());
		}
	}

	#[tokio::test]
	async fn populated_response_extension_reaches_http_event() {
		// Mimic what `SurrealAuthService` does in production: stamp the
		// per-tenant ctx onto the response extensions on the way out.
		// The outer metrics layer must snapshot it from the response
		// during `poll` and emit it from the tracker's `Drop`.
		let observer: Arc<CapturingObserver> = Arc::new(CapturingObserver::default());

		async fn handler() -> Response {
			let ctx = HttpRequestEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("alice".into()),
				..Default::default()
			};
			let mut resp = Response::new(Body::from("ok"));
			resp.extensions_mut().insert(ctx);
			resp
		}

		let layer =
			HttpMetricsLayer::new(Some(Arc::clone(&observer) as Arc<dyn ExecutionObserver>));
		let app: Router = Router::new().route("/", get(handler)).layer(layer);
		let res =
			app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
		// Drain the response body so `Drop` fires on the tracker before
		// we assert against the capture.
		let _ = to_bytes(res.into_body(), 1024).await.unwrap();

		let started = observer.started();
		let complete = observer.complete();
		assert_eq!(started.len(), 1, "expected exactly one start event");
		assert_eq!(complete.len(), 1, "expected exactly one complete event");
		// The ctx is only resolved post-auth, so the start event MUST
		// carry the default (empty) ctx and the complete event MUST
		// carry the populated one.
		assert!(started[0].ctx.namespace.is_none());
		assert_eq!(complete[0].ctx.namespace.as_deref(), Some("acme"));
		assert_eq!(complete[0].ctx.user.as_deref(), Some("alice"));
		// The same ctx should flow into the network bytes events too,
		// projected through `to_network_bytes`.
		let evs = observer.bytes();
		assert!(!evs.is_empty(), "expected at least one network bytes event");
		for e in &evs {
			assert_eq!(e.ctx.namespace.as_deref(), Some("acme"));
			assert_eq!(e.ctx.database.as_deref(), Some("prod"));
			assert_eq!(e.ctx.user.as_deref(), Some("alice"));
		}
	}

	#[tokio::test]
	async fn missing_response_extension_falls_back_to_default_ctx() {
		// Regression guard for unauthenticated / auth-failed requests:
		// the metrics tracker still emits, with `ctx == default()`.
		let observer: Arc<CapturingObserver> = Arc::new(CapturingObserver::default());
		let layer =
			HttpMetricsLayer::new(Some(Arc::clone(&observer) as Arc<dyn ExecutionObserver>));
		let app: Router = Router::new().route("/", get(|| async { "ok" })).layer(layer);
		let res =
			app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
		let _ = to_bytes(res.into_body(), 1024).await.unwrap();

		let complete = observer.complete();
		assert_eq!(complete.len(), 1);
		assert!(complete[0].ctx.namespace.is_none(), "leaked ctx.namespace");
		assert!(complete[0].ctx.user.is_none(), "leaked ctx.user");

		let evs = observer.bytes();
		assert!(!evs.is_empty(), "metrics coverage must not regress for anon requests");
		for e in &evs {
			assert!(e.ctx.namespace.is_none(), "missing extension leaked ctx.namespace");
			assert!(e.ctx.database.is_none(), "missing extension leaked ctx.database");
			assert!(e.ctx.user.is_none(), "missing extension leaked ctx.user");
		}
	}

	#[tokio::test]
	async fn route_is_interned_to_static_str() {
		// Bounded-cardinality guard: two requests against the same
		// route MUST resolve to the same `&'static str` so dimensional
		// observers can use the value as a metric attribute without
		// per-request allocation.
		let observer: Arc<CapturingObserver> = Arc::new(CapturingObserver::default());
		let layer =
			HttpMetricsLayer::new(Some(Arc::clone(&observer) as Arc<dyn ExecutionObserver>));
		let app: Router =
			Router::new().route("/key/{tb}/{id}", get(|| async { "ok" })).layer(layer);
		let app2 = app.clone();
		let r1 = app
			.oneshot(Request::builder().uri("/key/users/1").body(Body::empty()).unwrap())
			.await
			.unwrap();
		let _ = to_bytes(r1.into_body(), 1024).await.unwrap();
		let r2 = app2
			.oneshot(Request::builder().uri("/key/users/2").body(Body::empty()).unwrap())
			.await
			.unwrap();
		let _ = to_bytes(r2.into_body(), 1024).await.unwrap();

		let complete = observer.complete();
		assert_eq!(complete.len(), 2);
		let r1 = complete[0].safe.route.expect("route missing on first request");
		let r2 = complete[1].safe.route.expect("route missing on second request");
		// `&'static str` equality on identical inputs MUST hit the
		// same interned slot, not just be string-equal: pointer
		// equality is a stricter guarantee.
		assert!(std::ptr::eq(r1, r2), "route was not interned across requests");
		assert_eq!(r1, "/key/{tb}/{id}");
	}
}
