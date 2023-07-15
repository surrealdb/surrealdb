use axum::extract::MatchedPath;
use opentelemetry::{metrics::MetricsError, Context as TelemetryContext, KeyValue};
use pin_project_lite::pin_project;
use std::{
	cell::Cell,
	fmt,
	pin::Pin,
	task::{Context, Poll},
	time::{Duration, Instant},
};

use futures::Future;
use http::{Request, Response, StatusCode, Version};
use tower::{Layer, Service};

use super::{HTTP_METER, HTTP_SERVER_ACTIVE_REQUESTS, HTTP_SERVER_DURATION};

#[derive(Clone, Default)]
pub struct HttpMetricsLayer;

impl HttpMetricsLayer {
	pub fn new() -> Self {
		Self::default()
	}
}

impl<S> Layer<S> for HttpMetricsLayer {
	type Service = HttpMetrics<S>;

	fn layer(&self, inner: S) -> Self::Service {
		HttpMetrics {
			inner,
		}
	}
}

#[derive(Clone)]
pub struct HttpMetrics<S> {
	inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for HttpMetrics<S>
where
	S: Service<Request<ReqBody>, Response = Response<ResBody>>,
	S::Error: fmt::Display + 'static,
{
	type Response = Response<ResBody>;
	type Error = S::Error;
	type Future = HttpCallMetricsFuture<S::Future>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
		let tracker = HttpCallMetricTracker::new(&request);

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
	E: std::fmt::Display + 'static,
{
	type Output = Result<Response<ResBody>, E>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();

		this.tracker.set_state(ResultState::Started);

		if let Err(err) = on_request_start(this.tracker) {
			error!("Failed to setup metrics when request started: {}", err);
			// Consider this request not tracked: reset the state to None, so that the drop handler does not decrease the counter.
			this.tracker.set_state(ResultState::None);
		};

		let response = futures_util::ready!(this.inner.poll(cx));

		let result = match response {
			Ok(reply) => {
				this.tracker.set_state(ResultState::Result(reply.status(), reply.version()));
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

pub struct HttpCallMetricTracker {
	version: String,
	method: hyper::Method,
	scheme: Option<http::uri::Scheme>,
	host: Option<String>,
	route: Option<String>,
	state: Cell<ResultState>,
	status_code: Option<StatusCode>,
	start: Instant,
	finish: Option<Instant>,
}

pub enum ResultState {
	/// The result was already processed.
	None,
	/// Request was started.
	Started,
	/// The result failed with an error.
	Failed,
	/// The result is an actual HTTP response.
	Result(StatusCode, Version),
}

impl HttpCallMetricTracker {
	fn new<B>(request: &Request<B>) -> Self {
		Self {
			version: format!("{:?}", request.version()),
			method: request.method().clone(),
			scheme: request.uri().scheme().cloned(),
			host: request.uri().host().map(|s| s.to_string()),
			route: request.extensions().get::<MatchedPath>().map(|v| v.as_str().to_string()),
			state: Cell::new(ResultState::None),
			status_code: None,
			start: Instant::now(),
			finish: None,
		}
	}

	fn set_state(&self, state: ResultState) {
		self.state.set(state)
	}

	pub fn duration(&self) -> Duration {
		self.finish.unwrap_or(Instant::now()) - self.start
	}

	// Follows the OpenTelemetry semantic conventions for HTTP metrics define here: https://github.com/open-telemetry/semantic-conventions/blob/main/specification/http/http-metrics.md
	fn olel_common_attrs(&self) -> Vec<KeyValue> {
		let mut res = vec![
			KeyValue::new("http.request.method", self.method.as_str().to_owned()),
			KeyValue::new("network.protocol.name", "http".to_owned()),
		];

		if let Some(scheme) = &self.scheme {
			res.push(KeyValue::new("url.scheme", scheme.as_str().to_owned()));
		}

		if let Some(host) = &self.host {
			res.push(KeyValue::new("server.address", host.to_owned()));
		}

		res
	}

	pub(super) fn active_req_attrs(&self) -> Vec<KeyValue> {
		self.olel_common_attrs()
	}

	pub(super) fn request_duration_attrs(&self) -> Vec<KeyValue> {
		let mut res = self.olel_common_attrs();

		res.push(KeyValue::new(
			"http.response.status_code",
			self.status_code.map(|v| v.as_str().to_owned()).unwrap_or("000".to_owned()),
		));

		if let Some(v) = self.version.strip_prefix("HTTP/") {
			res.push(KeyValue::new("network.protocol.version", v.to_owned()));
		}

		if let Some(target) = &self.route {
			res.push(KeyValue::new("http.route", target.to_owned()));
		}

		res
	}
}

impl Drop for HttpCallMetricTracker {
	fn drop(&mut self) {
		match self.state.replace(ResultState::None) {
			ResultState::None => {
				// Request was not tracked, so no need to decrease the counter.
				return;
			}
			ResultState::Started => {
				// If the response was never processed, we can't get a valid status code
			}
			ResultState::Failed => {
				// If there's an error processing the request and we don't have a response, we can't get a valid status code
			}
			ResultState::Result(s, v) => {
				self.status_code = Some(s);
				self.version = format!("{:?}", v);
			}
		};

		self.finish = Some(Instant::now());

		if let Err(err) = on_request_finish(self) {
			error!(target: "surrealdb::telemetry", "Failed to setup metrics when request finished: {}", err);
		}
	}
}

pub fn on_request_start(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	// Setup the active_requests observer
	observe_active_request_start(tracker)
}

pub fn on_request_finish(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	// Setup the active_requests observer
	observe_active_request_finish(tracker)?;

	// Record the duration of the request.
	record_request_duration(tracker);

	Ok(())
}

fn observe_active_request_start(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();
	// Setup the callback to observe the active requests.
	HTTP_METER.register_callback(move |ctx| HTTP_SERVER_ACTIVE_REQUESTS.observe(ctx, 1, &attrs))
}

fn observe_active_request_finish(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();
	// Setup the callback to observe the active requests.
	HTTP_METER.register_callback(move |ctx| HTTP_SERVER_ACTIVE_REQUESTS.observe(ctx, -1, &attrs))
}

fn record_request_duration(tracker: &HttpCallMetricTracker) {
	// Record the duration of the request.
	HTTP_SERVER_DURATION.record(
		&TelemetryContext::current(),
		tracker.duration().as_millis() as u64,
		&tracker.request_duration_attrs(),
	);
}
