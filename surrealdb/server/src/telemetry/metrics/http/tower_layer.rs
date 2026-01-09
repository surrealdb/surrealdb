use std::cell::Cell;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::extract::MatchedPath;
use futures::Future;
use http::{Request, Response, StatusCode, Version};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MetricsError;
use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::cnf::TELEMETRY_NAMESPACE;

#[derive(Clone, Default)]
pub struct HttpMetricsLayer;

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
	ResBody: http_body::Body,
	E: std::fmt::Display + 'static,
{
	type Output = Result<Response<ResBody>, E>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();

		// Initialize the metrics if not already done.
		if this.tracker.state.get_mut() == &ResultState::None {
			this.tracker.set_state(ResultState::Started);

			if let Err(err) = on_request_start(this.tracker) {
				error!("Failed to setup metrics when request started: {}", err);
				// Consider this request not tracked: reset the state to None, so that the drop
				// handler does not decrease the counter.
				this.tracker.set_state(ResultState::None);
			};
		}

		let response = futures_util::ready!(this.inner.poll(cx));

		let result = match response {
			Ok(reply) => {
				this.tracker.set_state(ResultState::Result(
					reply.status(),
					reply.version(),
					reply.body().size_hint().exact(),
				));
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
	request_size: Option<u64>,
	response_size: Option<u64>,
	start: Instant,
	finish: Option<Instant>,
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
	fn new<B>(request: &Request<B>) -> Self
	where
		B: http_body::Body,
	{
		Self {
			version: format!("{:?}", request.version()),
			method: request.method().clone(),
			scheme: request.uri().scheme().cloned(),
			host: request.uri().host().map(|s| s.to_string()),
			route: request.extensions().get::<MatchedPath>().map(|v| v.as_str().to_string()),
			state: Cell::new(ResultState::None),
			status_code: None,
			request_size: request.body().size_hint().exact(),
			response_size: None,
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

	// Follows the OpenTelemetry semantic conventions for HTTP metrics define here: https://github.com/open-telemetry/opentelemetry-specification/blob/v1.23.0/specification/metrics/semantic_conventions/http-metrics.md
	fn otel_common_attrs(&self) -> Vec<KeyValue> {
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

		if let Some(namespace) = TELEMETRY_NAMESPACE.clone() {
			res.push(KeyValue::new("namespace", namespace.trim().to_owned()));
		};

		res
	}

	pub(super) fn active_req_attrs(&self) -> Vec<KeyValue> {
		self.otel_common_attrs()
	}

	pub(super) fn request_duration_attrs(&self) -> Vec<KeyValue> {
		let mut res = self.otel_common_attrs();

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

	pub(super) fn request_size_attrs(&self) -> Vec<KeyValue> {
		self.request_duration_attrs()
	}

	pub(super) fn response_size_attrs(&self) -> Vec<KeyValue> {
		self.request_duration_attrs()
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
				// If the response was never processed, we can't get a valid
				// status code
			}
			ResultState::Failed => {
				// If there's an error processing the request and we don't have
				// a response, we can't get a valid status code
			}
			ResultState::Result(s, v, size) => {
				self.status_code = Some(s);
				self.version = format!("{v:?}");
				self.response_size = size;
			}
		};

		self.finish = Some(Instant::now());

		if let Err(err) = on_request_finish(self) {
			error!(target: "surrealdb::telemetry", "Failed to setup metrics when request finished: {}", err);
		}
	}
}

pub fn on_request_start(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	// Increase the number of active requests.
	super::observe_active_request(1, tracker)
}

pub fn on_request_finish(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	// Decrease the number of active requests.
	super::observe_active_request(-1, tracker)?;

	// Record the duration of the request.
	super::record_request_duration(tracker);

	// Increment the request counter
	super::record_request_count(tracker);

	// Record the request size if known
	if let Some(size) = tracker.request_size {
		super::record_request_size(tracker, size)
	}

	// Record the response size if known
	if let Some(size) = tracker.response_size {
		super::record_response_size(tracker, size)
	}

	Ok(())
}
