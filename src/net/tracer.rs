use std::fmt;
use std::time::Duration;

use axum::extract::MatchedPath;
use http::header;
use hyper::{Request, Response};
use tower_http::request_id::RequestId;
use tower_http::trace::{MakeSpan, OnFailure, OnRequest, OnResponse};
use tracing::{Level, Span, field};

use super::client_ip::ExtractClientIP;

///
/// HttpTraceLayerHooks implements custom hooks for the
/// tower_http::trace::TraceLayer layer.
///
/// Example:
///
/// ```rust
/// use tower_http::trace::TraceLayer;
/// use surrealdb::net::HttpTraceLayerHooks;
/// use axum::Router;
///
/// let trace = TraceLayer::new_for_http().on_request(HttpTraceLayerHooks::default());
///
/// let app = Router::new()
///   .route("/version", get(|| async { "0.1.0" }))
///   .layer(trace);
/// ```
#[derive(Default, Clone)]
pub(crate) struct HttpTraceLayerHooks;

impl<B> MakeSpan<B> for HttpTraceLayerHooks {
	fn make_span(&mut self, req: &Request<B>) -> Span {
		// The fields follow the OTEL semantic conventions: https://github.com/open-telemetry/opentelemetry-specification/blob/v1.23.0/specification/trace/semantic_conventions/http.md
		let span = debug_span!(
			"request",
			otel.name = field::Empty,
			otel.kind = "server",
			http.route = field::Empty,
			http.request.method = req.method().as_str(),
			http.request.body.size = field::Empty,
			url.path = req.uri().path(),
			url.query = field::Empty,
			url.scheme = field::Empty,
			http.request.id = field::Empty,
			user_agent.original = field::Empty,
			network.protocol.name = "http",
			network.protocol.version = format!("{:?}", req.version()).strip_prefix("HTTP/"),
			client.address = field::Empty,
			client.port = field::Empty,
			client.socket.address = field::Empty,
			server.address = field::Empty,
			server.port = field::Empty,
			// set on the response hook
			http.latency.ms = field::Empty,
			http.response.status_code = field::Empty,
			http.response.body.size = field::Empty,
			// set on the failure hook
			error = field::Empty,
			error_message = field::Empty,
		);
		// Record the basic request query information
		req.uri().query().map(|v| span.record("url.query", v));
		req.uri().scheme().map(|v| span.record("url.scheme", v.as_str()));
		req.uri().host().map(|v| span.record("server.address", v));
		req.uri().port_u16().map(|v| span.record("server.port", v));
		// Fetch and record the total input body length
		req.headers()
			.get(header::CONTENT_LENGTH)
			.map(|v| v.to_str().map(|v| span.record("http.request.body.size", v)));
		// Fetch and record the specified user agent
		req.headers()
			.get(header::USER_AGENT)
			.map(|v| v.to_str().map(|v| span.record("user_agent.original", v)));
		// Fetch and record the request method and path
		if let Some(path) = req.extensions().get::<MatchedPath>() {
			span.record("otel.name", format!("{} {}", req.method(), path.as_str()));
			span.record("http.route", path.as_str());
		} else {
			span.record("otel.name", format!("{} -", req.method()));
		};
		// Extract and record the client request id
		if let Some(req_id) = req.extensions().get::<RequestId>() {
			match req_id.header_value().to_str() {
				Err(err) => error!(error = %err, "failed to parse request id"),
				Ok(request_id) => {
					span.record("http.request.id", request_id);
				}
			}
		}
		// Extract and record the client ip address
		if let Some(client_ip) = req.extensions().get::<ExtractClientIP>() {
			if let Some(ref client_ip) = client_ip.0 {
				span.record("client.address", client_ip);
			}
		}
		// Return the span
		span
	}
}

impl<B> OnRequest<B> for HttpTraceLayerHooks {
	fn on_request(&mut self, _: &Request<B>, _: &Span) {
		event!(Level::DEBUG, "Started processing request");
	}
}

impl<B> OnResponse<B> for HttpTraceLayerHooks {
	fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
		// Record the returned response status code
		span.record("http.response.status_code", response.status().as_u16());
		// Record the response body size if specified
		if let Some(size) = response.headers().get(header::CONTENT_LENGTH) {
			span.record("http.response.body.size", size.to_str().unwrap());
		}
		// Server errors are handled by the on_failure hook
		if !response.status().is_server_error() {
			span.record("http.latency.ms", latency.as_millis());
			event!(Level::DEBUG, "Finished processing request");
		}
	}
}

impl<FailureClass> OnFailure<FailureClass> for HttpTraceLayerHooks
where
	FailureClass: fmt::Display,
{
	fn on_failure(&mut self, error: FailureClass, latency: Duration, span: &Span) {
		span.record("error_message", error.to_string());
		span.record("http.latency.ms", latency.as_millis());
		event!(Level::ERROR, error = error.to_string(), "Response failed");
	}
}
