use std::{fmt, time::Duration};

use axum::{
	body::{boxed, Body, BoxBody},
	extract::MatchedPath,
	headers::{
		authorization::{Basic, Bearer},
		Authorization, Origin,
	},
	Extension, RequestPartsExt, TypedHeader,
};
use futures_util::future::BoxFuture;
use http::{header, request::Parts, StatusCode};
use hyper::{Request, Response};
use surrealdb::{dbs::Session, iam::verify::token};
use tower_http::{
	auth::AsyncAuthorizeRequest,
	request_id::RequestId,
	trace::{MakeSpan, OnFailure, OnRequest, OnResponse},
};
use tracing::{field, Level, Span};

use crate::{dbs::DB, err::Error, iam::verify::basic};

use super::{client_ip::ExtractClientIP, AppState};

///
/// SurrealAuth is a tower layer that implements the AsyncAuthorizeRequest trait.
/// It is used to authorize requests to SurrealDB using Basic or Token authentication.
///
/// It has to be used in conjunction with the tower_http::auth::RequireAuthorizationLayer layer:
///
/// ```rust
/// use tower_http::auth::RequireAuthorizationLayer;
/// use surrealdb::net::SurrealAuth;
/// use axum::Router;
///
/// let auth = RequireAuthorizationLayer::new(SurrealAuth);
///
/// let app = Router::new()
///   .route("/version", get(|| async { "0.1.0" }))
///   .layer(auth);
/// ```
#[derive(Clone, Copy)]
pub(super) struct SurrealAuth;

impl<B> AsyncAuthorizeRequest<B> for SurrealAuth
where
	B: Send + Sync + 'static,
{
	type RequestBody = B;
	type ResponseBody = BoxBody;
	type Future = BoxFuture<'static, Result<Request<B>, Response<Self::ResponseBody>>>;

	fn authorize(&mut self, request: Request<B>) -> Self::Future {
		Box::pin(async {
			let (mut parts, body) = request.into_parts();
			match check_auth(&mut parts).await {
				Ok(sess) => {
					parts.extensions.insert(sess);
					Ok(Request::from_parts(parts, body))
				}
				Err(err) => {
					let unauthorized_response = Response::builder()
						.status(StatusCode::UNAUTHORIZED)
						.body(boxed(Body::from(err.to_string())))
						.unwrap();
					Err(unauthorized_response)
				}
			}
		})
	}
}

async fn check_auth(parts: &mut Parts) -> Result<Session, Error> {
	let kvs = DB.get().unwrap();

	let or = if let Ok(or) = parts.extract::<TypedHeader<Origin>>().await {
		if !or.is_null() {
			Some(or.to_string())
		} else {
			None
		}
	} else {
		None
	};

	let id = parts.headers.get("id").map(|v| v.to_str().unwrap().to_string()); // TODO: Use a TypedHeader
	let ns = parts.headers.get("ns").map(|v| v.to_str().unwrap().to_string()); // TODO: Use a TypedHeader
	let db = parts.headers.get("db").map(|v| v.to_str().unwrap().to_string()); // TODO: Use a TypedHeader

	let Extension(state) = parts.extract::<Extension<AppState>>().await.map_err(|err| {
		tracing::error!("Error extracting the app state: {:?}", err);
		Error::InvalidAuth
	})?;
	let ExtractClientIP(ip) =
		parts.extract_with_state(&state).await.unwrap_or(ExtractClientIP(None));

	// Create session
	#[rustfmt::skip]
    let mut session = Session { ip, or, id, ns, db, ..Default::default() };

	// If Basic authentication data was supplied
	if let Ok(au) = parts.extract::<TypedHeader<Authorization<Basic>>>().await {
		basic(&mut session, au.username(), au.password()).await
	} else if let Ok(au) = parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
		token(kvs, &mut session, au.token().into()).await.map_err(|e| e.into())
	} else {
		Err(Error::InvalidAuth)
	}?;

	Ok(session)
}

///
/// HttpTraceLayerHooks implements custom hooks for the tower_http::trace::TraceLayer layer.
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
		// The fields follow the OTEL semantic conventions: https://github.com/open-telemetry/opentelemetry-specification/blob/v1.22.0/specification/trace/semantic_conventions/http.md
		let span = tracing::info_span!(
			target: "surreal::http",
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
			client.port	= field::Empty,
			client.socket.address = field::Empty,
			server.address = field::Empty,
			server.port    = field::Empty,
			// set on the response hook
			http.latency.ms = field::Empty,
			http.response.status_code = field::Empty,
			http.response.body.size = field::Empty,
			// set on the failure hook
			error = field::Empty,
			error_message = field::Empty,
		);

		req.uri().query().map(|v| span.record("url.query", v));
		req.uri().scheme().map(|v| span.record("url.scheme", v.as_str()));
		req.uri().host().map(|v| span.record("server.address", v));
		req.uri().port_u16().map(|v| span.record("server.port", v));

		req.headers()
			.get(header::CONTENT_LENGTH)
			.map(|v| v.to_str().map(|v| span.record("http.request.body.size", v)));
		req.headers()
			.get(header::USER_AGENT)
			.map(|v| v.to_str().map(|v| span.record("user_agent.original", v)));

		if let Some(path) = req.extensions().get::<MatchedPath>() {
			span.record("otel.name", format!("{} {}", req.method(), path.as_str()));
			span.record("http.route", path.as_str());
		} else {
			span.record("otel.name", format!("{} -", req.method()));
		};

		if let Some(req_id) = req.extensions().get::<RequestId>() {
			match req_id.header_value().to_str() {
				Err(err) => tracing::error!(error = %err, "failed to parse request id"),
				Ok(request_id) => {
					span.record("http.request.id", request_id);
				}
			}
		}

		if let Some(client_ip) = req.extensions().get::<ExtractClientIP>() {
			if let Some(ref client_ip) = client_ip.0 {
				span.record("client.address", client_ip);
			}
		}

		span
	}
}

impl<B> OnRequest<B> for HttpTraceLayerHooks {
	fn on_request(&mut self, _: &Request<B>, _: &Span) {
		tracing::event!(Level::INFO, "started processing request");
	}
}

impl<B> OnResponse<B> for HttpTraceLayerHooks {
	fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
		if let Some(size) = response.headers().get(header::CONTENT_LENGTH) {
			span.record("http.response.body.size", size.to_str().unwrap());
		}
		span.record("http.response.status_code", response.status().as_u16());

		// Server errors are handled by the OnFailure hook
		if !response.status().is_server_error() {
			span.record("http.latency.ms", latency.as_millis());
			tracing::event!(Level::INFO, "finished processing request");
		}
	}
}

impl<FailureClass> OnFailure<FailureClass> for HttpTraceLayerHooks
where
	FailureClass: fmt::Display,
{
	fn on_failure(&mut self, error: FailureClass, latency: Duration, span: &Span) {
		span.record("error_message", &error.to_string());
		span.record("http.latency.ms", latency.as_millis());
		tracing::event!(Level::ERROR, error = error.to_string(), "response failed");
	}
}
