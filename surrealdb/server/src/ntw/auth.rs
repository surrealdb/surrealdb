use std::task::{Context, Poll};

use anyhow::{Result, bail};
use axum::body::Body;
use axum::{Extension, RequestPartsExt};
use axum_extra::TypedHeader;
use axum_extra::headers::authorization::{Basic, Bearer};
use axum_extra::headers::{Authorization, Origin};
use futures_util::future::BoxFuture;
use http::StatusCode;
use http::request::Parts;
use hyper::{Request, Response};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::verify::{basic, token};
use surrealdb_core::observe::HttpRequestEventCtx;
use tower::{Layer, Service};
use uuid::Uuid;

use super::AppState;
use super::client_ip::ExtractClientIP;
use super::headers::{
	SurrealAuthDatabase, SurrealAuthNamespace, SurrealDatabase, SurrealId, SurrealNamespace,
	parse_typed_header,
};
use crate::ntw::error::Error as NetError;

/// Tower layer applying [`SurrealAuthService`] to a wrapped service.
///
/// Mounted between the outer HTTP metrics layer and the route handlers in
/// [`crate::ntw::SurrealRouter`]. Replaces `tower_http`'s
/// `AsyncRequireAuthorizationLayer<SurrealAuth>` so the service can both
/// authenticate the request and stamp the resulting [`HttpRequestEventCtx`]
/// onto the response before it propagates back through the metrics layer.
#[derive(Clone, Copy)]
pub(super) struct SurrealAuthLayer;

impl<S> Layer<S> for SurrealAuthLayer {
	type Service = SurrealAuthService<S>;

	fn layer(&self, inner: S) -> Self::Service {
		SurrealAuthService {
			inner,
		}
	}
}

/// Authentication middleware running [`check_auth`] on every request and
/// short-circuiting failures with a 401 response.
///
/// On success, the authenticated [`Session`] is inserted into the request
/// extensions for downstream handlers and the derived
/// [`HttpRequestEventCtx`] is inserted into the response extensions on the
/// way out so the outer HTTP metrics tower layer can attribute the
/// [`HttpRequestEvent`] and any [`NetworkBytesEvent`]s per
/// `(namespace, database, user, session_id, client_ip)`. Auth-failed and
/// anonymous requests carry no ctx; the metrics layer falls back to
/// [`HttpRequestEventCtx::default`] in that case.
#[derive(Clone)]
pub(super) struct SurrealAuthService<S> {
	inner: S,
}

impl<S> Service<Request<Body>> for SurrealAuthService<S>
where
	S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Response = Response<Body>;
	type Error = S::Error;
	type Future = BoxFuture<'static, Result<Response<Body>, S::Error>>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, request: Request<Body>) -> Self::Future {
		// Standard tower idiom: the service captured by the future must be
		// the one that has been `poll_ready`'d, not the placeholder we
		// leave behind in `self`.
		let clone = self.inner.clone();
		let mut inner = std::mem::replace(&mut self.inner, clone);
		Box::pin(async move {
			let (mut parts, body) = request.into_parts();
			match check_auth(&mut parts).await {
				Ok(sess) => {
					// Build the per-tenant ctx from the authenticated
					// session before we hand the request down. We stash
					// it on the response on the way out so the outer
					// metrics layer can attribute the per-request
					// telemetry (HttpRequestEvent + NetworkBytesEvent)
					// without reaching back into request state.
					let ctx = HttpRequestEventCtx::from_session(&sess);
					parts.extensions.insert(sess);
					let mut response = inner.call(Request::from_parts(parts, body)).await?;
					response.extensions_mut().insert(ctx);
					Ok(response)
				}
				Err(err) => {
					let unauthorized = Response::builder()
						.status(StatusCode::UNAUTHORIZED)
						.body(Body::new(err.to_string()))
						.unwrap_or_else(|_| {
							let mut resp = Response::new(Body::empty());
							*resp.status_mut() = StatusCode::UNAUTHORIZED;
							resp
						});
					Ok(unauthorized)
				}
			}
		})
	}
}

async fn check_auth(parts: &mut Parts) -> Result<Session> {
	let or = match parts.extract::<TypedHeader<Origin>>().await {
		Ok(or) => {
			if !or.is_null() {
				Some(or.to_string())
			} else {
				None
			}
		}
		_ => None,
	};

	// Extract the session id from the headers or generate a new one.
	let id = match parse_typed_header::<SurrealId>(parts.extract::<TypedHeader<SurrealId>>().await)?
	{
		Some(id) => {
			// Attempt to parse the request id as a UUID.
			match Uuid::try_parse(&id) {
				// The specified request id was a valid UUID.
				Ok(id) => Some(id),
				// The specified request id was not a valid UUID.
				Err(_) => bail!(NetError::Request),
			}
		}
		// No request id was specified, create a new id.
		None => Some(Uuid::new_v4()),
	};

	// Extract the namespace from the headers.
	let ns = parse_typed_header::<SurrealNamespace>(
		parts.extract::<TypedHeader<SurrealNamespace>>().await,
	)?;

	// Extract the database from the headers.
	let db = parse_typed_header::<SurrealDatabase>(
		parts.extract::<TypedHeader<SurrealDatabase>>().await,
	)?;

	// Extract the authentication namespace and database from the headers.
	let auth_ns = parse_typed_header::<SurrealAuthNamespace>(
		parts.extract::<TypedHeader<SurrealAuthNamespace>>().await,
	)?;
	let auth_db = parse_typed_header::<SurrealAuthDatabase>(
		parts.extract::<TypedHeader<SurrealAuthDatabase>>().await,
	)?;

	let Extension(state) = parts.extract::<Extension<AppState>>().await.map_err(|err| {
		tracing::error!("Error extracting the app state: {:?}", err);
		NetError::InvalidAuth
	})?;

	let kvs = &state.datastore;

	let ExtractClientIP(ip) =
		parts.extract_with_state(&state).await.unwrap_or(ExtractClientIP(None));

	// Create session
	let mut session = Session {
		ip,
		or,
		id,
		ns,
		db,
		..Session::default()
	};

	// If Basic authentication data was supplied
	if let Ok(au) = parts.extract::<TypedHeader<Authorization<Basic>>>().await {
		basic(
			kvs,
			&mut session,
			au.username(),
			au.password(),
			auth_ns.as_deref(),
			auth_db.as_deref(),
		)
		.await?;
	};

	// If Token authentication data was supplied
	if let Ok(au) = parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
		token(kvs, &mut session, au.token()).await?;
	};

	Ok(session)
}
