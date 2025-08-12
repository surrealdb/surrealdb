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
use tower_http::auth::AsyncAuthorizeRequest;
use uuid::Uuid;

use super::AppState;
use super::client_ip::ExtractClientIP;
use super::headers::{
	SurrealAuthDatabase, SurrealAuthNamespace, SurrealDatabase, SurrealId, SurrealNamespace,
	parse_typed_header,
};
use crate::core::dbs::Session;
use crate::core::iam::verify::{basic, token};
use crate::net::error::Error as NetError;

///
/// SurrealAuth is a tower layer that implements the AsyncAuthorizeRequest
/// trait. It is used to authorize requests to SurrealDB using Basic or Token
/// authentication.
///
/// It has to be used in conjunction with the
/// tower_http::auth::RequireAuthorizationLayer layer:
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

impl AsyncAuthorizeRequest<Body> for SurrealAuth {
	type RequestBody = Body;
	type ResponseBody = Body;
	type Future = BoxFuture<'static, Result<Request<Body>, Response<Self::ResponseBody>>>;

	fn authorize(&mut self, request: Request<Body>) -> Self::Future {
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
						.body(Body::new(err.to_string()))
						.unwrap();
					Err(unauthorized_response)
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
				Ok(id) => Some(id.to_string()),
				// The specified request id was not a valid UUID.
				Err(_) => bail!(NetError::Request),
			}
		}
		// No request id was specified, create a new id.
		None => Some(Uuid::new_v4().to_string()),
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
