use axum::{
	body::{boxed, Body, BoxBody},
	headers::{
		authorization::{Basic, Bearer},
		Authorization, Origin,
	},
	Extension, RequestPartsExt, TypedHeader,
};
use futures_util::future::BoxFuture;
use http::{request::Parts, StatusCode};
use hyper::{Request, Response};
use surrealdb::{
	dbs::Session,
	iam::verify::{basic, token},
};
use tower_http::auth::AsyncAuthorizeRequest;

use crate::{dbs::DB, err::Error};

use super::{
	client_ip::ExtractClientIP,
	headers::{
		parse_typed_header, SurrealAuthDatabase, SurrealAuthNamespace, SurrealDatabase,
		SurrealDatabaseLegacy, SurrealId, SurrealIdLegacy, SurrealNamespace,
		SurrealNamespaceLegacy,
	},
	AppState,
};

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

	// Extract the session id from the headers. If not found, fallback to the legacy header name.
	let id = match parse_typed_header::<SurrealId>(parts.extract::<TypedHeader<SurrealId>>().await)
	{
		Ok(None) => parse_typed_header::<SurrealIdLegacy>(
			parts.extract::<TypedHeader<SurrealIdLegacy>>().await,
		),
		res => res,
	}?;

	// Extract the namespace from the headers. If not found, fallback to the legacy header name.
	let ns = match parse_typed_header::<SurrealNamespace>(
		parts.extract::<TypedHeader<SurrealNamespace>>().await,
	) {
		Ok(None) => parse_typed_header::<SurrealNamespaceLegacy>(
			parts.extract::<TypedHeader<SurrealNamespaceLegacy>>().await,
		),
		res => res,
	}?;

	// Extract the database from the headers. If not found, fallback to the legacy header name.
	let db = match parse_typed_header::<SurrealDatabase>(
		parts.extract::<TypedHeader<SurrealDatabase>>().await,
	) {
		Ok(None) => parse_typed_header::<SurrealDatabaseLegacy>(
			parts.extract::<TypedHeader<SurrealDatabaseLegacy>>().await,
		),
		res => res,
	}?;

	// Extract the authentication namespace and database from the headers.
	let auth_ns = parse_typed_header::<SurrealAuthNamespace>(
		parts.extract::<TypedHeader<SurrealAuthNamespace>>().await,
	)?;
	let auth_db = parse_typed_header::<SurrealAuthDatabase>(
		parts.extract::<TypedHeader<SurrealAuthDatabase>>().await,
	)?;

	let Extension(state) = parts.extract::<Extension<AppState>>().await.map_err(|err| {
		tracing::error!("Error extracting the app state: {:?}", err);
		Error::InvalidAuth
	})?;

	let ExtractClientIP(ip) =
		parts.extract_with_state(&state).await.unwrap_or(ExtractClientIP(None));

	// Create session
	let mut session = Session::default();
	session.ip = ip;
	session.or = or;
	session.id = id;
	session.ns = ns;
	session.db = db;

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
