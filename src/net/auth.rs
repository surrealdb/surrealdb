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
		basic(kvs, &mut session, au.username(), au.password()).await?;
	};

	// If Token authentication data was supplied
	if let Ok(au) = parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
		token(kvs, &mut session, au.token()).await?;
	};

	Ok(session)
}
