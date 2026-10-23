use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use http::HeaderMap;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::signin::signin;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_types::Object;

use super::AppState;
use super::auth::{
	CookiePolicy, RefreshCookie, cookie_output, require_allowed_origin, token_cookies,
};
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::ntw::error::Error as NetError;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/refresh/cookie", options(|| async {}).post(handler))
}

/// Exchanges the refresh cookie for a rotated pair of cookies, for browser
/// clients. The request needs no body; any body is ignored.
///
/// Contract:
/// - With cookie authentication off the route does not exist (404), and an origin the browser
///   origin gate does not allow is refused (403), both before any work.
/// - The refresh cookie is untrusted input: core verifies its key against the stored grant, rotates
///   it (single use) and issues a new access token and refresh token, which replace both cookies.
/// - Any failure (cookie missing or undecodable, key expired, revoked, reused or wrong) is a 401
///   with no `Set-Cookie`. Rotation is single-use, so concurrent refreshes (two tabs, a reconnect)
///   race and the loser fails; clearing on failure would wipe the winner's fresh cookies. A dead
///   refresh cookie is harmless: it is read only here and on `/signout`, and bounded by its
///   `Max-Age`. Clients must serialise their refreshes.
/// - The session is a fresh anonymous one: neither the access cookie nor an expired bearer token
///   can block the exchange.
async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Extension(policy): Extension<CookiePolicy>,
	headers: HeaderMap,
	accept: Option<TypedHeader<Accept>>,
) -> Result<(HeaderMap, Output), ResponseError> {
	let kvs = &state.datastore;
	// Refreshing is part of the credential exchange, so it is gated with it
	if !kvs.allows_http_route(&RouteTarget::Signin) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Signin);
		return Err(NetError::ForbiddenRoute(RouteTarget::Signin.to_string()).into());
	}
	// The origin gate runs before the cookie is read
	require_allowed_origin(&policy, &headers, "/refresh/cookie")?;
	let Some(cookie) = RefreshCookie::from_headers(&headers) else {
		debug!("Refusing a refresh: no usable refresh cookie");
		return Err(NetError::InvalidAuth.into());
	};
	let mut vars = Object::new();
	vars.insert("ns", cookie.ns.clone());
	vars.insert("db", cookie.db.clone());
	vars.insert("ac", cookie.ac.clone());
	vars.insert("refresh", cookie.rt.clone());
	// A fresh anonymous session, keeping only where the request came from
	let mut session = Session {
		ip: session.ip.clone(),
		or: session.or.clone(),
		..Session::default()
	};
	match signin(kvs, &mut session, vars.into()).await {
		Ok(token) => {
			let out =
				token_cookies(kvs, &token, &session, Some((&cookie.ns, &cookie.db, &cookie.ac)))
					.await?;
			Ok((out, cookie_output(accept.as_deref())?))
		}
		Err(_) => {
			debug!("Refusing a refresh: the refresh token was not accepted");
			Err(NetError::InvalidAuth.into())
		}
	}
}
