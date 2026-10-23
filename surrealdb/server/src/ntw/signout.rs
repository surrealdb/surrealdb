use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use http::header::SET_COOKIE;
use http::{HeaderMap, StatusCode};
use surrealdb_core::iam::access::revoke_refresh_token;
use surrealdb_rpc::capabilities::RouteTarget;

use super::AppState;
use super::auth::{
	CookiePolicy, RefreshCookie, clear_refresh_cookie, clear_session_cookie, require_allowed_origin,
};
use super::error::ResponseError;
use crate::ntw::error::Error as NetError;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/signout", options(|| async {}).post(handler))
}

/// Ends a cookie session by clearing the session and refresh cookies.
///
/// The auth layer does not read the cookies on this route, so a cookie that no
/// longer verifies cannot block its own removal. Only an origin the browser
/// origin gate allows may call it, so a cross-site page cannot sign the user
/// out. With cookie authentication off the route does not exist.
///
/// A refresh cookie that decodes has its grant revoked before the response, but
/// only after core has verified the key against the stored grant: the cookie
/// is client-controlled, so a grant id alone never authorises a revocation. A
/// failed revocation (wrong, expired or already revoked key) is not an error to
/// the client; the cookies are cleared either way. Verifying a key is a
/// credential check, so the route spends the sign-in rate limit budget.
async fn handler(
	Extension(state): Extension<AppState>,
	Extension(policy): Extension<CookiePolicy>,
	headers: HeaderMap,
) -> Result<impl IntoResponse, ResponseError> {
	// Signing out is part of the credential exchange, so it is gated with it
	if !state.datastore.allows_http_route(&RouteTarget::Signin) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Signin);
		return Err(NetError::ForbiddenRoute(RouteTarget::Signin.to_string()).into());
	}
	require_allowed_origin(&policy, &headers, "/signout")?;
	if let Some(cookie) = RefreshCookie::from_headers(&headers)
		&& revoke_refresh_token(&state.datastore, &cookie.ns, &cookie.db, &cookie.ac, &cookie.rt)
			.await
			.is_err()
	{
		debug!("Signout did not revoke a refresh token: the cookie's key was not accepted");
	}
	let mut out = HeaderMap::new();
	out.append(SET_COOKIE, clear_session_cookie());
	out.append(SET_COOKIE, clear_refresh_cookie());
	Ok((out, StatusCode::OK))
}
