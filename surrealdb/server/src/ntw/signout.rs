use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use http::header::SET_COOKIE;
use http::{HeaderMap, StatusCode};
use surrealdb_rpc::capabilities::RouteTarget;

use super::AppState;
use super::auth::{CookiePolicy, clear_session_cookie, require_allowed_origin};
use super::error::ResponseError;
use crate::ntw::error::Error as NetError;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/signout", options(|| async {}).post(handler))
}

/// Ends a cookie session by clearing the session cookie.
///
/// The auth layer does not read the cookie on this route, so a cookie that no
/// longer verifies cannot block its own removal. Only an origin the browser
/// origin gate allows may call it, so a cross-site page cannot sign the user
/// out. With cookie authentication off the route does not exist.
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
	Ok(([(SET_COOKIE, clear_session_cookie())], StatusCode::OK))
}
