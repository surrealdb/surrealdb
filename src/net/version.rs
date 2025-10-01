use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};

use super::AppState;
use crate::cnf::{PKG_NAME, PKG_VERSION};
use crate::core::dbs::capabilities::RouteTarget;
use crate::net::error::Error as NetError;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/version", get(handler))
}

async fn handler(Extension(state): Extension<AppState>) -> Result<impl IntoResponse, NetError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Version) {
		warn!(
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::Version
		);
		return Err(NetError::ForbiddenRoute(RouteTarget::Version.to_string()));
	}

	Ok(format!("{PKG_NAME}-{}", *PKG_VERSION))
}
