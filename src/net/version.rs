use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::net::error::Error as NetError;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use surrealdb::dbs::capabilities::RouteTarget;

use super::AppState;

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
