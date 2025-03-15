use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::err::Error;
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

async fn handler(
	Extension(state): Extension<AppState>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Version) {
		warn!(
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::Version
		);
		return Err(Error::ForbiddenRoute(RouteTarget::Version.to_string()));
	}

	Ok(format!("{PKG_NAME}-{}", *PKG_VERSION))
}
