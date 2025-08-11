use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};

use super::AppState;
use crate::core::dbs::capabilities::RouteTarget;
use crate::net::error::Error as NetError;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/sync", get(save).post(load))
}

async fn load(
	Extension(state): Extension<AppState>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Sync) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Sync);
		return Err(NetError::ForbiddenRoute(RouteTarget::Sync.to_string()));
	}

	Ok("Load")
}

async fn save(
	Extension(state): Extension<AppState>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Sync) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Sync);
		return Err(NetError::ForbiddenRoute(RouteTarget::Sync.to_string()));
	}

	Ok("Save")
}
