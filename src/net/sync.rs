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
	Router::new().route("/sync", get(save).post(load))
}

async fn load(
	Extension(state): Extension<AppState>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::SyncLoad) {
		return Err(Error::OperationForbidden);
	}

	Ok("Load")
}

async fn save(
	Extension(state): Extension<AppState>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::SyncSave) {
		return Err(Error::OperationForbidden);
	}

	Ok("Save")
}
