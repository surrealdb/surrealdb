use axum::routing::get;
use axum::{Extension, Router};

use super::AppState;
use crate::core::dbs::capabilities::RouteTarget;
use crate::net::error::Error as NetError;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/health", get(handler))
}

async fn handler(Extension(state): Extension<AppState>) -> Result<(), NetError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Health) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Health);
		return Err(NetError::ForbiddenRoute(RouteTarget::Health.to_string()));
	}

	db.health_check().await.map_err(|err| {
		tracing::error!("Health check failed: {err}");
		NetError::InvalidStorage
	})
}
