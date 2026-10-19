use std::time::Duration;

use axum::routing::get;
use axum::{Extension, Router};
use surrealdb_rpc::capabilities::RouteTarget;

use super::AppState;
use crate::ntw::error::Error as NetError;

/// The `/health` route: the Kubernetes liveness signal.
///
/// Returns `200` while the instance can serve. Fails when either:
///
/// - the memory guard has been engaged without a break for at least
///   `SURREAL_MEMORY_THRESHOLD_UNHEALTHY_AFTER` (`503`; see [`crate::ntw::memory_guard`]). While
///   engaged the guard refuses every RPC request, and memory held by caches is not released, so
///   only a restart recovers the instance.
/// - the storage backend fails its health check (`500`).
///
/// Contrast with `/status` (process/listener liveness only) and `/ready`
/// (startup and cluster heartbeat).
pub fn router<S>() -> Router<S>
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

	memory_guard_health(
		crate::ntw::memory_guard::probe(),
		*crate::cnf::MEMORY_THRESHOLD_UNHEALTHY_AFTER,
	)?;

	db.health_check().await.map_err(|err| {
		tracing::error!("Health check failed: {err}");
		NetError::InvalidStorage
	})
}

/// The memory half of the `/health` decision.
///
/// `engaged_for` is how long the memory guard has been engaged (`None` when it
/// is not), and `unhealthy_after` the configured limit (`None` when the check
/// is disabled). Fails with [`NetError::MemoryGuardEngaged`] once the guard has
/// been engaged for at least the limit.
fn memory_guard_health(
	engaged_for: Option<Duration>,
	unhealthy_after: Option<Duration>,
) -> Result<(), NetError> {
	match (engaged_for, unhealthy_after) {
		(Some(engaged), Some(limit)) if engaged >= limit => {
			tracing::error!(
				engaged_secs = engaged.as_secs(),
				limit_secs = limit.as_secs(),
				"Health check failed: tracked memory has been beyond SURREAL_MEMORY_THRESHOLD \
				 for longer than SURREAL_MEMORY_THRESHOLD_UNHEALTHY_AFTER"
			);
			Err(NetError::MemoryGuardEngaged(engaged.as_secs()))
		}
		_ => Ok(()),
	}
}

#[cfg(test)]
mod tests {
	use axum::response::IntoResponse;
	use http::StatusCode;

	use super::*;

	fn secs(n: u64) -> Duration {
		Duration::from_secs(n)
	}

	#[test]
	fn healthy_while_the_guard_is_disengaged() {
		assert!(memory_guard_health(None, Some(secs(60))).is_ok());
	}

	#[test]
	fn healthy_while_engaged_for_less_than_the_limit() {
		assert!(memory_guard_health(Some(secs(59)), Some(secs(60))).is_ok());
	}

	#[test]
	fn unhealthy_with_503_once_engaged_for_the_limit() {
		let err = memory_guard_health(Some(secs(60)), Some(secs(60))).unwrap_err();
		assert!(matches!(err, NetError::MemoryGuardEngaged(60)));
		assert_eq!(err.into_response().status(), StatusCode::SERVICE_UNAVAILABLE);
	}

	#[test]
	fn a_disabled_check_never_fails() {
		assert!(memory_guard_health(Some(secs(86_400)), None).is_ok());
	}

	/// End to end through the latch `/health` reads: an engagement shorter
	/// than the limit passes, one that reaches it fails, and one observation
	/// below the threshold clears it.
	#[test]
	fn the_latch_drives_the_verdict() {
		use web_time::Instant;

		use crate::ntw::memory_guard::Latch;

		let origin = Instant::now();
		let latch = Latch::new(origin);
		let limit = Some(secs(60));
		latch.observe(true, origin);
		assert!(memory_guard_health(latch.engaged_for(origin + secs(30)), limit).is_ok());
		assert!(memory_guard_health(latch.engaged_for(origin + secs(61)), limit).is_err());
		latch.observe(false, origin + secs(62));
		assert!(memory_guard_health(latch.engaged_for(origin + secs(63)), limit).is_ok());
	}
}
