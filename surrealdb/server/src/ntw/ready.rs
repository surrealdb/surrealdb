use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use http::HeaderValue;
use http::header::RETRY_AFTER;

use super::AppState;
use crate::ntw::error::Error as NetError;

/// The `/ready` route: the Kubernetes startup/readiness signal.
///
/// Returns `200` only once the instance has finished starting up (datastore
/// initialisation, import and credentials) **and**, when a heartbeat budget is
/// configured, the current node's cluster heartbeat is fresh. Returns `503`
/// while still starting up or when the heartbeat is stale, and `500` if the
/// heartbeat can't be read.
/// Contrast with `/status` (process/listener liveness only) and `/health`
/// (backend reachability only).
pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/ready", get(ready_handler))
}

async fn ready_handler(Extension(state): Extension<AppState>) -> Result<(), NetError> {
	community_readiness(&state).await
}

/// The community readiness decision behind the `/ready` route, exposed so an
/// edition that overrides `/ready` can reuse it for the community half of its
/// own check.
///
/// Returns `Ok(())` once the deferred startup work has completed and, when a
/// heartbeat budget is configured, the current node's cluster heartbeat is
/// fresh. Returns `Err(NotReady)` while still starting up, when this node has
/// no cluster membership row, or when the heartbeat is stale, and
/// `Err(InvalidStorage)` if the heartbeat cannot be read.
pub async fn community_readiness(state: &AppState) -> Result<(), NetError> {
	// Not ready until the deferred startup work has completed.
	if !state.readiness.ready.load(Ordering::SeqCst) {
		return Err(NetError::NotReady);
	}
	// When a budget is configured, confirm this node's cluster heartbeat is
	// fresh. The node-membership refresh task rewrites it every
	// `node_membership_refresh_interval`, so a recent heartbeat proves the
	// storage read and write paths are working without this probe doing any
	// duplicate work. Disabled (`None`) on the embedder path, which does not run
	// that task.
	if let Some(max_age) = state.readiness.max_heartbeat_age {
		let age = state.datastore.node_heartbeat_age().await.map_err(|err| {
			tracing::error!("Readiness check could not read the node heartbeat: {err}");
			NetError::InvalidStorage
		})?;
		// A node with no cluster row has either not registered yet or has been
		// archived and reaped by a peer. Both mean this node is not currently a
		// serving member — a 503 that the next registration clears — rather than
		// the storage failure a 500 would report.
		let Some(age) = age else {
			tracing::warn!("Node is not registered in the cluster; reporting not ready");
			return Err(NetError::NotReady);
		};
		if age > max_age {
			tracing::warn!("Node heartbeat is stale ({age:?} > {max_age:?}); reporting not ready");
			return Err(NetError::NotReady);
		}
	}
	Ok(())
}

/// Paths that stay reachable while the instance is still starting up (i.e.
/// before the deferred startup work completes). Everything else is gated.
///
/// `/ready` is allowed through so its handler can report not-ready (503) during
/// startup; `/status` is liveness; `/health` is backend reachability; `/version`
/// is static metadata; and `/metrics` must stay scrapeable throughout startup.
fn always_available(path: &str) -> bool {
	matches!(path, "/" | "/status" | "/health" | "/version" | "/ready" | "/metrics")
}

/// Middleware that returns `503 Service Unavailable` for user-facing endpoints
/// until the instance is ready to serve (the deferred startup work has
/// completed).
///
/// The web server binds its listener before any of that work runs — including
/// the version check and this node's cluster registration — so this gate keeps
/// query/auth endpoints from being hit against a datastore that is not yet
/// initialised, while liveness/version/metrics and the probe endpoints stay
/// reachable. It sits ahead of the auth layer so gated requests short-circuit
/// before authentication touches the datastore.
pub(super) async fn readiness_gate(
	State(ready): State<Arc<AtomicBool>>,
	request: Request,
	next: Next,
) -> Response {
	if ready.load(Ordering::SeqCst) || always_available(request.uri().path()) {
		return next.run(request).await;
	}
	let mut response = NetError::NotReady.into_response();
	// Hint clients/SDKs to retry shortly once startup completes.
	response.headers_mut().insert(RETRY_AFTER, HeaderValue::from_static("1"));
	response
}

#[cfg(test)]
mod tests {
	use axum::Router;
	use axum::body::Body;
	use axum::middleware::from_fn_with_state;
	use axum::routing::get;
	use http::header::RETRY_AFTER;
	use http::{Request, StatusCode};
	use tower::ServiceExt;

	use super::*;

	fn app(ready: bool) -> Router {
		let flag = Arc::new(AtomicBool::new(ready));
		Router::new()
			.route("/", get(|| async { "ok" }))
			.route("/status", get(|| async {}))
			.route("/health", get(|| async { "ok" }))
			.route("/version", get(|| async { "ok" }))
			.route("/ready", get(|| async { "ok" }))
			.route("/metrics", get(|| async { "ok" }))
			.route("/sql", get(|| async { "ok" }))
			.route("/rpc", get(|| async { "ok" }))
			.route("/key/{tb}/{id}", get(|| async { "ok" }))
			.layer(from_fn_with_state(flag, readiness_gate))
	}

	async fn status_of(app: Router, uri: &str) -> StatusCode {
		app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
			.await
			.unwrap()
			.status()
	}

	#[tokio::test]
	async fn allowlisted_routes_serve_while_starting() {
		for uri in ["/", "/status", "/health", "/version", "/ready", "/metrics"] {
			assert_eq!(
				status_of(app(false), uri).await,
				StatusCode::OK,
				"{uri} should bypass the readiness gate while starting"
			);
		}
	}

	#[tokio::test]
	async fn query_routes_are_gated_while_starting() {
		for uri in ["/sql", "/rpc", "/key/users/1"] {
			assert_eq!(
				status_of(app(false), uri).await,
				StatusCode::SERVICE_UNAVAILABLE,
				"{uri} should be gated while starting"
			);
		}
	}

	#[tokio::test]
	async fn gate_sets_retry_after_header() {
		let res = app(false)
			.oneshot(Request::builder().uri("/sql").body(Body::empty()).unwrap())
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
		assert_eq!(res.headers().get(RETRY_AFTER).unwrap().to_str().unwrap(), "1");
	}

	/// A node with no cluster membership row reports not-ready, rather than the
	/// storage failure a missing row used to read as. The row is absent until
	/// this node registers during startup, and again once a peer archives and
	/// reaps it; both clear on the next registration.
	#[cfg(feature = "storage-mem")]
	#[tokio::test]
	async fn an_unregistered_node_is_not_ready() {
		use std::time::Duration;

		use surrealdb_core::kvs::Datastore;

		use crate::ntw::Readiness;
		use crate::ntw::client_ip::ClientIp;

		let state = AppState {
			client_ip: ClientIp::None,
			datastore: Datastore::new("memory").await.unwrap(),
			metrics_observer: None,
			// Started: only the heartbeat is left to decide readiness.
			readiness: Readiness {
				ready: Arc::new(AtomicBool::new(true)),
				max_heartbeat_age: Some(Duration::from_secs(9)),
			},
		};

		assert!(
			matches!(community_readiness(&state).await, Err(NetError::NotReady)),
			"an unregistered node should report not-ready"
		);

		// And a registered node with a fresh heartbeat is ready, so the arm above
		// is reporting the missing row rather than refusing everything.
		state.datastore.insert_node().await.unwrap();
		assert!(
			community_readiness(&state).await.is_ok(),
			"a freshly registered node should report ready"
		);
	}

	#[tokio::test]
	async fn all_routes_serve_once_ready() {
		for uri in ["/sql", "/rpc", "/key/users/1", "/status", "/health", "/ready"] {
			assert_eq!(
				status_of(app(true), uri).await,
				StatusCode::OK,
				"{uri} should serve once ready"
			);
		}
	}
}
