//! Axum sub-router for the `/metrics` endpoint.

use std::sync::Arc;

use axum::Router;
use axum::routing::get;

use super::handler::metrics;
use crate::rpc::RpcState;

/// Build the axum sub-router for `/metrics`.
///
/// The caller is responsible for installing a [`MetricsState`] extension on
/// the parent router before this sub-router is reached.
pub fn router() -> Router<Arc<RpcState>> {
	Router::new().route("/metrics", get(metrics))
}
