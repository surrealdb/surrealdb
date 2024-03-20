use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/version", get(handler))
}

async fn handler() -> impl IntoResponse {
	format!("{PKG_NAME}-{}", *PKG_VERSION)
}
