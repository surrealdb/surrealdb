use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use http_body::Body as HttpBody;

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/version", get(handler))
}

async fn handler() -> impl IntoResponse {
	format!("{PKG_NAME}-{}", *PKG_VERSION)
}
