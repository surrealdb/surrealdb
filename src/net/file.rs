use axum::{extract::DefaultBodyLimit, Router};
use tower_http::limit::RequestBodyLimitLayer;

use crate::cnf::HTTP_MAX_FILE_SIZE;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
	// .route("/file/:ns/:db/:bucket/*path", any(handler))
	// .route_layer(DefaultBodyLimit::disable())
	// .layer(RequestBodyLimitLayer::new(*HTTP_MAX_FILE_SIZE))
}
