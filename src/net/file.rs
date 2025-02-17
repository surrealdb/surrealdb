use axum::Router;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/file/:ns/:db/:bucket/*path", any(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_FILE_SIZE))
}
