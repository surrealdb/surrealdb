use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use http_body::Body as HttpBody;

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/sync", get(save).post(load))
}

async fn load() -> impl IntoResponse {
	"Load"
}

async fn save() -> impl IntoResponse {
	"Save"
}
