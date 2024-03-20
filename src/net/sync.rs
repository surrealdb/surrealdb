use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

pub(super) fn router<S>() -> Router<S>
where
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
