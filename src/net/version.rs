use warp::http;
use warp::Filter;

const NAME: &'static str = env!("CARGO_PKG_NAME");
const VERS: &'static str = env!("CARGO_PKG_VERSION");

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("version").and(warp::path::end()).and(warp::get()).and_then(handler)
}

pub async fn handler() -> Result<impl warp::Reply, warp::Rejection> {
	let val = format!("{}-{}", NAME, VERS);
	Ok(warp::reply::with_status(val, http::StatusCode::OK))
}
