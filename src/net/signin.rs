use crate::net::head;
use warp::http;
use warp::Filter;

const MAX: u64 = 1024; // 1 KiB

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("signin").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base.and(warp::post()).and(warp::body::content_length_limit(MAX)).and_then(handler);
	// Specify route
	opts.or(post).with(head::cors())
}

async fn handler() -> Result<impl warp::Reply, warp::Rejection> {
	Ok(warp::reply::with_status("Ok", http::StatusCode::OK))
}
