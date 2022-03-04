use warp::http;
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("import").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(warp::body::content_length_limit(1024 * 1024 * 1024)) // 1GiB
		.and_then(handler);
	// Specify route
	opts.or(post)
}

async fn handler() -> Result<impl warp::Reply, warp::Rejection> {
	Ok(warp::reply::with_status("Ok", http::StatusCode::OK))
}
