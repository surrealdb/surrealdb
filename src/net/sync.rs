use warp::http;
use warp::Filter;

#[allow(opaque_hidden_inferred_bound)]
pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("sync").and(warp::path::end());
	// Set save method
	let save = base.and(warp::get()).and_then(save);
	// Set load method
	let load = base.and(warp::post()).and_then(load);
	// Specify route
	save.or(load)
}

pub async fn load() -> Result<impl warp::Reply, warp::Rejection> {
	Ok(warp::reply::with_status("Load", http::StatusCode::OK))
}

pub async fn save() -> Result<impl warp::Reply, warp::Rejection> {
	Ok(warp::reply::with_status("Save", http::StatusCode::OK))
}
