use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use warp::http;
use warp::Filter;

// FIXME: It finds a trait that isn't `pub` or something in this way (@Jerrody).
#[allow(opaque_hidden_inferred_bound)]
pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("version").and(warp::path::end()).and(warp::get()).and_then(handler)
}

pub async fn handler() -> Result<impl warp::Reply, warp::Rejection> {
	let val = format!("{PKG_NAME}-{}", *PKG_VERSION);
	Ok(warp::reply::with_status(val, http::StatusCode::OK))
}
