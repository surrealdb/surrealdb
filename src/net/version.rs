use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERS;
use warp::http;
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("version").and(warp::path::end()).and(warp::get()).and_then(handler)
}

pub async fn handler() -> Result<impl warp::Reply, warp::Rejection> {
	let val = format!("{}-{}", PKG_NAME, *PKG_VERS);
	Ok(warp::reply::with_status(val, http::StatusCode::OK))
}
