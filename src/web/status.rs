use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("status").and(warp::path::end()).and(warp::get()).map(warp::reply)
}
