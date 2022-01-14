use crate::cnf;
use warp::http::Uri;
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path::end().and(warp::get()).map(|| warp::redirect(Uri::from_static(cnf::APP_ENDPOINT)))
}
