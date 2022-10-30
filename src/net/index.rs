use crate::cnf;
use warp::http::Uri;
use warp::Filter;

// FIXME: It finds a trait that isn't `pub` or something in this way.
#[allow(opaque_hidden_inferred_bound)]
pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path::end().and(warp::get()).map(|| warp::redirect(Uri::from_static(cnf::APP_ENDPOINT)))
}
