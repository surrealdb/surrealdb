use crate::cnf;
use warp::http::Uri;
use warp::Filter;

#[allow(opaque_hidden_inferred_bound)]
pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path::end()
		.and(warp::get())
		.map(|| warp::redirect::temporary(Uri::from_static(cnf::APP_ENDPOINT)))
}
