use crate::dbs::export;
use crate::dbs::Session;
use crate::net::conf;
use crate::net::DB;
use hyper::body::Body;
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("export").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set get method
	let get = base.and(warp::get()).and(conf::build()).and_then(handler);
	// Specify route
	opts.or(get)
}

async fn handler(session: Session) -> Result<impl warp::Reply, warp::Rejection> {
	let db = DB.get().unwrap().clone();
	let (chn, body) = Body::channel();
	tokio::spawn(export(db, session, chn));
	Ok(warp::reply::Response::new(body))
}
