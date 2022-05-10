// use crate::net::DB;
// use hyper::body::Body;
// use surrealdb::dbs::export;
use crate::net::session;
use surrealdb::Session;
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

async fn handler(_session: Session) -> Result<impl warp::Reply, warp::Rejection> {
	// let db = DB.get().unwrap().clone();
	// let (chn, body) = Body::channel();
	// tokio::spawn(export(db, session, chn));
	// Ok(warp::reply::Response::new(body))
	Ok(warp::reply::reply())
}
