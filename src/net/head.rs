use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERS;
use crate::cnf::SERVER_NAME;

const ID: &str = "ID";
const NS: &str = "NS";
const DB: &str = "DB";
const SERVER: &str = "Server";
const VERSION: &str = "Version";

pub fn version() -> warp::filters::reply::WithHeader {
	let val = format!("{}-{}", PKG_NAME, *PKG_VERS);
	warp::reply::with::header(VERSION, val)
}

pub fn server() -> warp::filters::reply::WithHeader {
	warp::reply::with::header(SERVER, SERVER_NAME)
}

pub fn cors() -> warp::filters::cors::Builder {
	warp::cors()
		.max_age(86400)
		.allow_any_origin()
		.allow_methods(vec![
			http::Method::GET,
			http::Method::PUT,
			http::Method::POST,
			http::Method::PATCH,
			http::Method::DELETE,
			http::Method::OPTIONS,
		])
		.allow_headers(vec![
			http::header::ACCEPT,
			http::header::AUTHORIZATION,
			http::header::CONTENT_TYPE,
			http::header::ORIGIN,
			NS.parse().unwrap(),
			DB.parse().unwrap(),
			ID.parse().unwrap(),
		])
}
