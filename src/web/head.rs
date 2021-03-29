const NAME: &'static str = env!("CARGO_PKG_NAME");
const VERS: &'static str = env!("CARGO_PKG_VERSION");

pub fn version() -> warp::filters::reply::WithHeader {
	let val = format!("{}-{}", NAME, VERS);
	warp::reply::with::header("Version", val)
}

pub fn server() -> warp::filters::reply::WithHeader {
	warp::reply::with::header("Server", "SurrealDB")
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
			"NS".parse().unwrap(),
			"DB".parse().unwrap(),
			"ID".parse().unwrap(),
		])
}
