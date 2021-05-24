use crate::web::head;
use futures::{FutureExt, StreamExt};
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("sql").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(warp::header::<String>(http::header::CONTENT_TYPE.as_str()))
		.and(warp::body::content_length_limit(1024 * 1024 * 1)) // 1MiB
		.and(warp::body::bytes())
		.and_then(handler);
	// Set sock method
	let sock = base.and(warp::ws()).map(|ws: warp::ws::Ws| {
		ws.on_upgrade(|websocket| {
			// Just echo all messages back...
			let (tx, rx) = websocket.split();
			rx.forward(tx).map(|result| {
				if let Err(e) = result {
					eprintln!("websocket error: {:?}", e);
				}
			})
		})
	});
	// Specify route
	opts.or(post).or(sock).with(head::cors())
}

async fn handler(out: String, sql: bytes::Bytes) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = std::str::from_utf8(&sql).unwrap();
	let res = crate::dbs::execute(sql, None).unwrap();
	match out.as_ref() {
		"application/json" => Ok(warp::reply::json(&res)),
		"application/cbor" => Ok(warp::reply::json(&res)),
		_ => Err(warp::reject::not_found()),
	}
}
