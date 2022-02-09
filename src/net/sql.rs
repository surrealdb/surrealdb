use crate::dbs::Session;
use crate::net::conf;
use crate::net::head;
use crate::net::output;
use bytes::Bytes;
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
		.and(conf::build())
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

async fn handler(
	session: Session,
	output: String,
	sql: Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	let sql = std::str::from_utf8(&sql).unwrap();
	match crate::dbs::execute(sql, session, None).await {
		Ok(res) => match output.as_ref() {
			"application/json" => Ok(output::json(&res)),
			"application/cbor" => Ok(output::cbor(&res)),
			"application/msgpack" => Ok(output::pack(&res)),
			_ => Err(warp::reject::not_found()),
		},
		Err(err) => Err(warp::reject::custom(err)),
	}
}
