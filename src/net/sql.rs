use crate::err::Error;
use crate::net::head;
use crate::net::output;
use crate::net::session;
use crate::net::DB;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use surrealdb::Session;
use warp::Filter;

const MAX: u64 = 1024 * 1024; // 1 MiB

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("sql").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(session::build())
		.and(warp::header::<String>(http::header::CONTENT_TYPE.as_str()))
		.and(warp::body::content_length_limit(MAX))
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
	let db = DB.get().unwrap();
	let sql = std::str::from_utf8(&sql).unwrap();
	match db.execute(sql, &session, None).await {
		Ok(res) => match output.as_ref() {
			"application/json" => Ok(output::json(&res)),
			"application/cbor" => Ok(output::cbor(&res)),
			"application/msgpack" => Ok(output::pack(&res)),
			_ => Err(warp::reject::not_found()),
		},
		Err(err) => Err(warp::reject::custom(Error::from(err))),
	}
}
