use crate::cli::CF;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use crate::net::params::Params;
use crate::net::session;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use surrealdb::Session;
use warp::ws::{Message, WebSocket, Ws};
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
		.and(warp::header::<String>(http::header::ACCEPT.as_str()))
		.and(warp::body::content_length_limit(MAX))
		.and(warp::body::bytes())
		.and(warp::query())
		.and(session::build())
		.and_then(handler);
	// Set sock method
	let sock = base
		.and(warp::ws())
		.and(session::build())
		.map(|ws: Ws, session: Session| ws.on_upgrade(move |ws| socket(ws, session)));
	// Specify route
	opts.or(post).or(sock)
}

async fn handler(
	output: String,
	sql: Bytes,
	params: Params,
	session: Session,
) -> Result<impl warp::Reply, warp::Rejection> {
	// Get a database reference
	let db = DB.get().unwrap();
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Convert the received sql query
	let sql = bytes_to_utf8(&sql)?;
	// Execute the received sql query
	match db.execute(sql, &session, params.parse().into(), opt.strict).await {
		// Convert the response to JSON
		Ok(res) => match output.as_ref() {
			"application/json" => Ok(output::json(&res)),
			"application/cbor" => Ok(output::cbor(&res)),
			"application/msgpack" => Ok(output::pack(&res)),
			// An incorrect content-type was requested
			_ => Err(warp::reject::custom(Error::InvalidType)),
		},
		// There was an error when executing the query
		Err(err) => Err(warp::reject::custom(Error::from(err))),
	}
}

async fn socket(ws: WebSocket, session: Session) {
	// Split the WebSocket connection
	let (mut tx, mut rx) = ws.split();
	// Wait to receive the next message
	while let Some(res) = rx.next().await {
		if let Ok(msg) = res {
			if let Ok(sql) = msg.to_str() {
				// Get a database reference
				let db = DB.get().unwrap();
				// Get local copy of options
				let opt = CF.get().unwrap();
				// Execute the received sql query
				let _ = match db.execute(sql, &session, None, opt.strict).await {
					// Convert the response to JSON
					Ok(v) => match serde_json::to_string(&v) {
						// Send the JSON response to the client
						Ok(v) => tx.send(Message::text(v)).await,
						// There was an error converting to JSON
						Err(e) => tx.send(Message::text(Error::from(e))).await,
					},
					// There was an error when executing the query
					Err(e) => tx.send(Message::text(Error::from(e))).await,
				};
			}
		}
	}
}
