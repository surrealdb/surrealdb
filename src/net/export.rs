use crate::err::Error;
use crate::net::session;
use crate::net::DB;
use hyper::body::Body;
use surrealdb::Session;
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("export")
		.and(warp::path::end())
		.and(warp::get())
		.and(session::build())
		.and_then(handler)
}

async fn handler(session: Session) -> Result<impl warp::Reply, warp::Rejection> {
	// Check the permissions
	match session.au.is_db() {
		true => {
			// Get the datastore reference
			let db = DB.get().unwrap();
			// Extract the NS header value
			let nsv = session.ns.clone().unwrap();
			// Extract the DB header value
			let dbv = session.db.clone().unwrap();
			// Create a chunked response
			let (mut chn, bdy) = Body::channel();
			// Initiate a new async channel
			let (snd, mut rcv) = tokio::sync::mpsc::channel(100);
			// Spawn a new database export
			tokio::spawn(db.export(nsv, dbv, snd));
			// Process all processed values
			tokio::spawn(async move {
				while let Some(v) = rcv.recv().await {
					let _ = chn.send_data(v).await;
				}
			});
			// Return the chunked body
			Ok(warp::reply::Response::new(bdy))
		}
		_ => Err(warp::reject::custom(Error::InvalidAuth)),
	}
}
