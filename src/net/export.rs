use crate::dbs::DB;
use crate::err::Error;
use crate::net::session;
use bytes::Bytes;
use hyper::body::Body;
use surrealdb::dbs::Session;
use warp::Filter;

#[allow(opaque_hidden_inferred_bound)]
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
			let nsv = match session.ns {
				Some(ns) => ns,
				None => return Err(warp::reject::custom(Error::NoNsHeader)),
			};
			// Extract the DB header value
			let dbv = match session.db {
				Some(db) => db,
				None => return Err(warp::reject::custom(Error::NoDbHeader)),
			};
			// Create a chunked response
			let (mut chn, bdy) = Body::channel();
			// Create a new bounded channel
			let (snd, rcv) = surrealdb::channel::new(1);
			// Spawn a new database export
			tokio::spawn(db.export(nsv, dbv, snd));
			// Process all processed values
			tokio::spawn(async move {
				while let Ok(v) = rcv.recv().await {
					let _ = chn.send_data(Bytes::from(v)).await;
				}
			});
			// Return the chunked body
			Ok(warp::reply::Response::new(bdy))
		}
		// There was an error with permissions
		_ => Err(warp::reject::custom(Error::InvalidAuth)),
	}
}
