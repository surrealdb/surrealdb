use crate::dbs::DB;
use axum::Router;
use axum::routing::get;
use axum::{Extension, response::Response};
use axum::response::IntoResponse;
use bytes::Bytes;
use http::{StatusCode};
use http_body::Body as HttpBody;
use hyper::body::Body;
use surrealdb::dbs::Session;

pub(super) fn router<S, B>() -> Router<S, B>
where
    B: HttpBody + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/export", get(handler))
}

async fn handler(
	Extension(session): Extension<Session>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Check the permissions
	match session.au.is_db() {
		true => {
			// Get the datastore reference
			let db = DB.get().unwrap();
			// Extract the NS header value
			let nsv = match session.ns {
				Some(ns) => ns,
				None => return Err((StatusCode::BAD_REQUEST, "No namespace provided")),
			};
			// Extract the DB header value
			let dbv = match session.db {
				Some(db) => db,
				None => return Err((StatusCode::BAD_REQUEST, "No database provided")),
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
			return Ok(Response::builder()
				.status(StatusCode::OK)
				.body(bdy)
				.unwrap())
		}
		// The user does not have the correct permissions
		_ => return Err((StatusCode::FORBIDDEN, "Invalid permissions")),
	}
}
