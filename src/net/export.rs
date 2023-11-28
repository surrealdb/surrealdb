use crate::dbs::DB;
use crate::err::Error;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum::{response::Response, Extension};
use bytes::Bytes;
use http::StatusCode;
use http_body::Body as HttpBody;
use hyper::body::Body;
use surrealdb::dbs::Session;

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/export", get(handler))
}

async fn handler(
	Extension(session): Extension<Session>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Extract the NS header value
	let nsv = match session.ns.clone() {
		Some(ns) => ns,
		None => return Err(Error::NoNamespace),
	};
	// Extract the DB header value
	let dbv = match session.db.clone() {
		Some(db) => db,
		None => return Err(Error::NoDatabase),
	};
	// Create a chunked response
	let (mut chn, bdy) = Body::channel();
	// Create a new bounded channel
	let (snd, rcv) = surrealdb::channel::bounded(1);

	let export_job = db.export(&session, nsv, dbv, snd).await.map_err(Error::from)?;
	// Spawn a new database export job
	tokio::spawn(export_job);
	// Process all processed values
	tokio::spawn(async move {
		while let Ok(v) = rcv.recv().await {
			let _ = chn.send_data(Bytes::from(v)).await;
		}
	});
	// Return the chunked body
	Ok(Response::builder().status(StatusCode::OK).body(bdy).unwrap())
}
