use super::AppState;
use crate::err::Error;
use axum::body::Body;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum::{response::Response, Extension};
use bytes::Bytes;
use http::StatusCode;
use surrealdb::dbs::Session;
use surrealdb::iam::check::check_ns_db;
use surrealdb::iam::Action::View;
use surrealdb::iam::ResourceKind::Any;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/export", get(handler))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
) -> Result<impl IntoResponse, Error> {
	// Get the datastore reference
	let db = &state.datastore;
	// Create a chunked response
	let (chn, body_stream) = surrealdb::channel::bounded::<Result<Bytes, Error>>(1);
	let body = Body::from_stream(body_stream);
	// Ensure a NS and DB are set
	let (nsv, dbv) = check_ns_db(&session)?;
	// Check the permissions level
	db.check(&session, View, Any.on_db(&nsv, &dbv))?;
	// Create a new bounded channel
	let (snd, rcv) = surrealdb::channel::bounded(1);
	// Start the export task
	let task = db.export(&session, snd).await?;
	// Spawn a new database export job
	tokio::spawn(task);
	// Process all chunk values
	tokio::spawn(async move {
		while let Ok(v) = rcv.recv().await {
			// let _ = chn.send_data(Bytes::from(v)).await;
			let _ = chn.send(Ok(Bytes::from(v))).await;
		}
	});
	// Return the chunked body
	Ok(Response::builder().status(StatusCode::OK).body(body).unwrap())
}
