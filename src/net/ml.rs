//! This file defines the endpoints for the ML API for importing and exporting SurrealML models.
use super::headers::Accept;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use axum::extract::{BodyStream, DefaultBodyLimit, Path};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::post;
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use futures_util::StreamExt;
use http::StatusCode;
use http_body::Body as HttpBody;
use hyper::body::Body;
use surrealdb::dbs::Session;
use surrealdb::kvs::{LockType::Optimistic, TransactionType::Read};
use surrealdb::sql::statements::{DefineModelStatement, DefineStatement};
use surrealml_core::storage::surml_file::SurMlFile;
use tower_http::limit::RequestBodyLimitLayer;

const MAX: usize = 1024 * 1024 * 1024 * 4; // 4 GiB

/// The router definition for the ML API endpoints.
pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send + Into<Bytes>,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/ml/import", post(import))
		.route("/ml/export/:name/:version", post(export))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
}

/// This endpoint allows the user to import a model into the database.
async fn import(
	Extension(session): Extension<Session>,
	maybe_output: Option<TypedHeader<Accept>>,
	mut stream: BodyStream,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Convert the body to a byte slice
	match maybe_output.as_deref() {
		// Match application/octet-stream
		Some(Accept::ApplicationOctetStream) => {
			// Create a new buffer
			let mut buffer = Vec::new();
			// Load all the uploaded file chunks
			while let Some(chunk) = stream.next().await {
				buffer.extend_from_slice(&chunk?);
			}
			// Check that the SurrealML file is valid
			let file = match SurMlFile::from_bytes(buffer) {
				Ok(file) => file,
				Err(err) => return Err(Error::Other(err.to_string())),
			};
			// Convert the file back in to raw bytes
			let data = file.to_bytes();
			// Calculate the hash of the model file
			let hash = surrealdb::obs::hash(&data);
			// Insert the file data in to the store
			surrealdb::obs::put(&hash, data).await?;
			// Insert the model in to the database
			db.compute(
				DefineStatement::Model(DefineModelStatement {
					hash,
					name: file.header.name.to_string().into(),
					version: file.header.version.to_string(),
					..Default::default()
				})
				.into(),
				&session,
				None,
			)
			.await?;
			//
			Ok(output::none())
		}
		// An incorrect content-type was requested
		_ => Err(Error::InvalidType),
	}
}

/// This endpoint allows the user to export a model from the database.
async fn export(
	Extension(session): Extension<Session>,
	Path((name, version)): Path<(String, String)>,
) -> Result<impl IntoResponse, Error> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Check the permissions level
	let (nsv, dbv) = db.check(&session).await?;
	// Start a new readonly transaction
	let mut tx = db.transaction(Read, Optimistic).await?;
	// Attempt to get the model definition
	let info = tx.get_db_model(&nsv, &dbv, &name, &version).await?;
	// Export the file data in to the store
	let mut data = surrealdb::obs::stream(info.hash.to_owned()).await?;
	// Create a chunked response
	let (mut chn, body) = Body::channel();
	// Process all stream values
	tokio::spawn(async move {
		while let Some(Ok(v)) = data.next().await {
			let _ = chn.send_data(v).await;
		}
	});
	// Return the streamed body
	Ok(Response::builder().status(StatusCode::OK).body(body).unwrap())
}
