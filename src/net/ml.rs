//! This file defines the endpoints for the ML API for uploading models and performing inference on the models for either raw tensors or buffered computes.
use super::headers::Accept;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use axum::extract::{BodyStream, DefaultBodyLimit};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
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
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
}

/// This endpoint allows the user to import the model into the database.
async fn import(
	Extension(session): Extension<Session>,
	maybe_output: Option<TypedHeader<Accept>>,
	mut stream: BodyStream,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Convert the body to a byte slice
	match maybe_output.as_deref() {
		// Return nothing
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
