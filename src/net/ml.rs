//! This file defines the endpoints for the ML API for importing and exporting SurrealML models.
use super::AppState;
use crate::err::Error;
use crate::net::output;
use axum::extract::{BodyStream, DefaultBodyLimit, Path};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::{get, post};
use axum::Extension;
use axum::Router;
use bytes::Bytes;
use futures_util::StreamExt;
use http::StatusCode;
use http_body::Body as HttpBody;
use hyper::body::Body;
use surrealdb::dbs::Session;
use surrealdb::iam::check::check_ns_db;
use surrealdb::iam::Action::{Edit, View};
use surrealdb::iam::ResourceKind::Model;
use surrealdb::kvs::{LockType::Optimistic, TransactionType::Read};
use surrealdb::ml::storage::surml_file::SurMlFile;
use surrealdb::sql::statements::{DefineModelStatement, DefineStatement};
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
		.route("/ml/export/:name/:version", get(export))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
}

/// This endpoint allows the user to import a model into the database.
async fn import(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	mut stream: BodyStream,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Ensure a NS and DB are set
	let (nsv, dbv) = check_ns_db(&session)?;
	// Check the permissions level
	db.check(&session, Edit, Model.on_db(&nsv, &dbv))?;
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

	// reject the file if there is no model name or version
	if file.header.name.to_string() == "" || file.header.version.to_string() == "" {
		return Err(Error::Other("Model name and version must be set".to_string()));
	}

	// Convert the file back in to raw bytes
	let data = file.to_bytes();
	// Calculate the hash of the model file
	let hash = surrealdb::obs::hash(&data);
	// Calculate the path of the model file
	let path = format!(
		"ml/{nsv}/{dbv}/{}-{}-{hash}.surml",
		file.header.name.to_string(),
		file.header.version.to_string()
	);
	// Insert the file data in to the store
	surrealdb::obs::put(&path, data).await?;
	// Insert the model in to the database
	let mut model = DefineModelStatement::default();
	model.name = file.header.name.to_string().into();
	model.version = file.header.version.to_string();
	model.comment = Some(file.header.description.to_string().into());
	model.hash = hash;
	db.process(DefineStatement::Model(model).into(), &session, None).await?;
	//
	Ok(output::none())
}

/// This endpoint allows the user to export a model from the database.
async fn export(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Path((name, version)): Path<(String, String)>,
) -> Result<impl IntoResponse, Error> {
	// Get the datastore reference
	let db = &state.datastore;
	// Ensure a NS and DB are set
	let (nsv, dbv) = check_ns_db(&session)?;
	// Check the permissions level
	db.check(&session, View, Model.on_db(&nsv, &dbv))?;
	// Start a new readonly transaction
	let tx = db.transaction(Read, Optimistic).await?;
	// Attempt to get the model definition
	let info = tx.get_db_model(&nsv, &dbv, &name, &version).await?;
	// Calculate the path of the model file
	let path = format!("ml/{nsv}/{dbv}/{name}-{version}-{}.surml", info.hash);
	// Export the file data in to the store
	let mut data = surrealdb::obs::stream(path).await?;
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
