//! This file defines the endpoints for the ML API for importing and exporting SurrealML models.
use super::AppState;
use crate::cnf::HTTP_MAX_ML_BODY_SIZE;
use crate::err::Error;
#[cfg(feature = "ml")]
use crate::net::output;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path};
use axum::response::IntoResponse;
#[cfg(feature = "ml")]
use axum::response::Response;
use axum::routing::{get, post};
use axum::Extension;
use axum::Router;
#[cfg(feature = "ml")]
use bytes::Bytes;
#[cfg(feature = "ml")]
use futures_util::StreamExt;
#[cfg(feature = "ml")]
use http::StatusCode;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
#[cfg(feature = "ml")]
use surrealdb::iam::check::check_ns_db;
#[cfg(feature = "ml")]
use surrealdb::iam::Action::{Edit, View};
#[cfg(feature = "ml")]
use surrealdb::iam::ResourceKind::Model;
#[cfg(feature = "ml")]
use surrealdb::kvs::{LockType::Optimistic, TransactionType::Read};
#[cfg(feature = "ml")]
use surrealdb::ml::storage::surml_file::SurMlFile;
#[cfg(feature = "ml")]
use surrealdb::sql::statements::{DefineModelStatement, DefineStatement};
use tower_http::limit::RequestBodyLimitLayer;

/// The router definition for the ML API endpoints.
pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/ml/import", post(import))
		.route("/ml/export/:name/:version", get(export))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_ML_BODY_SIZE))
}

/// This endpoint allows the user to import a model into the database.
#[cfg(feature = "ml")]
async fn import(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	body: Body,
) -> Result<impl IntoResponse, impl IntoResponse> {
	let mut stream = body.into_data_stream();
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Ml) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
		return Err(Error::ForbiddenRoute(RouteTarget::Ml.to_string()));
	}
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
#[cfg(feature = "ml")]
async fn export(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Path((name, version)): Path<(String, String)>,
) -> Result<impl IntoResponse, Error> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Ml) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
		return Err(Error::ForbiddenRoute(RouteTarget::Ml.to_string()));
	}
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
	let (chn, body_stream) = surrealdb::channel::bounded::<Result<Bytes, Error>>(1);
	let body = Body::from_stream(body_stream);
	// Process all stream values
	tokio::spawn(async move {
		while let Some(Ok(v)) = data.next().await {
			let _ = chn.send(Ok(v)).await;
		}
	});
	// Return the streamed body
	Ok(Response::builder().status(StatusCode::OK).body(body).unwrap())
}

/// This endpoint allows the user to import a model into the database.
#[cfg(not(feature = "ml"))]
async fn import(
	Extension(state): Extension<AppState>,
	Extension(_): Extension<Session>,
	_: Body,
) -> Result<(), impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Ml) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
		return Err(Error::ForbiddenRoute(RouteTarget::Ml.to_string()));
	}
	Err(Error::Request)
}

/// This endpoint allows the user to export a model from the database.
#[cfg(not(feature = "ml"))]
async fn export(
	Extension(state): Extension<AppState>,
	Extension(_): Extension<Session>,
	Path((_, _)): Path<(String, String)>,
) -> Result<(), impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Ml) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
		return Err(Error::ForbiddenRoute(RouteTarget::Ml.to_string()));
	}
	Err(Error::Request)
}
