//! This file defines the endpoints for the ML API for importing and exporting
//! SurrealML models.

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use tower_http::limit::RequestBodyLimitLayer;

use crate::cnf::HTTP_MAX_ML_BODY_SIZE;

/// The router definition for the ML API endpoints.
pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/ml/import", post(implementation::import))
		.route("/ml/export/:name/:version", get(implementation::export))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_ML_BODY_SIZE))
}

#[cfg(feature = "ml")]
mod implementation {
	use anyhow::Context;
	use axum::Extension;
	use axum::body::Body;
	use axum::extract::Path;
	use axum::response::Response;
	use bytes::Bytes;
	use futures_util::StreamExt;
	use http::StatusCode;

	use crate::core::dbs::Session;
	use crate::core::dbs::capabilities::RouteTarget;
	use crate::core::expr::statements::{DefineModelStatement, DefineStatement};
	use crate::core::expr::{Expr, Ident, LogicalPlan, TopLevelExpr, get_model_path};
	use crate::core::iam::check::check_ns_db;
	use crate::core::iam::{Action, ResourceKind};
	use crate::core::kvs::{LockType, TransactionType};
	use crate::core::ml::storage::surml_file::SurMlFile;
	use crate::net::AppState;
	use crate::net::error::{Error as NetError, ResponseError};
	use crate::net::output::Output;

	/// This endpoint allows the user to import a model into the database.
	pub async fn import(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		body: Body,
	) -> Result<Output, ResponseError> {
		let mut stream = body.into_data_stream();
		// Get the datastore reference
		let db = &state.datastore;
		// Check if capabilities allow querying the requested HTTP route
		if !db.allows_http_route(&RouteTarget::Ml) {
			warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
			return Err(NetError::ForbiddenRoute(RouteTarget::Ml.to_string()).into());
		}
		// Ensure a NS and DB are set
		let (nsv, dbv) = check_ns_db(&session).map_err(ResponseError)?;
		// Check the permissions level
		db.check(&session, Action::Edit, ResourceKind::Model.on_db(&nsv, &dbv))
			.map_err(ResponseError)?;
		// Create a new buffer
		let mut buffer = Vec::new();
		// Load all the uploaded file chunks
		while let Some(chunk) = stream.next().await {
			buffer.extend_from_slice(&chunk?);
		}
		// Check that the SurrealML file is valid
		let file =
			SurMlFile::from_bytes(buffer).map_err(anyhow::Error::new).map_err(ResponseError)?;

		// reject the file if there is no model name or version
		if file.header.name.to_string() == "" || file.header.version.to_string() == "" {
			return Err(ResponseError(anyhow::Error::msg("Model name and version must be set")));
		}

		// Convert the file back in to raw bytes
		let data = file.to_bytes();
		// Calculate the hash of the model file
		let hash = crate::core::obs::hash(&data);
		// Calculate the path of the model file
		let path = get_model_path(
			&nsv,
			&dbv,
			&file.header.name.to_string(),
			&file.header.version.to_string(),
			&hash,
		);
		// Insert the file data in to the store
		crate::core::obs::put(&path, data).await.map_err(ResponseError)?;
		// Insert the model in to the database
		let model = DefineModelStatement {
			name: Ident::new(file.header.name.to_string()).unwrap(),
			version: file.header.version.to_string(),
			comment: Some(file.header.description.to_string().into()),
			hash,
			..Default::default()
		};

		let q = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Model(
				model,
			))))],
		};

		db.process_plan(q, &session, None).await.map_err(ResponseError)?;
		//
		Ok(Output::None)
	}

	/// This endpoint allows the user to export a model from the database.
	pub async fn export(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		Path((name, version)): Path<(String, String)>,
	) -> Result<Response, ResponseError> {
		// Get the datastore reference

		let db = &state.datastore;
		// Check if capabilities allow querying the requested HTTP route
		if !db.allows_http_route(&RouteTarget::Ml) {
			warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
			return Err(NetError::ForbiddenRoute(RouteTarget::Ml.to_string()).into());
		}
		// Ensure a NS and DB are set
		let (nsv, dbv) = check_ns_db(&session).map_err(ResponseError)?;
		// Check the permissions level
		db.check(&session, Action::View, ResourceKind::Model.on_db(&nsv, &dbv))
			.map_err(ResponseError)?;
		// Start a new readonly transaction
		let tx = db
			.transaction(TransactionType::Read, LockType::Optimistic)
			.await
			.map_err(ResponseError)?;

		let db = tx.ensure_ns_db(&nsv, &dbv, false).await.map_err(ResponseError)?;
		// Attempt to get the model definition
		let info = match tx
			.get_db_model(db.namespace_id, db.database_id, &name, &version)
			.await
			.map_err(ResponseError)?
		{
			Some(info) => info,
			None => {
				return Err(NetError::NotFound(format!("Model {name} {version} not found")).into());
			}
		};
		// Calculate the path of the model file
		let path = format!("ml/{nsv}/{dbv}/{name}-{version}-{}.surml", info.hash);
		// Export the file data in to the store
		let mut data = crate::core::obs::stream(path)
			.await
			.context("Failed to read model file")
			.map_err(ResponseError)?;
		// Create a chunked response
		let (chn, body_stream) = surrealdb::channel::bounded::<Result<Bytes, anyhow::Error>>(1);
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
}

#[cfg(not(feature = "ml"))]
mod implementation {
	use axum::Extension;
	use axum::body::Body;
	use axum::extract::Path;

	use crate::core::dbs::Session;
	use crate::core::dbs::capabilities::RouteTarget;
	use crate::net::AppState;
	use crate::net::error::{Error as NetError, ResponseError};

	/// This endpoint allows the user to import a model into the database.
	pub async fn import(
		Extension(state): Extension<AppState>,
		Extension(_): Extension<Session>,
		_: Body,
	) -> Result<(), ResponseError> {
		// Get the datastore reference

		let db = &state.datastore;
		// Check if capabilities allow querying the requested HTTP route
		if !db.allows_http_route(&RouteTarget::Ml) {
			warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
			return Err(NetError::ForbiddenRoute(RouteTarget::Ml.to_string()).into());
		}
		Err(NetError::Request.into())
	}

	/// This endpoint allows the user to export a model from the database.
	pub async fn export(
		Extension(state): Extension<AppState>,
		Extension(_): Extension<Session>,
		Path((_, _)): Path<(String, String)>,
	) -> Result<(), ResponseError> {
		// Get the datastore reference

		let db = &state.datastore;
		// Check if capabilities allow querying the requested HTTP route
		if !db.allows_http_route(&RouteTarget::Ml) {
			warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Ml);
			return Err(NetError::ForbiddenRoute(RouteTarget::Ml.to_string()).into());
		}
		Err(NetError::Request.into())
	}
}
