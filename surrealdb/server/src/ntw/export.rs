use std::ops::Deref;

use anyhow::Result;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use http::StatusCode;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::iam::Action::View;
use surrealdb_core::iam::ResourceKind::Any;
use surrealdb_core::iam::check::check_ns_db;
use surrealdb_core::kvs::export;
use surrealdb_core::rpc::format::Format;
use surrealdb_types::SurrealValue;

use super::AppState;
use super::error::ResponseError;
use super::headers::ContentType;
use crate::ntw::error::Error as NetError;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/export", options(|| async {}).get(get_handler).post(post_handler))
}

async fn get_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
) -> Result<impl IntoResponse, ResponseError> {
	let cfg = export::Config::default();
	handle_inner(state, session, cfg).await
}

async fn post_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	content_type: TypedHeader<ContentType>,
	body: Bytes,
) -> Result<impl IntoResponse, ResponseError> {
	let rec_limit = state.datastore.config().max_object_parsing_depth;
	let fmt = content_type.deref();
	let fmt: Format = fmt.into();
	let val = match fmt {
		Format::Json => surrealdb_core::rpc::format::json::decode(&body, rec_limit as usize)
			.map_err(anyhow::Error::msg)
			.map_err(ResponseError)?,
		Format::Cbor => surrealdb_core::rpc::format::cbor::decode(&body, rec_limit as usize)
			.map_err(anyhow::Error::msg)
			.map_err(ResponseError)?,
		// FIXME: Add flatbuffer recursion limit.
		Format::Flatbuffers => surrealdb_core::rpc::format::flatbuffers::decode(&body)
			.map_err(anyhow::Error::msg)
			.map_err(ResponseError)?,
		Format::Unsupported => {
			return Err(ResponseError(anyhow::Error::msg("unsupported body format")));
		}
	};

	let cfg =
		export::Config::from_value(val).map_err(|e| ResponseError(anyhow::anyhow!("{}", e)))?;
	handle_inner(state, session, cfg).await
}

async fn handle_inner(
	state: AppState,
	session: Session,
	cfg: export::Config,
) -> Result<impl IntoResponse, ResponseError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Export) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Export);
		return Err(NetError::ForbiddenRoute(RouteTarget::Export.to_string()).into());
	}
	// Create a chunked response
	let (chn, body_stream) = surrealdb::channel::bounded::<Result<Bytes>>(1);
	let body = Body::from_stream(body_stream);
	// Ensure a NS and DB are set
	let (nsv, dbv) = check_ns_db(&session).map_err(ResponseError)?;
	// Check the permissions level
	db.check(&session, View, Any.on_db(&nsv, &dbv)).map_err(ResponseError)?;
	// Create a new bounded channel
	let (snd, rcv) = surrealdb::channel::bounded(1);
	// Start the export task
	let task = db.export_with_config(&session, snd, cfg).await.map_err(ResponseError)?;
	// Spawn a new database export job
	let export_handle = tokio::spawn(task);
	// Process all chunk values, stopping if the client disconnects
	tokio::spawn(async move {
		let mut client_disconnected = false;
		while let Ok(v) = rcv.recv().await {
			if let Err(err) = chn.send(Ok(Bytes::from(v))).await {
				// The client disconnected; dropping `rcv` here will cause the
				// export task's channel sends to fail, aborting it promptly.
				tracing::info!("Export client disconnected, aborting export: {}", err);
				client_disconnected = true;
				break;
			}
		}
		// In the non-cancellation path, surface any error from the export task
		// that would otherwise be silently dropped.
		if !client_disconnected {
			match export_handle.await {
				Ok(Ok(())) => {}
				Ok(Err(err)) => tracing::error!("Export task failed: {err}"),
				Err(join_err) => tracing::error!("Export task panicked: {join_err}"),
			}
		}
	});
	// Return the chunked body
	Ok(Response::builder().status(StatusCode::OK).body(body)?)
}
