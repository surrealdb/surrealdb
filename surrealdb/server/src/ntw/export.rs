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
use surrealdb_core::iam::check::check_ns_db;
use surrealdb_core::rpc::format::Format;
use surrealdb_iam::Action::View;
use surrealdb_iam::ResourceKind::Any;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_rpc::export;
use surrealdb_types::SurrealValue;

use super::AppState;
use super::error::ResponseError;
use super::headers::ContentType;
use crate::ntw::error::Error as NetError;

/// Export chunks buffered between the exporter and the body task.
const EXPORT_QUEUE_CHUNKS: usize = 2;

/// Upper bound of one coalesced body chunk. An export chunk larger than this is
/// sent on its own, unsplit.
const EXPORT_COALESCE_BYTES: usize = 64 * 1024;

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
	let rec_limit = state.datastore.parser_config().max_object_parsing_depth;
	let fmt = content_type.deref();
	let fmt: Format = fmt.into();
	let val = match fmt {
		Format::Json => surrealdb_core::rpc::format::json::decode(&body, rec_limit as usize)
			.map_err(anyhow::Error::msg)
			.map_err(ResponseError)?,
		// A shaped request body is CBOR.
		Format::Cbor | Format::Shaped => {
			surrealdb_core::rpc::format::cbor::decode(&body, rec_limit as usize)
				.map_err(anyhow::Error::msg)
				.map_err(ResponseError)?
		}
		Format::Cbor2 => {
			surrealdb_core::rpc::format::cbor::decode_request_v2(&body, rec_limit as usize)
				.map_err(anyhow::Error::msg)
				.map_err(ResponseError)?
		}
		// FIXME: Add flatbuffer recursion limit.
		Format::Flatbuffers => {
			surrealdb_types::decode(&body).map_err(anyhow::Error::msg).map_err(ResponseError)?
		}
		Format::Unsupported => {
			return Err(ResponseError(anyhow::Error::msg("unsupported body format")));
		}
	};

	let cfg =
		export::Config::from_value(val).map_err(|e| ResponseError(anyhow::anyhow!("{}", e)))?;
	handle_inner(state, session, cfg).await
}

/// Receives the next body chunk by merging export chunks until the next one
/// would push the total past [`EXPORT_COALESCE_BYTES`] or the export closes the
/// channel. Returns `None` once the channel is closed and drained. The chunk
/// that does not fit is held in `pending` and starts the next body chunk, so a
/// body chunk never exceeds the threshold unless a single export chunk does,
/// and such a chunk is returned as-is. The exporter emits one chunk per line, and
/// each body chunk becomes its own HTTP/2 DATA frame; a peer that budgets small
/// frames (h2's `data_frame_budget`) closes the connection with
/// ENHANCE_YOUR_CALM. Waits for input, so the first body bytes are delayed until
/// the threshold is reached or the export ends.
async fn recv_coalesced(
	rcv: &surrealdb::channel::Receiver<Vec<u8>>,
	pending: &mut Option<Vec<u8>>,
) -> Option<Vec<u8>> {
	let mut buf = match pending.take() {
		Some(v) => v,
		None => rcv.recv().await.ok()?,
	};
	if buf.len() >= EXPORT_COALESCE_BYTES {
		return Some(buf);
	}
	buf.reserve(EXPORT_COALESCE_BYTES - buf.len());
	while buf.len() < EXPORT_COALESCE_BYTES {
		let Ok(next) = rcv.recv().await else {
			break;
		};
		if buf.len() + next.len() > EXPORT_COALESCE_BYTES {
			*pending = Some(next);
			break;
		}
		buf.extend_from_slice(&next);
	}
	Some(buf)
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
	// Bounded channel between the exporter and the body task. It caps how far
	// the exporter can run ahead of the body task.
	let (snd, rcv) = surrealdb::channel::bounded(EXPORT_QUEUE_CHUNKS);
	// Start the export task
	let task = db.export_with_config(&session, snd, cfg).await.map_err(ResponseError)?;
	// Spawn a new database export job
	let export_handle = tokio::spawn(task);
	// Process all chunk values, stopping if the client disconnects
	tokio::spawn(async move {
		let mut client_disconnected = false;
		let mut pending = None;
		while let Some(v) = recv_coalesced(&rcv, &mut pending).await {
			if let Err(err) = chn.send(Ok(Bytes::from(v))).await {
				// The client disconnected; dropping `rcv` here will cause the
				// export task's channel sends to fail, aborting it promptly.
				tracing::info!("Export client disconnected, aborting export: {}", err);
				client_disconnected = true;
				break;
			}
		}
		// In the non-cancellation path, a failed export aborts the body rather
		// than ending it cleanly, so the client cannot mistake a partial export
		// for a complete one.
		if !client_disconnected {
			let err = match export_handle.await {
				Ok(Ok(())) => return,
				Ok(Err(err)) => {
					tracing::error!("Export task failed: {err}");
					err
				}
				Err(join_err) => {
					tracing::error!("Export task panicked: {join_err}");
					join_err.into()
				}
			};
			let _ = chn.send(Err(err)).await;
		}
	});
	// Return the chunked body
	Ok(Response::builder().status(StatusCode::OK).body(body)?)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn recv_coalesced_merges_small_chunks_until_close() {
		let (snd, rcv) = surrealdb::channel::bounded::<Vec<u8>>(10);
		for _ in 0..3 {
			snd.send(b"ab".to_vec()).await.unwrap();
		}
		drop(snd);
		let mut pending = None;
		assert_eq!(recv_coalesced(&rcv, &mut pending).await.unwrap(), b"ababab");
		assert!(recv_coalesced(&rcv, &mut pending).await.is_none());
	}

	#[tokio::test]
	async fn recv_coalesced_stops_at_the_cap() {
		let (snd, rcv) = surrealdb::channel::bounded::<Vec<u8>>(10);
		let big = vec![0u8; EXPORT_COALESCE_BYTES];
		snd.send(b"h".to_vec()).await.unwrap();
		snd.send(big.clone()).await.unwrap();
		snd.send(b"a".to_vec()).await.unwrap();
		drop(snd);
		let mut pending = None;
		assert_eq!(recv_coalesced(&rcv, &mut pending).await.unwrap(), b"h");
		assert_eq!(recv_coalesced(&rcv, &mut pending).await.unwrap(), big);
		assert_eq!(recv_coalesced(&rcv, &mut pending).await.unwrap(), b"a");
		assert!(recv_coalesced(&rcv, &mut pending).await.is_none());
	}

	#[tokio::test]
	async fn recv_coalesced_never_exceeds_the_cap() {
		let (snd, rcv) = surrealdb::channel::bounded::<Vec<u8>>(10);
		let near = vec![0u8; EXPORT_COALESCE_BYTES - 1];
		snd.send(near.clone()).await.unwrap();
		snd.send(b"ab".to_vec()).await.unwrap();
		drop(snd);
		let mut pending = None;
		assert_eq!(recv_coalesced(&rcv, &mut pending).await.unwrap(), near);
		assert_eq!(recv_coalesced(&rcv, &mut pending).await.unwrap(), b"ab");
		assert!(recv_coalesced(&rcv, &mut pending).await.is_none());
	}
}
