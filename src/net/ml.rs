//! This file defines the endpoints for the ML API for uploading models and performing inference on the models for either raw tensors or buffered computes.
use crate::net::output;
use axum::extract::{BodyStream, DefaultBodyLimit};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::Body as HttpBody;
use serde::Deserialize;
use serde_json::from_slice;
use std::collections::HashMap;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb::kvs::LockType::Optimistic;
use surrealdb::kvs::TransactionType::{Read, Write};
use tower_http::limit::RequestBodyLimitLayer;

use surrealdb::obs::{
	get::get_local_file,
	insert::{insert_local_file, InsertStatus},
};
use surrealml_core::execution::compute::ModelComputation;
use surrealml_core::storage::surml_file::SurMlFile;

use super::headers::Accept;

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
		.route("/ml/import", post(import).layer(RequestBodyLimitLayer::new(MAX)))
		.route("/ml/compute/raw", get(raw_compute))
		.route("/ml/compute/buffered", get(buffered_compute))
		.route_layer(DefaultBodyLimit::disable())
}

/// This endpoint allows the user to import the model into the database.
async fn import(
	Extension(_session): Extension<Session>,
	_maybe_output: Option<TypedHeader<Accept>>,
	mut stream: BodyStream,
) -> Result<impl IntoResponse, impl IntoResponse> {
	let mut buffer = Vec::new();
	while let Some(chunk) = stream.next().await {
		let chunk = chunk.unwrap();
		buffer.extend_from_slice(&chunk);
	}
	let file = match SurMlFile::from_bytes(buffer) {
		Ok(file) => file,
		Err(err) => return Err(output::json::<String>(&err.to_string())),
	};

	// define the key and value to be inserted
	let id = format!("{}-{}", file.header.name.to_string(), file.header.version.to_string());
	let bytes = file.to_bytes();

	let file_hash = match insert_local_file(bytes).await.unwrap() {
		InsertStatus::Inserted(hash) => hash,
		InsertStatus::AlreadyExists(hash) => hash,
	};
	if false == true {
		return Err(output::json::<String>(&"Not implemented".to_string()));
	}

	let ds = Datastore::new("file://ml_cache.db").await.unwrap();
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(id.clone(), file_hash).await.unwrap();
	let _ = tx.commit().await.unwrap();
	Ok(output::json(&output::simplify(id)))
}

/// The body for the raw compute endpoint on the ML API for a model.
///
/// # Fields
/// * `id` - The id of the model to compute
/// * `input` - The input to the model
/// * `dims` - The dimensions of the input
#[derive(Deserialize)]
pub struct RawComputeBody {
	pub id: String,
	pub input: Vec<f32>,
	pub dims: Option<[i64; 2]>,
}

/// This endpoint allows the user to compute the model with the given input of a raw tensor.
async fn raw_compute(
	Extension(_session): Extension<Session>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// get the body
	let body: RawComputeBody = from_slice(&body.to_vec()).expect("Failed to deserialize");
	let response: String;
	{
		let ds = Datastore::new("file://ml_cache.db")
			.await
			.map_err(|e| output::json::<String>(&e.to_string()))?;
		let mut tx = ds
			.transaction(Read, Optimistic)
			.await
			.map_err(|e| output::json::<String>(&e.to_string()))?;
		response = String::from_utf8(tx.get(body.id).await.unwrap().unwrap()).unwrap();
	}
	// get the local file bytes from the object storage
	let file_bytes = get_local_file(response).await.unwrap();
	let mut file = SurMlFile::from_bytes(file_bytes).unwrap();

	// define the inputs for the model
	let tensor = ndarray::arr1::<f32>(&body.input.as_slice()).into_dyn();
	let dims: Option<(i32, i32)> = match body.dims {
		Some(unwrapped_dims) => Some((unwrapped_dims[0] as i32, unwrapped_dims[1] as i32)),
		None => None,
	};

	// compute the model
	let compute_unit = ModelComputation {
		surml_file: &mut file,
	};
	if false == true {
		return Err(output::json::<String>(&"Not implemented".to_string()));
	}

	let output_tensor = compute_unit.raw_compute(tensor, dims).unwrap();
	Ok(output::json(&output::simplify(output_tensor)))
}

/// The body for the buffered compute endpoint on the ML API for a model.
///
/// # Fields
/// * `id` - The id of the model to compute
/// * `input` - The inputs for the model to compute
#[derive(Deserialize)]
pub struct BufferedComputeBody {
	pub id: String,
	pub input: HashMap<String, f32>,
}

/// This endpoint allows the user to compute the model with the given input of a buffered compute.
async fn buffered_compute(
	Extension(_session): Extension<Session>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	let mut body: BufferedComputeBody = from_slice(&body.to_vec()).expect("Failed to deserialize");

	let response: String;
	{
		let ds = Datastore::new("file://ml_cache.db").await.unwrap();
		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		response = String::from_utf8(tx.get(body.id).await.unwrap().unwrap()).unwrap();
	}

	// get the local file bytes from the object storage
	let file_bytes = get_local_file(response).await.unwrap();
	let mut file = SurMlFile::from_bytes(file_bytes).unwrap();

	let compute_unit = ModelComputation {
		surml_file: &mut file,
	};

	if false == true {
		return Err(output::json::<String>(&"Not implemented".to_string()));
	}

	let output_tensor = compute_unit.buffered_compute(&mut body.input).unwrap();
	Ok(output::json(&output::simplify(output_tensor)))
}
