//! This file defines the endpoints for the ML API for uploading models and performing inference on the models for either raw tensors or buffered computes.
// Standard library imports
use std::collections::HashMap;

// External crates imports
use axum::{
	body::{boxed, Body},
	extract::{BodyStream, DefaultBodyLimit},
	response::IntoResponse,
	routing::{get, post},
	Extension, Router, TypedHeader,
};
use bytes::Bytes;
use serde_json;
use futures_util::StreamExt;
use http::StatusCode;
use http_body::Body as HttpBody;
use hyper::Response;
use serde::Deserialize;
use serde_json::from_slice;
use surrealdb::{
	dbs::Session,
	kvs::{
		Datastore,
		LockType::Optimistic,
		TransactionType::{Read, Write},
	},
	obs::{
		get::get_local_file,
		insert::{insert_local_file, InsertStatus},
	},
};
use surrealml_core::{execution::compute::ModelComputation, storage::surml_file::SurMlFile};
use tower_http::limit::RequestBodyLimitLayer;

// Local module imports
use super::headers::Accept;
use crate::net::output;

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
	// if false == true {
	// 	return Err(output::json::<String>(&"Not implemented".to_string()));
	// }
	let mut buffer = Vec::new();
	while let Some(chunk) = stream.next().await {
		let chunk = chunk.unwrap();
		buffer.extend_from_slice(&chunk);
	}
	let file = match SurMlFile::from_bytes(buffer) {
		Ok(file) => file,
		Err(err) => {
			let file_error_response = Response::builder()
				.status(StatusCode::BAD_REQUEST)
				.body(boxed(Body::from(err.to_string())))
				.unwrap();
			return Err(file_error_response);
		}
	};

	// // define the key and value to be inserted
	let id = format!("{}-{}", file.header.name.to_string(), file.header.version.to_string());
	let bytes = file.to_bytes();

	let file_hash_result = match insert_local_file(bytes).await {
		Ok(file_hash_result) => file_hash_result,
		Err(err) => {
			let insert_error_response = Response::builder()
				.status(StatusCode::INTERNAL_SERVER_ERROR)
				.body(boxed(Body::from(err.to_string())))
				.unwrap();
			return Err(insert_error_response);
		}
	};
	let file_hash = match file_hash_result {
		InsertStatus::Inserted(hash) => hash,
		InsertStatus::AlreadyExists(hash) => hash,
	};

	let ds = match Datastore::new("file://ml_cache.db").await {
		Ok(ds) => ds,
		Err(err) => {
			let datastore_error_response = Response::builder()
				.status(StatusCode::FAILED_DEPENDENCY)
				.body(boxed(Body::from(err.to_string())))
				.unwrap();
			return Err(datastore_error_response);
		}
	};
	let mut tx = match ds.transaction(Write, Optimistic).await {
		Ok(tx) => tx,
		Err(err) => {
			let transaction_error_response = Response::builder()
				.status(StatusCode::INTERNAL_SERVER_ERROR)
				.body(boxed(Body::from(err.to_string())))
				.unwrap();
			return Err(transaction_error_response);
		}
	};
	match tx.set(id.clone(), file_hash).await {
		Ok(_) => (),
		Err(err) => {
			let set_error_response = Response::builder()
				.status(StatusCode::INTERNAL_SERVER_ERROR)
				.body(boxed(Body::from(err.to_string())))
				.unwrap();
			return Err(set_error_response);
		}
	};
	let _ = match tx.commit().await {
		Ok(_) => (),
		Err(err) => {
			let commit_error_response = Response::builder()
				.status(StatusCode::INTERNAL_SERVER_ERROR)
				.body(boxed(Body::from(err.to_string())))
				.unwrap();
			return Err(commit_error_response);
		}
	};
	let response = Response::builder()
		.status(StatusCode::CREATED)
		.body(id)
		.unwrap();
	Ok(response)
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
	let json = serde_json::to_string(&output_tensor).unwrap();
	let response = Response::builder()
		.status(StatusCode::OK)
		.body(boxed(Body::from(json)))
		.unwrap();
	Ok(response)
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
	let json = serde_json::to_string(&output_tensor).unwrap();
	let response = Response::builder()
		.status(StatusCode::OK)
		.body(boxed(Body::from(json)))
		.unwrap();
	Ok(response)
}
