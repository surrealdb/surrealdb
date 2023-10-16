//! This file defines the endpoints for the ML API for uploading models and performing inference on the models for either raw tensors or buffered computes.
use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use http_body::Body as HttpBody;
use serde::Deserialize;
use serde_json::from_slice;
use std::collections::HashMap;
use surrealdb::dbs::Session;
use surrealdb::sql::{Part, Value};
use surrealml_utils::execution::compute::ModelComputation;
use surrealml_utils::storage::surml_file::SurMlFile;

use super::headers::Accept;


/// The router definition for the ML API endpoints.
pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/ml/import", post(import))
		.route("/ml/compute/raw", get(raw_compute))
		.route("/ml/compute/buffered", get(buffered_compute))
		.route_layer(DefaultBodyLimit::disable())
}

/// The body for the import endpoint.
/// 
/// # Fields
/// * `file` - The file containing all the information to store a model.
#[derive(Deserialize)]
struct Body {
	pub file: Vec<u8>,
}


/// This endpoint allows the user to import the model into the database.
async fn import(
	Extension(session): Extension<Session>,
	maybe_output: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// extract the bytes and file from the body
	let body: Body = from_slice(&body.to_vec()).expect("Failed to deserialize");
	let file = SurMlFile::from_bytes(body.file).map_err(|e| Error::Io(e))?; 

	// define the key and value to be inserted
	let id = format!("{}-{}", file.header.name.to_string(), file.header.version.to_string());
	let bytes = file.to_bytes();

	// convert the key and value to the correct types
	let id_value = Value::from(id.as_str());
	let data_value = Value::Bytes(bytes.into());

	// Get the datastore reference
	let db = match DB.get() {
		Some(db) => db,
		None => return Err(Error::NoDatabase),
	};

	let sql = "CREATE type::thing($table, $id) CONTENT { data: $data }";
	let vars = map! {
		// for now we are merely putting the model into a table called ML
		String::from("table") => Value::from("ML"),
		String::from("id") => id_value,
		String::from("data") => data_value,
	};
	match db.execute(sql, &session, Some(vars)).await {
		Ok(res) => match maybe_output.as_deref() {
			// Simple serialization
			Some(Accept::ApplicationJson) => Ok(output::json(&output::simplify(res))),
			Some(Accept::ApplicationCbor) => Ok(output::cbor(&output::simplify(res))),
			Some(Accept::ApplicationPack) => Ok(output::pack(&output::simplify(res))),
			// Internal serialization
			Some(Accept::Surrealdb) => Ok(output::full(&res)),
			// An incorrect content-type was requested
			_ => Err(Error::InvalidType),
		},
		// There was an error when executing the query
		Err(err) => Err(Error::from(err)),
	}
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
	Extension(session): Extension<Session>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// get the body
	let body: RawComputeBody = from_slice(&body.to_vec()).expect("Failed to deserialize");

	// get the value for the model
	let sql = "RETURN type::thing($table, $id).*";
	let vars = map! {
		// for now we are merely putting the model into a table called ML
		String::from("table") => Value::from("ML"),
		String::from("id") => Value::from(body.id)
	};

	// get the db
	let db = match DB.get() {
		Some(db) => db,
		None => return Err(Error::NoDatabase),
	};

	// perform the calculation
	let result = match db.execute(sql, &session, Some(vars)).await {
		Ok(res) => res,
		Err(e) => return Err(Error::from(e)),
	};

	let unwrapped_result = match result[0].result.as_ref() {
		Ok(unwrapped_result) => unwrapped_result,
		Err(e) => return Err(Error::Thrown(e.to_string())),
	};
	let response = match unwrapped_result.pick(&[Part::from("data")]) {
		Value::Bytes(unwrapped_bytes) => unwrapped_bytes,
		// _ => return Err(Error::from("bytes not returned for the value from the database query for the model")),
		_ => return Err(Error::Thrown("bytes not returned for the value from the database query for the model".to_string())),
	};

	let mut file = SurMlFile::from_bytes(response.to_vec()).map_err(|e| Error::from(e))?;
	let tensor = ndarray::arr1::<f32>(&body.input.as_slice()).into_dyn();

	let dims: Option<(i32, i32)> = match body.dims {
		Some(unwrapped_dims) => Some((unwrapped_dims[0] as i32, unwrapped_dims[1] as i32)),
		None => None,
	};

	let compute_unit = ModelComputation {
		surml_file: &mut file,
	};
	let output_tensor = compute_unit.raw_compute(tensor, dims);
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
	Extension(session): Extension<Session>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	let mut body: BufferedComputeBody = from_slice(&body.to_vec()).expect("Failed to deserialize");

	let vars = map! {
		// for now we are merely putting the model into a table called ML
		String::from("table") => Value::from("ML"),
		String::from("id") => Value::from(body.id)
	};

	// get the db
	let db = match DB.get() {
		Some(db) => db,
		None => return Err(Error::NoDatabase),
	};

	// perform the calculation
	let sql = "RETURN type::thing($table, $id).*";
	let result = match db.execute(sql, &session, Some(vars)).await {
		Ok(res) => res,
		Err(e) => return Err(Error::from(e)),
	};

	let unwrapped_result = match result[0].result.as_ref() {
		Ok(unwrapped_result) => unwrapped_result,
		Err(e) => return Err(Error::Thrown(e.to_string())),
	};
	let response = match unwrapped_result.pick(&[Part::from("data")]) {
		Value::Bytes(unwrapped_bytes) => unwrapped_bytes,
		_ => return Err(Error::Thrown("bytes not returned for the value from the database query for the model".to_string())),
	};

	let mut file = SurMlFile::from_bytes(response.to_vec()).map_err(|e| Error::from(e))?;

	let compute_unit = ModelComputation {
		surml_file: &mut file,
	};

	let output_tensor = compute_unit.buffered_compute(&mut body.input);
	Ok(output::json(&output::simplify(output_tensor)))
}
