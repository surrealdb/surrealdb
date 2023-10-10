use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use axum::routing::{post, get};
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
use serde_json::from_slice;
use surrealml_utils::storage::surml_file::SurMlFile;
use surrealml_utils::execution::compute::ModelComputation;
use surrealdb::sql::{Value, Part};
use serde::Deserialize;
use std::collections::HashMap;

use super::headers::Accept;



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


/// The body for the import endpoint
#[derive(Deserialize)]
struct Body {
    pub file: Vec<u8>,
}


async fn import(
	Extension(session): Extension<Session>,
	maybe_output: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// extract the bytes and file from the body
	let body: Body = from_slice(&body.to_vec()).expect("Failed to deserialize");
	let file = SurMlFile::from_bytes(body.file).map_err(|_| Error::Request)?;

	// define the key and value to be inserted
	let id = format!("{}-{}", file.header.name.to_string(), file.header.version.to_string());
	let bytes = file.to_bytes();

	// convert the key and value to the correct types
	let id_value = Value::from(id.as_str());
	let data_value = Value::Bytes(bytes.into());

	// Get the datastore reference
	let db = DB.get().unwrap();

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


#[derive(Deserialize)]
pub struct RawComputeBody {
	pub id: String,
	pub input: Vec<f32>,
	pub dims: Option<[i64; 2]>,
}


async fn raw_compute(
	Extension(session): Extension<Session>,
	body: Bytes
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
	// perform the calculation
	let result = match DB.get().unwrap().execute(sql, &session, Some(vars)).await {
		Ok(res) => res,
		Err(_) => unreachable!(),
	};

	let response = match result[0].result.as_ref().unwrap().pick(&[Part::from("data")]) {
		Value::Bytes(unwrapped_bytes) => unwrapped_bytes,
		_ => return Err("not supported format for ML model")
	};

	let mut file = SurMlFile::from_bytes(response.to_vec()).unwrap();
	let mut tensor = ndarray::arr1::<f32>(&body.input.as_slice()).into_dyn();

	match body.dims {
        Some(_dims) => {
            tensor = tensor.into_shape((1, 28)).unwrap().into_dyn();
        },
        None => {}
    }

    let compute_unit = ModelComputation {
        surml_file: &mut file
    };
    let output_tensor = compute_unit.raw_compute(tensor);
	Ok(output::json(&output::simplify(output_tensor)))
}


#[derive(Deserialize)]
pub struct BufferedComputeBody {
	pub id: String,
	pub input: HashMap<String, f32>,
}


async fn buffered_compute(
	Extension(session): Extension<Session>,
	body: Bytes
) -> Result<impl IntoResponse, impl IntoResponse> {
	let mut body: BufferedComputeBody = from_slice(&body.to_vec()).expect("Failed to deserialize");

	let vars = map! {
		// for now we are merely putting the model into a table called ML
		String::from("table") => Value::from("ML"),
		String::from("id") => Value::from(body.id)
	};
	// perform the calculation
	let sql = "RETURN type::thing($table, $id).*";
	let result = match DB.get().unwrap().execute(sql, &session, Some(vars)).await {
		Ok(res) => res,
		Err(_) => unreachable!(),
	};

	let response = match result[0].result.as_ref().unwrap().pick(&[Part::from("data")]) {
		Value::Bytes(unwrapped_bytes) => unwrapped_bytes,
		_ => return Err("not supported format for ML model")
	};

	let mut file = SurMlFile::from_bytes(response.to_vec()).unwrap();

    let compute_unit = ModelComputation {
        surml_file: &mut file
    };

	let output_tensor = compute_unit.buffered_compute(&mut body.input);
	Ok(output::json(&output::simplify(output_tensor)))
}
