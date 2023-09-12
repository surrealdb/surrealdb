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
use surrealml::storage::surml_file::SurMlFile;
use surrealml::execution::compute::ModelComputation;
use surrealdb::sql::{Value, Part};
use serde::Deserialize;
use tch::Tensor;
// use surrealdb::sql::statements::OutputStatement;
// use surrealdb::sql::Thing;
use std::collections::HashMap;

use super::headers::Accept;

// const MAX: usize = 1024 * 1024 * 1024 * 4; // 4 GiB


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
		// .layer(RequestBodyLimitLayer::new(MAX))
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
		Value::Bytes(unwrapped_bytes) => unwrapped_bytes.0,
		_ => return Err("not supported format for ML model")
	};

	let mut file = SurMlFile::from_bytes(response).unwrap();
	let mut tensor = Tensor::f_from_slice::<f32>(body.input.as_slice()).unwrap();

	match body.dims {
        Some(dims) => {
            tensor = tensor.reshape(&dims);
        },
        None => {}
    }

	file.model.set_eval();
    let compute_unit = ModelComputation {
        surml_file: &mut file
    };
    let output_tensor = compute_unit.raw_compute(tensor);
    // output_tensor
    let mut buffer = Vec::with_capacity(output_tensor.size()[0] as usize);

    for i in 0..output_tensor.size()[0] {
        buffer.push(output_tensor.double_value(&[i]) as f32);
    }
	Ok(output::json(&output::simplify(buffer)))
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
		Value::Bytes(unwrapped_bytes) => unwrapped_bytes.0,
		_ => return Err("not supported format for ML model")
	};

	let mut file = SurMlFile::from_bytes(response).unwrap();
	// let tensor = Tensor::f_from_slice::<f32>(body.input.as_slice()).unwrap();

	file.model.set_eval();
    let compute_unit = ModelComputation {
        surml_file: &mut file
    };

	let output_tensor = compute_unit.buffered_compute(&mut body.input);
	// output_tensor
    let mut buffer = Vec::with_capacity(output_tensor.size()[0] as usize);

    for i in 0..output_tensor.size()[0] {
        buffer.push(output_tensor.double_value(&[i]) as f32);
    }
	Ok(output::json(&output::simplify(buffer)))
}
