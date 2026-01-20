// Standard library imports
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// External crate imports
use base64::prelude::*;
use hyper::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use surrealml_core::storage::stream_adapter::StreamAdapter;

// Local module imports
use crate::utils::EmptyReturn;
use crate::{empty_return_safe_eject, process_string_for_empty_return};

/// Uploads a model to a remote server.
///
/// # Arguments
/// * `file_path_ptr` - The path to the file to upload.
/// * `url_ptr` - The URL to upload the file to.
/// * `chunk_size` - The size of the chunks to upload the file in.
/// * `ns_ptr` - The namespace to upload the file to.
/// * `db_ptr` - The database to upload the file to.
/// * `username_ptr` - The username to use for authentication.
/// * `password_ptr` - The password to use for authentication.
///
/// # Returns
/// An empty return object indicating success or failure.
#[no_mangle]
pub extern "C" fn upload_model(
	file_path_ptr: *const c_char,
	url_ptr: *const c_char,
	chunk_size: usize,
	ns_ptr: *const c_char,
	db_ptr: *const c_char,
	username_ptr: *const c_char,
	password_ptr: *const c_char,
) -> EmptyReturn {
	// process the inputs
	let file_path = process_string_for_empty_return!(file_path_ptr, "file path");
	let url = process_string_for_empty_return!(url_ptr, "url");
	let ns = process_string_for_empty_return!(ns_ptr, "namespace");
	let db = process_string_for_empty_return!(db_ptr, "database");
	let username = match username_ptr.is_null() {
		true => None,
		false => Some(process_string_for_empty_return!(username_ptr, "username")),
	};
	let password = match password_ptr.is_null() {
		true => None,
		false => Some(process_string_for_empty_return!(password_ptr, "password")),
	};

	let client = Client::builder(TokioExecutor::new()).build_http();

	let uri: Uri = empty_return_safe_eject!(url.parse());
	let generator = empty_return_safe_eject!(StreamAdapter::new(chunk_size, file_path));
	let body = http_body_util::StreamBody::new(generator);
	let body = http_body_util::BodyExt::boxed(body);

	let part_req = Request::builder()
		.method(Method::POST)
		.uri(uri)
		.header(CONTENT_TYPE, "application/octet-stream")
		.header("surreal-ns", empty_return_safe_eject!(HeaderValue::from_str(&ns)))
		.header("surreal-db", empty_return_safe_eject!(HeaderValue::from_str(&db)));

	let req = if username.is_some() && password.is_some() {
		// unwraps are safe because we have already checked that the values are not None
		let encoded_credentials =
			BASE64_STANDARD.encode(format!("{}:{}", username.unwrap(), password.unwrap()));
		empty_return_safe_eject!(part_req
			.header(AUTHORIZATION, format!("Basic {}", encoded_credentials))
			.body(body))
	} else {
		empty_return_safe_eject!(part_req.body(body))
	};

	let tokio_runtime = empty_return_safe_eject!(tokio::runtime::Builder::new_current_thread()
		.enable_io()
		.enable_time()
		.build());
	tokio_runtime.block_on(async move {
		let _response = client.request(req).await.unwrap();
	});
	EmptyReturn::success()
}
