//! Contains the actual fetch function.

use std::sync::Arc;

use futures::TryStreamExt;
use js::function::Opt;
use js::{Class, Ctx, Exception, Result, Value};
use reqwest::Body as ReqBody;
use reqwest::header::{CONTENT_TYPE, HeaderValue};

use super::classes::Headers;
use crate::fnc::script::fetch::RequestError;
use crate::fnc::script::fetch::body::{Body, BodyData, BodyKind};
use crate::fnc::script::fetch::classes::{
	Request, RequestInit, RequestRedirect, Response, ResponseInit, ResponseType,
};
use crate::fnc::script::modules::surrealdb::query::QueryContext;
use crate::http::HttpClient;

#[js::function]
pub async fn fetch<'js>(
	ctx: Ctx<'js>,
	input: Value<'js>,
	init: Opt<RequestInit<'js>>,
) -> Result<Response<'js>> {
	// Create a request from the input.
	let js_req = Request::new(ctx.clone(), input, init)?;

	let url = js_req.url;

	// Check if the url is allowed to be fetched.
	let query_ctx = if let Some(query_ctx) = ctx.userdata::<QueryContext<'js>>() {
		query_ctx.context.clone()
	} else {
		panic!(
			"Trying to fetch a URL but no QueryContext is present. QueryContext is required for checking if the URL is allowed to be fetched."
		)
	};

	query_ctx
		.check_allowed_net(&url)
		.await
		.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))?;

	// SurrealDB Implementation keeps all javascript parts inside the context::with
	// scope so this unwrap should never panic.
	let headers = js_req.init.headers;
	let headers = headers.borrow();
	let mut headers = headers.inner.clone();

	let client = match js_req.init.request_redirect {
		RequestRedirect::Follow => query_ctx.http_client(),
		RequestRedirect::Error => {
			let cap = query_ctx.get_capabilities();
			Arc::new(
				HttpClient::new_with_redirect_policy(
					cap.allow_net.clone(),
					cap.allow_net.clone(),
					|attempt| attempt.error("unexpected redirect"),
				)
				.map_err(|e| {
					Exception::throw_internal(
						&ctx,
						&format!("Could not initialize http client: {e}"),
					)
				})?,
			)
		}
		RequestRedirect::Manual => {
			let cap = query_ctx.get_capabilities();
			Arc::new(
				HttpClient::new_with_redirect_policy(
					cap.allow_net.clone(),
					cap.allow_net.clone(),
					|attempt| attempt.stop(),
				)
				.map_err(|e| {
					Exception::throw_internal(
						&ctx,
						&format!("Could not initialize http client: {e}"),
					)
				})?,
			)
		}
	};

	let mut req_builder = client.request(js_req.init.method, url.clone());
	// Set the body for the request.
	if let Some(body) = js_req.init.body {
		match body.data.replace(BodyData::Used) {
			BodyData::Stream(x) => {
				let body = ReqBody::wrap_stream(x.into_inner());
				req_builder = req_builder.body(body);
			}
			BodyData::Buffer(x) => {
				let body = ReqBody::from(x);
				req_builder = req_builder.body(body);
			}
			BodyData::Used => return Err(Exception::throw_type(&ctx, "Body unusable")),
		};
		match body.kind {
			BodyKind::Buffer => {}
			BodyKind::String => {
				headers
					.entry(CONTENT_TYPE)
					.or_insert_with(|| HeaderValue::from_static("text/plain;charset=UTF-8"));
			}
			BodyKind::Blob(mime) => {
				if let Ok(x) = HeaderValue::from_bytes(mime.as_bytes()) {
					// TODO: Not according to spec, figure out the specific Mime -> Content-Type
					// -> Mime conversion process.
					headers.entry(CONTENT_TYPE).or_insert_with(|| x);
				}
			}
		}
	}

	// make the request
	let response = req_builder
		.headers(headers)
		.send()
		.await
		.map_err(|e| Exception::throw_type(&ctx, &e.to_string()))?;

	// Extract the headers
	let headers = Headers::from_map(response.headers().clone());
	let headers = Class::instance(ctx, headers)?;
	let init = ResponseInit {
		headers,
		status: response.status().as_u16(),
		status_text: response.status().canonical_reason().unwrap_or("").to_owned(),
	};

	// Extract the body
	let body = Body::stream(
		BodyKind::Buffer,
		response.bytes_stream().map_err(Arc::new).map_err(RequestError::Reqwest),
	);
	let response = Response {
		body,
		init,
		url: Some(url),
		r#type: ResponseType::Default,
		was_redirected: false,
	};
	Ok(response)
}
