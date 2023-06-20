//! Contains the actual fetch function.

use crate::fnc::script::fetch::{
	body::{Body, BodyData, BodyKind},
	classes::{
		self, HeadersClass, RequestClass, RequestInit, ResponseClass, ResponseInit, ResponseType,
	},
	RequestError,
};
use futures::TryStreamExt;
use js::{bind, function::Opt, prelude::*, Class, Ctx, Exception, Persistent, Result, Value};
use reqwest::{
	header::{HeaderValue, CONTENT_TYPE},
	redirect, Body as ReqBody,
};
use std::sync::Arc;

#[bind(object, public)]
#[allow(unused_variables)]
pub async fn fetch<'js>(
	ctx: Ctx<'js>,
	input: Value<'js>,
	init: Opt<RequestInit>,
	args: Rest<()>,
) -> Result<ResponseClass> {
	// Create a request from the input.
	let js_req = RequestClass::new(ctx, input, init, args)?;

	let url = js_req.url;

	let req = reqwest::Request::new(js_req.init.method, url.clone());

	// SurrealDB Implementation keeps all javascript parts inside the context::with scope so this
	// unwrap should never panic.
	let headers = js_req.init.headers.restore(ctx).unwrap();
	let headers = headers.borrow();
	let mut headers = headers.inner.borrow().clone();

	let redirect = js_req.init.request_redirect;

	// set the policy for redirecting requests.
	let policy = redirect::Policy::custom(move |attempt| {
		match redirect {
			classes::RequestRedirect::Follow => {
				// Fetch spec limits redirect to a max of 20
				if attempt.previous().len() > 20 {
					attempt.error("too many redirects")
				} else {
					attempt.follow()
				}
			}
			classes::RequestRedirect::Error => attempt.error("unexpected redirect"),
			classes::RequestRedirect::Manual => attempt.stop(),
		}
	});

	let client = reqwest::Client::builder().redirect(policy).build().map_err(|e| {
		Exception::throw_internal(ctx, &format!("Could not initialize http client: {e}"))
	})?;

	// Set the body for the request.
	let mut req_builder = reqwest::RequestBuilder::from_parts(client, req);
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
			BodyData::Used => return Err(Exception::throw_type(ctx, "Body unusable")),
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
		.map_err(|e| Exception::throw_type(ctx, &e.to_string()))?;

	// Extract the headers
	let headers = HeadersClass::from_map(response.headers().clone());
	let headers = Class::instance(ctx, headers)?;
	let headers = Persistent::save(ctx, headers);
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
	let response = ResponseClass {
		body,
		init,
		url: Some(url),
		r#type: ResponseType::Default,
		was_redirected: false,
	};
	Ok(response)
}
