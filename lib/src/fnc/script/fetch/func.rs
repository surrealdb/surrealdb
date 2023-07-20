//! Contains the actual fetch function.

use crate::fnc::script::fetch::{
	body::{Body, BodyData, BodyKind},
	classes::{self, Request, RequestInit, Response, ResponseInit, ResponseType},
	RequestError,
};
use futures::TryStreamExt;
use js::{function::Opt, Class, Ctx, Exception, Result, Value};
use reqwest::{
	header::{HeaderValue, CONTENT_TYPE},
	redirect, Body as ReqBody,
};
use std::sync::Arc;

use super::classes::Headers;

#[js::function]
#[allow(unused_variables)]
pub async fn fetch<'js>(
	ctx: Ctx<'js>,
	input: Value<'js>,
	init: Opt<RequestInit<'js>>,
) -> Result<Response<'js>> {
	// Create a request from the input.
	let js_req = Request::new(ctx.clone(), input, init)?;

	let url = js_req.url;

	let req = reqwest::Request::new(js_req.init.method, url.clone());

	// SurrealDB Implementation keeps all javascript parts inside the context::with scope so this
	// unwrap should never panic.
	let headers = js_req.init.headers;
	let headers = headers.borrow();
	let mut headers = headers.inner.clone();

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
		Exception::throw_internal(&ctx, &format!("Could not initialize http client: {e}"))
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

#[cfg(test)]
mod test {
	use crate::fnc::script::fetch::test::create_test_context;

	#[tokio::test]
	async fn test_fetch_get() {
		use js::{promise::Promise, CatchResultExt};
		use wiremock::{
			matchers::{header, method, path},
			Mock, MockServer, ResponseTemplate,
		};

		let server = MockServer::start().await;

		Mock::given(method("GET"))
			.and(path("/hello"))
			.and(header("some-header", "some-value"))
			.respond_with(ResponseTemplate::new(200).set_body_string("some body once told me"))
			.expect(1)
			.mount(&server)
			.await;

		let server_ref = &server;

		create_test_context!(ctx => {
			ctx.globals().set("SERVER_URL",server_ref.uri()).unwrap();

			ctx.eval::<Promise<()>,_>(r#"
				(async () => {
					let res = await fetch(SERVER_URL + '/hello',{
                        headers: {
                            "some-header": "some-value",
                        }
                    });
					assert.seq(res.status,200);
					let body = await res.text();
					assert.seq(body,'some body once told me');
				})()
			"#).catch(&ctx).unwrap().await.catch(&ctx).unwrap()
		})
		.await;

		server.verify().await;
	}

	#[tokio::test]
	async fn test_fetch_put() {
		use js::{promise::Promise, CatchResultExt};
		use wiremock::{
			matchers::{body_string, header, method, path},
			Mock, MockServer, ResponseTemplate,
		};

		let server = MockServer::start().await;

		Mock::given(method("PUT"))
			.and(path("/hello"))
			.and(header("some-header", "some-value"))
			.and(body_string("some text"))
			.respond_with(ResponseTemplate::new(201).set_body_string("some body once told me"))
			.expect(1)
			.mount(&server)
			.await;

		let server_ref = &server;

		create_test_context!(ctx => {
			ctx.globals().set("SERVER_URL",server_ref.uri()).unwrap();

			ctx.eval::<Promise<()>,_>(r#"
				(async () => {
					let res = await fetch(SERVER_URL + '/hello',{
                        method: "PuT",
                        headers: {
                            "some-header": "some-value",
                        },
                        body: "some text",
                    });
					assert.seq(res.status,201);
					assert(res.ok);
					let body = await res.text();
					assert.seq(body,'some body once told me');
				})()
			"#).catch(&ctx).unwrap().await.catch(&ctx).unwrap()
		})
		.await;

		server.verify().await;
	}

	#[tokio::test]
	async fn test_fetch_error() {
		use js::{promise::Promise, CatchResultExt};
		use wiremock::{
			matchers::{body_string, header, method, path},
			Mock, MockServer, ResponseTemplate,
		};

		let server = MockServer::start().await;

		Mock::given(method("PROPPATCH"))
			.and(path("/hello"))
			.and(header("some-header", "some-value"))
			.and(body_string("some text"))
			.respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
				"foo": "bar",
				"baz": 2,
			})))
			.expect(1)
			.mount(&server)
			.await;

		let server_ref = &server;

		create_test_context!(ctx => {
			ctx.globals().set("SERVER_URL",server_ref.uri()).unwrap();

			ctx.eval::<Promise<()>,_>(r#"
				(async () => {
                    let req = new Request(SERVER_URL + '/hello',{
                        method: "PROPPATCH",
                        headers: {
                            "some-header": "some-value",
                        },
                        body: "some text",
                    })
					let res = await fetch(req);
					assert.seq(res.status,500);
					assert(!res.ok);
					let body = await res.json();
					assert(body.foo !== undefined, "body.foo not defined");
					assert(body.baz !== undefined, "body.foo not defined");
					assert.seq(body.foo, "bar");
					assert.seq(body.baz, 2);
				})()
			"#).catch(&ctx).unwrap().await.catch(&ctx).unwrap()
		})
		.await;

		server.verify().await;
	}
}
