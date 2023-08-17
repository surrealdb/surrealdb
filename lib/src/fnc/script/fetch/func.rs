//! Contains the actual fetch function.

use crate::{
	fnc::script::fetch::classes::{self, Request, RequestInit, Response},
	http::{RedirectAction, RedirectPolicy},
};
use js::{function::Opt, Class, Ctx, Exception, Result, Value};

use super::{
	body::Body,
	classes::{Headers, ResponseType},
};

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

	// Check if the url is allowed to be fetched.
	if ctx.globals().contains_key(QUERY_DATA_PROP_NAME)? {
		let query_ctx =
			ctx.globals().get::<_, OwnedBorrow<'js, QueryContext<'js>>>(QUERY_DATA_PROP_NAME)?;
		query_ctx
			.context
			.check_allowed_net(&url)
			.map_err(|e| Exception::throw_message(&ctx, &e.to_string()))?;
	} else {
		#[cfg(debug_assertions)]
		panic!("Trying to fetch a URL but no QueryContext is present. QueryContext is required for checking if the URL is allowed to be fetched.")
	}

	let req = reqwest::Request::new(js_req.init.method, url.clone());

	// SurrealDB Implementation keeps all javascript parts inside the context::with scope so this
	// unwrap should never panic.
	let headers = js_req.headers;
	let headers = headers.borrow();
	let headers = headers.inner.clone();

	let redirect = js_req.request_redirect;

	// set the policy for redirecting requests.
	let policy = RedirectPolicy::custom(move |attempt| {
		match redirect {
			classes::RequestRedirect::Follow => {
				// Fetch spec limits redirect to a max of 20
				if attempt.previous().len() > 20 {
					RedirectAction::error("too many redirects")
				} else {
					RedirectAction::Follow
				}
			}
			classes::RequestRedirect::Error => RedirectAction::error("unexpected redirect"),
			classes::RequestRedirect::Manual => RedirectAction::Stop,
		}
	});

	let client = crate::http::Client::builder().redirect(policy).build().map_err(|e| {
		Exception::throw_internal(&ctx, &format!("Could not initialize http client: {e}"))
	})?;

	// method and url already parsed object so this should never panic.
	let req = crate::http::Request::new(js_req.method, url.clone(), client.clone()).unwrap();

	// make the request
	let response = req
		.headers(headers)
		.body(js_req.body.into_backend_body())
		.send()
		.await
		.map_err(|e| Exception::throw_type(&ctx, &e.to_string()))?;

	let status = response.status().as_u16();
	let status_text = response.status().canonical_reason().unwrap_or_default().to_owned();

	// Extract the headers
	let headers = Headers::from_map(response.headers().clone());
	let headers = Class::instance(ctx, headers)?;

	// Extract the body
	let body = Body::stream(response.bytes_stream());
	let response = Response {
		body,
		headers,
		status,
		status_text,
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
