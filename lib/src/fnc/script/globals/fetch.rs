use crate::fnc::script::classes::request::request::Request;
use crate::fnc::script::classes::request::RequestInit;
use crate::fnc::script::classes::response::response::Response;
use js::function::Opt;
use js::prelude::Rest;
use js::Class;
use js::Result;

#[js::bind(object, public)]
#[quickjs(rename = "fetch")]
#[allow(unused_variables)]
pub async fn fetch<'js>(
	ctx: js::Ctx<'js>,
	input: js::Value<'js>,
	init: Opt<RequestInit>,
	args: Rest<()>,
) -> Result<Class<'js, Response>> {
	fetch_impl(ctx, input, init, args).await
}

#[cfg(not(feature = "http"))]
async fn fetch_impl<'js>(
	ctx: js::Ctx<'js>,
	_input: js::Value<'js>,
	_init: Opt<RequestInit>,
	_args: Rest<()>,
) -> Result<Class<'js, Response>> {
	Err(js::Exception::from_message(
		ctx,
		"`fetch` is only available when surrealdb is built with the `http` feature enabled",
	)
	.throw())
}

#[cfg(feature = "http")]
async fn fetch_impl<'js>(
	ctx: js::Ctx<'js>,
	input: js::Value<'js>,
	init: Opt<RequestInit>,
	args: Rest<()>,
) -> Result<Class<'js, Response>> {
	use js::Persistent;

	use crate::fnc::script::classes::{
		headers::headers::Headers,
		response::{BodyInit, ResponseInit},
	};
	use std::{cell::RefCell, rc::Rc};

	let js_req = Request::new(ctx, input, init, args)?;

	let client = reqwest::Client::new();
	let url = js_req.url;

	let req = reqwest::Request::new(js_req.init.method, url.clone());

	// Can't be from a different runtime, so should be fine.
	let headers = js_req.init.headers.restore(ctx).unwrap();
	let headers = headers.as_class_def().inner.borrow().clone();

	let response = reqwest::RequestBuilder::from_parts(client, req)
		.headers(headers)
		.send()
		.await
		.map_err(|e| throw!(ctx, e))?;

	let headers = Headers::from_map(response.headers().clone());
	let headers = Class::instance(ctx, headers)?;
	let headers = Persistent::save(ctx, headers);
	let init = ResponseInit {
		headers,
		status: response.status(),
		status_text: response.status().canonical_reason().unwrap_or("").to_owned(),
	};

	let body = BodyInit::Stream(Rc::new(RefCell::new(Some(response))));
	let response = Response::new_inner(Some(url), Some(body), init);
	Class::instance(ctx, response)
}
