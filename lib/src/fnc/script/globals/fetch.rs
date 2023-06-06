use crate::fnc::script::classes::request::request::Request;
use crate::fnc::script::classes::request::RequestInit;
use js::function::Opt;
use js::prelude::Rest;
use js::Result;

#[js::bind(object, public)]
#[quickjs(rename = "fetch")]
#[allow(unused_variables)]
pub async fn fetch<'js>(
	ctx: js::Ctx<'js>,
	input: js::Value<'js>,
	init: Opt<RequestInit>,
	args: Rest<()>,
) -> Result<String> {
	fetch_impl(ctx, input, init, args).await
}

#[cfg(not(feature = "http"))]
async fn fetch_impl<'js>(
	ctx: js::Ctx<'js>,
	_input: js::Value<'js>,
	_init: Opt<RequestInit>,
	_args: Rest<()>,
) -> Result<String> {
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
) -> Result<String> {
	let js_req = Request::new(ctx, input, init, args)?;

	let client = reqwest::Client::new();

	let req = reqwest::Request::new(js_req.init.method, js_req.url);

	// Can't be from a different runtime, so should be fine.
	let headers = js_req.init.headers.restore(ctx).unwrap();

	let response = reqwest::RequestBuilder::from_parts(client, req)
		.headers(headers.as_class_def().inner.borrow().clone())
		.send()
		.await
		.map_err(|e| throw!(ctx, e))?;

	response.text().await.map_err(|e| throw!(ctx, e))
}
