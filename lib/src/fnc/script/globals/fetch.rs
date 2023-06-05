use crate::fnc::script::classes::request::request::Request;
use crate::sql::value::Value;
use js::prelude::Rest;
use js::Result;
use reqwest::Method;

#[js::bind(object, public)]
#[quickjs(rename = "fetch")]
#[allow(unused_variables)]
pub async fn fetch<'js>(
	ctx: js::Ctx<'js>,
	input: js::Value<'js>,
	init: Option<js::Object<'js>>,
	args: Rest<()>,
) -> Result<Value> {
	fetch_impl(ctx, input, init, args).await
}

#[cfg(not(feature = "http"))]
async fn fetch_impl<'js>(
	ctx: js::Ctx<'js>,
	_input: js::Value<'js>,
	_init: Option<js::Object<'js>>,
	_args: Rest<()>,
) -> Result<Value> {
	Err(js::Exception::from_message(
		ctx,
		"`fetch` is only available when surrealdb is built with the `http` feature enabled",
	)
	.throw())
}

#[cfg(feature = "http")]
async fn fetch_impl<'js>(
	ctx: js::Ctx<'js>,
	input_: js::Value<'js>,
	init: Option<js::Object<'js>>,
	args: Rest<()>,
) -> Result<Value> {
	let js_req = Request::new(ctx, input_, init, args)?;

	let client = reqwest::Client::new();

	let method = Method::from_bytes(js_req.method.as_bytes())
		.expect("request implementation should prefent invalid method");

	let req = reqwest::Request::new(method, js_req.url);

	let response = reqwest::RequestBuilder::from_parts(client, req).send().await;

	todo!();
}
