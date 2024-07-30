use crate::ctx::Context;
use crate::err::Error;
use crate::sql::{Bytes, Object, Strand, Value};
use crate::syn;

use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, RequestBuilder, Response};
use url::Url;

pub(crate) fn uri_is_valid(uri: &str) -> bool {
	reqwest::Url::parse(uri).is_ok()
}

fn encode_body(req: RequestBuilder, body: Value) -> RequestBuilder {
	match body {
		Value::Bytes(bytes) => req.header(CONTENT_TYPE, "application/octet-stream").body(bytes.0),
		_ if body.is_some() => req.json(&body.into_json()),
		_ => req,
	}
}

async fn decode_response(res: Response) -> Result<Value, Error> {
	match res.status() {
		s if s.is_success() => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await?;
					let val = syn::json(&txt)?;
					Ok(val)
				}
				Ok(v) if v.starts_with("application/octet-stream") => {
					let bytes = res.bytes().await?;
					Ok(Value::Bytes(Bytes(bytes.into())))
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await?;
					let val = txt.into();
					Ok(val)
				}
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn head(ctx: &Context<'_>, uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.to_string()))?;
	ctx.check_allowed_net(&url)?;
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new HEAD request
	let mut req = cli.head(url);
	// Disable CORS
	if ctx.get_capabilities().allows_no_cors() {
		req = req.fetch_mode_no_cors();
	}
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}
	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_arch = "wasm32"))]
		Some(d) => req.timeout(d).send().await?,
		_ => req.send().await?,
	};
	// Check the response status
	match res.status() {
		s if s.is_success() => Ok(Value::None),
		s => Err(Error::Http(s.canonical_reason().unwrap_or_default().to_owned())),
	}
}

pub async fn get(ctx: &Context<'_>, uri: Strand, opts: impl Into<Object>) -> Result<Value, Error> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.to_string()))?;
	ctx.check_allowed_net(&url)?;
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.get(url);
	// Disable CORS
	if ctx.get_capabilities().allows_no_cors() {
		req = req.fetch_mode_no_cors();
	}
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}
	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_arch = "wasm32"))]
		Some(d) => req.timeout(d).send().await?,
		_ => req.send().await?,
	};
	// Receive the response as a value
	decode_response(res).await
}

pub async fn put(
	ctx: &Context<'_>,
	uri: Strand,
	body: Value,
	opts: impl Into<Object>,
) -> Result<Value, Error> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.to_string()))?;
	ctx.check_allowed_net(&url)?;
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.put(url);
	// Disable CORS
	if ctx.get_capabilities().allows_no_cors() {
		req = req.fetch_mode_no_cors();
	}
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}
	// Submit the request body
	req = encode_body(req, body);
	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_arch = "wasm32"))]
		Some(d) => req.timeout(d).send().await?,
		_ => req.send().await?,
	};
	// Receive the response as a value
	decode_response(res).await
}

pub async fn post(
	ctx: &Context<'_>,
	uri: Strand,
	body: Value,
	opts: impl Into<Object>,
) -> Result<Value, Error> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.to_string()))?;
	ctx.check_allowed_net(&url)?;
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.post(url);
	// Disable CORS
	if ctx.get_capabilities().allows_no_cors() {
		req = req.fetch_mode_no_cors();
	}
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}
	// Submit the request body
	req = encode_body(req, body);
	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_arch = "wasm32"))]
		Some(d) => req.timeout(d).send().await?,
		_ => req.send().await?,
	};
	// Receive the response as a value
	decode_response(res).await
}

pub async fn patch(
	ctx: &Context<'_>,
	uri: Strand,
	body: Value,
	opts: impl Into<Object>,
) -> Result<Value, Error> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.to_string()))?;
	ctx.check_allowed_net(&url)?;
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.patch(url);
	// Disable CORS
	if ctx.get_capabilities().allows_no_cors() {
		req = req.fetch_mode_no_cors();
	}
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}
	// Submit the request body
	req = encode_body(req, body);
	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_arch = "wasm32"))]
		Some(d) => req.timeout(d).send().await?,
		_ => req.send().await?,
	};
	// Receive the response as a value
	decode_response(res).await
}

pub async fn delete(
	ctx: &Context<'_>,
	uri: Strand,
	opts: impl Into<Object>,
) -> Result<Value, Error> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.to_string()))?;
	ctx.check_allowed_net(&url)?;
	// Set a default client with no timeout
	let cli = Client::builder().build()?;
	// Start a new GET request
	let mut req = cli.delete(url);
	// Disable CORS
	if ctx.get_capabilities().allows_no_cors() {
		req = req.fetch_mode_no_cors();
	}
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}
	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_arch = "wasm32"))]
		Some(d) => req.timeout(d).send().await?,
		_ => req.send().await?,
	};
	// Receive the response as a value
	decode_response(res).await
}
