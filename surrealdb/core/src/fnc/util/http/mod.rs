use anyhow::{Context as _, Result, bail};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, RequestBuilder, Response};
use url::Url;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::{PublicBytes, PublicValue};
use crate::val::{Object, Value};

pub(crate) fn uri_is_valid(uri: &str) -> bool {
	reqwest::Url::parse(uri).is_ok()
}

fn encode_body(req: RequestBuilder, body: PublicValue) -> Result<RequestBuilder> {
	let res = match body {
		PublicValue::Bytes(v) => req.body(v.into_inner()),
		PublicValue::String(v) => req.body(v),
		//TODO: Improve the handling here. We should check if this value can be send as a json
		//value.
		_ if !body.is_nullish() => req.json(&body.into_json_value()),
		_ => req,
	};
	Ok(res)
}

async fn decode_response(res: Response) -> Result<PublicValue> {
	match res.error_for_status() {
		Ok(res) => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let txt = res.text().await.map_err(Error::from)?;
					let json: serde_json::Value = serde_json::from_str(&txt)
						.context("Failed to parse JSON response")
						.map_err(|e| Error::Http(e.to_string()))?;
					Ok(crate::rpc::format::json::json_to_value(json))
				}
				Ok(v) if v.starts_with("application/octet-stream") => {
					let bytes = res.bytes().await.map_err(Error::from)?;
					Ok(PublicValue::Bytes(PublicBytes::from(bytes)))
				}
				Ok(v) if v.starts_with("text") => {
					let txt = res.text().await.map_err(Error::from)?;
					let val = PublicValue::String(txt);
					Ok(val)
				}
				_ => Ok(PublicValue::None),
			},
			_ => Ok(PublicValue::None),
		},
		Err(err) => match err.status() {
			Some(s) => bail!(Error::Http(format!(
				"{} {}",
				s.as_u16(),
				s.canonical_reason().unwrap_or_default(),
			))),
			None => bail!(Error::Http(err.to_string())),
		},
	}
}

async fn request(
	ctx: &FrozenContext,
	method: Method,
	uri: String,
	body: Option<Value>,
	opts: impl Into<Object>,
) -> Result<Value> {
	// Check if the URI is valid and allowed
	let url = Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	let body = match body {
		Some(v) => Some(crate::val::convert_value_to_public_value(v)?),
		None => None,
	};

	let is_head = matches!(method, Method::HEAD);

	let cli = ctx.http_client();
	// Start a new HTTP request using the shared client
	let mut req = cli.request(method.clone(), url);
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}

	if let Some(b) = body {
		// Submit the request body
		req = encode_body(req, b)?;
	}

	// Send the request and wait
	let res = match ctx.timeout() {
		#[cfg(not(target_family = "wasm"))]
		Some(d) => req.timeout(d).send().await.map_err(Error::from)?,
		_ => req.send().await.map_err(Error::from)?,
	};

	if is_head {
		// Check the response status
		match res.error_for_status() {
			Ok(_) => Ok(Value::None),
			Err(err) => match err.status() {
				Some(s) => bail!(Error::Http(format!(
					"{} {}",
					s.as_u16(),
					s.canonical_reason().unwrap_or_default(),
				))),
				None => bail!(Error::Http(err.to_string())),
			},
		}
	} else {
		// Receive the response as a value
		let val = decode_response(res).await?;
		Ok(convert_public_value_to_internal(val))
	}
}

pub async fn head(ctx: &FrozenContext, uri: String, opts: impl Into<Object>) -> Result<Value> {
	request(ctx, Method::HEAD, uri, None, opts).await
}

pub async fn get(ctx: &FrozenContext, uri: String, opts: impl Into<Object>) -> Result<Value> {
	request(ctx, Method::GET, uri, None, opts).await
}

pub async fn put(
	ctx: &FrozenContext,
	uri: String,
	body: Value,
	opts: impl Into<Object>,
) -> Result<Value> {
	request(ctx, Method::PUT, uri, Some(body), opts).await
}

pub async fn post(
	ctx: &FrozenContext,
	uri: String,
	body: Value,
	opts: impl Into<Object>,
) -> Result<Value> {
	request(ctx, Method::POST, uri, Some(body), opts).await
}

pub async fn patch(
	ctx: &FrozenContext,
	uri: String,
	body: Value,
	opts: impl Into<Object>,
) -> Result<Value> {
	request(ctx, Method::PATCH, uri, Some(body), opts).await
}

pub async fn delete(ctx: &FrozenContext, uri: String, opts: impl Into<Object>) -> Result<Value> {
	request(ctx, Method::DELETE, uri, None, opts).await
}
