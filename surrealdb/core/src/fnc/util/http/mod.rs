use anyhow::{Context as _, Result, bail};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, RequestBuilder, Response};
use url::Url;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::types::{PublicBytes, PublicValue};
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::{Object, Value};

pub(crate) fn uri_is_valid(uri: &str) -> bool {
	reqwest::Url::parse(uri).is_ok()
}

fn encode_body(req: RequestBuilder, body: PublicValue) -> RequestBuilder {
	match body {
		PublicValue::Bytes(v) => req.body(v.into_inner()),
		PublicValue::String(v) => req.body(v),
		PublicValue::None | PublicValue::Null => req,
		PublicValue::Bool(_)
		| PublicValue::Number(_)
		| PublicValue::Duration(_)
		| PublicValue::Datetime(_)
		| PublicValue::Date(_)
		| PublicValue::Time(_)
		| PublicValue::Timestamp(_)
		| PublicValue::Interval(_)
		| PublicValue::Uuid(_)
		| PublicValue::Geometry(_)
		| PublicValue::Table(_)
		| PublicValue::RecordId(_)
		| PublicValue::File(_)
		| PublicValue::Range(_)
		| PublicValue::Regex(_)
		| PublicValue::Array(_)
		| PublicValue::Object(_)
		| PublicValue::Set(_) => req.json(&body.into_json_value()),
	}
}

/// Maximum HTTP response body size (64 MiB) accepted by SurrealQL `http::*`
/// functions, enforced incrementally as chunks arrive so an oversized response
/// is rejected before being buffered in full.
pub(crate) const MAX_HTTP_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

/// Reads an HTTP response body into memory while enforcing `max_bytes`.
pub(crate) async fn read_response_bytes(
	#[allow(unused_mut)] mut res: Response,
	max_bytes: u64,
) -> Result<bytes::Bytes> {
	if let Some(len) = res.content_length()
		&& len > max_bytes
	{
		bail!(Error::Http(format!(
			"HTTP response body of {len} bytes exceeds the {max_bytes} byte limit"
		)));
	}

	#[cfg(not(target_family = "wasm"))]
	{
		let mut body = bytes::BytesMut::with_capacity(
			res.content_length().unwrap_or(0).min(max_bytes) as usize,
		);
		while let Some(chunk) = res.chunk().await.map_err(Error::from)? {
			if (body.len() as u64).saturating_add(chunk.len() as u64) > max_bytes {
				bail!(Error::Http(format!(
					"HTTP response body exceeds the {max_bytes} byte limit"
				)));
			}
			body.extend_from_slice(&chunk);
		}
		Ok(body.freeze())
	}

	#[cfg(target_family = "wasm")]
	{
		let bytes = res.bytes().await.map_err(Error::from)?;
		if (bytes.len() as u64) > max_bytes {
			bail!(Error::Http(format!(
				"HTTP response body of {} bytes exceeds the {max_bytes} byte limit",
				bytes.len()
			)));
		}
		Ok(bytes)
	}
}

async fn decode_response(res: Response) -> Result<PublicValue> {
	match res.error_for_status() {
		Ok(res) => match res.headers().get(CONTENT_TYPE) {
			Some(mime) => match mime.to_str() {
				Ok(v) if v.starts_with("application/json") => {
					let bytes = read_response_bytes(res, MAX_HTTP_RESPONSE_BYTES).await?;
					let json: serde_json::Value = serde_json::from_slice(&bytes)
						.context("Failed to parse JSON response")
						.map_err(|e| Error::Http(e.to_string()))?;
					Ok(crate::rpc::format::json::json_to_value(json))
				}
				Ok(v) if v.starts_with("application/octet-stream") => {
					let bytes = read_response_bytes(res, MAX_HTTP_RESPONSE_BYTES).await?;
					Ok(PublicValue::Bytes(PublicBytes::from(bytes)))
				}
				Ok(v) if v.starts_with("text") => {
					let bytes = read_response_bytes(res, MAX_HTTP_RESPONSE_BYTES).await?;
					let txt = String::from_utf8_lossy(&bytes).into_owned();
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
	opts: Object,
) -> Result<Value> {
	// On browser WASM the reqwest futures and response held across the awaits
	// below are not `Send`; the wrapper asserts `Send` for the whole block.
	common::future::assert_send(async move {
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
		let mut req = cli.request(method, url);
		// Add specified header values
		for (k, v) in opts.iter() {
			req = req.header(k.as_str(), v.to_raw_string());
		}

		if let Some(b) = body {
			// Submit the request body
			req = encode_body(req, b);
		}

		// Send the request and wait
		let res = match ctx.timeout() {
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
	})
	.await
}

pub async fn head(ctx: &FrozenContext, uri: String, opts: Object) -> Result<Value> {
	request(ctx, Method::HEAD, uri, None, opts).await
}

pub async fn get(ctx: &FrozenContext, uri: String, opts: Object) -> Result<Value> {
	request(ctx, Method::GET, uri, None, opts).await
}

pub async fn put(ctx: &FrozenContext, uri: String, body: Value, opts: Object) -> Result<Value> {
	request(ctx, Method::PUT, uri, Some(body), opts).await
}

pub async fn post(ctx: &FrozenContext, uri: String, body: Value, opts: Object) -> Result<Value> {
	request(ctx, Method::POST, uri, Some(body), opts).await
}

pub async fn patch(ctx: &FrozenContext, uri: String, body: Value, opts: Object) -> Result<Value> {
	request(ctx, Method::PATCH, uri, Some(body), opts).await
}

pub async fn delete(ctx: &FrozenContext, uri: String, opts: Object) -> Result<Value> {
	request(ctx, Method::DELETE, uri, None, opts).await
}
