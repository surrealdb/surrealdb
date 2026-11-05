use std::collections::BTreeMap;

use anyhow::Result;
use http::{HeaderName, HeaderValue, StatusCode};

use crate::api::err::ApiError;
use crate::api::middleware::common::BodyStrategy;
use crate::api::middleware::res::{convert_response_value, output_body_strategy};
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::{FromPublic, Optional};
use crate::val::{Closure, Value};

/// Middleware function that serializes the response body according to the specified strategy.
///
/// This middleware handles response body serialization and sets the appropriate `Content-Type`
/// header. It also performs content negotiation based on the `Accept` header in the request.
///
/// # Arguments
/// * `req` - The API request object (used for Accept header negotiation)
/// * `next` - The next middleware or handler in the chain
/// * `strategy` - Optional serialization strategy. If not provided, defaults to `Auto`:
///   - `Auto`: Negotiates based on `Accept` header (supports all formats)
///   - `Json`: Always serialize as JSON
///   - `Cbor`: Always serialize as CBOR
///   - `Flatbuffers`: Always serialize as Flatbuffers
///   - `Plain`: Always serialize as plain text
///   - `Bytes`: Always serialize as raw bytes
///   - `Native`: Keep as native SurrealDB format
///
/// # Returns
/// * `Ok(response)` - The response with serialized body and Content-Type header
/// * `Err(e)` - Error if serialization fails or no acceptable format found
///
/// # Example
/// ```surql
/// DEFINE API "/data"
///     FOR get
///         MIDDLEWARE
///             api::res::body("json")
///         THEN {
///             RETURN { status: 200, body: { message: "Hello" } };
///         };
/// ```
pub async fn body(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	let res =
		crate::exec::closure::invoke_in(&next, ctx, opt, doc, vec![req.clone().into()]).await?;
	encode_body(&req, res, strategy.map(|x| x.0))
}

/// `api::res::body`'s work on the response `res` to `req`: its body encoded
/// by `strategy`, or by what the request accepts when none is named.
pub(crate) fn encode_body(
	req: &ApiRequest,
	res: Value,
	strategy: Option<BodyStrategy>,
) -> Result<Value> {
	let mut res: ApiResponse = res.try_into()?;
	let strategy = strategy.unwrap_or_default();
	let Some(strategy) = output_body_strategy(&req.headers, strategy) else {
		return Err(ApiError::NoOutputStrategy.into());
	};
	convert_response_value(&mut res, strategy)?;
	Ok(res.into())
}

/// Middleware function that sets the HTTP status code of the response.
///
/// # Arguments
/// * `req` - The API request object
/// * `next` - The next middleware or handler in the chain
/// * `status` - The HTTP status code (must be between 100 and 599)
///
/// # Returns
/// * `Ok(response)` - The response with the specified status code
/// * `Err(e)` - Error if the status code is invalid
///
/// # Example
/// ```surql
/// DEFINE API "/not-found"
///     FOR get
///         MIDDLEWARE
///             api::res::status(404)
///         THEN {
///             RETURN { body: { error: "Not found" } };
///         };
/// ```
pub async fn status(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, status): (Value, Box<Closure>, i64),
) -> Result<Value> {
	let res = crate::exec::closure::invoke_in(&next, ctx, opt, doc, vec![req]).await?;
	set_status(res, status)
}

/// `api::res::status`'s work on the response `res`: its status set to
/// `status`, which must be an HTTP status code.
pub(crate) fn set_status(res: Value, status: i64) -> Result<Value> {
	let mut res: ApiResponse = res.try_into()?;

	// Validate status code: must be a valid u16 and a valid HTTP status code (100-599)
	let status = u16::try_from(status)
		.ok()
		.and_then(|s| StatusCode::from_u16(s).ok())
		.ok_or(ApiError::InvalidStatusCode(status))?;

	res.status = status;
	Ok(res.into())
}

/// Middleware function that sets or removes a single response header.
///
/// # Arguments
/// * `req` - The API request object
/// * `next` - The next middleware or handler in the chain
/// * `name` - The header name (must be a valid HTTP header name)
/// * `value` - Optional header value. If `None`, the header is removed.
///
/// # Returns
/// * `Ok(response)` - The response with the header set or removed
/// * `Err(e)` - Error if the header name or value is invalid
///
/// # Example
/// ```surql
/// DEFINE API "/custom-header"
///     FOR get
///         MIDDLEWARE
///             api::res::header("X-Custom", "value")
///         THEN {
///             RETURN { status: 200, body: {} };
///         };
/// ```
pub async fn header(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, name, Optional(value)): (Value, Box<Closure>, String, Optional<String>),
) -> Result<Value> {
	let res = crate::exec::closure::invoke_in(&next, ctx, opt, doc, vec![req]).await?;
	set_header(res, &name, value)
}

/// `api::res::header`'s work on the response `res`: header `name` set to
/// `value`, or removed when there is none.
pub(crate) fn set_header(res: Value, name: &str, value: Option<String>) -> Result<Value> {
	let mut res: ApiResponse = res.try_into()?;

	let name: HeaderName =
		name.parse().map_err(|e| ApiError::InvalidHeaderName(format!("{}: {}", name, e)))?;
	if let Some(value) = value {
		let value: HeaderValue = value.parse().map_err(|e| ApiError::InvalidHeaderValue {
			name: name.to_string(),
			value: format!("{}: {}", value, e),
		})?;
		res.headers.insert(name, value)
	} else {
		res.headers.remove(name)
	};

	Ok(res.into())
}

/// Middleware function that sets or removes multiple response headers at once.
///
/// This is more efficient than calling `api::res::header` multiple times as it batches
/// the header operations.
///
/// # Arguments
/// * `req` - The API request object
/// * `next` - The next middleware or handler in the chain
/// * `headers` - A map of header names to optional values:
///   - If value is `Some(string)`, the header is set to that value
///   - If value is `None`, the header is removed
///
/// # Returns
/// * `Ok(response)` - The response with headers set or removed
/// * `Err(e)` - Error if any header name or value is invalid
///
/// # Example
/// ```surql
/// DEFINE API "/multiple-headers"
///     FOR get
///         MIDDLEWARE
///             api::res::headers({
///                 "X-Header1": "value1",
///                 "X-Header2": "value2",
///                 "X-Old-Header": NONE
///             })
///         THEN {
///             RETURN { status: 200, body: {} };
///         };
/// ```
pub async fn headers(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, headers): (Value, Box<Closure>, BTreeMap<String, Option<String>>),
) -> Result<Value> {
	let res = crate::exec::closure::invoke_in(&next, ctx, opt, doc, vec![req]).await?;
	set_headers(res, headers)
}

/// `api::res::headers`'s work on the response `res`: each header named set to
/// its value, or removed where it has none.
pub(crate) fn set_headers(res: Value, headers: BTreeMap<String, Option<String>>) -> Result<Value> {
	let mut res: ApiResponse = res.try_into()?;

	for (k, value) in headers {
		let name: HeaderName =
			k.parse().map_err(|e| ApiError::InvalidHeaderName(format!("{}: {}", k, e)))?;

		if let Some(value) = value {
			let value: HeaderValue = value.parse().map_err(|e| ApiError::InvalidHeaderValue {
				name: k.clone(),
				value: format!("{}: {}", value, e),
			})?;

			res.headers.insert(name, value);
		} else {
			res.headers.remove(name);
		}
	}

	Ok(res.into())
}
