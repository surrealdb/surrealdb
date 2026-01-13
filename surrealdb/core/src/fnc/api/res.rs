use std::collections::BTreeMap;

use anyhow::Result;
use http::header::CONTENT_TYPE;
use http::{HeaderName, HeaderValue, StatusCode};
use reblessive::tree::Stk;
use surrealdb_types::SurrealValue;

use crate::api::err::ApiError;
use crate::api::format as api_format;
use crate::api::middleware::common::BodyStrategy;
use crate::api::middleware::res::output_body_strategy;
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::{FromPublic, Optional};
use crate::rpc::format;
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::PublicBytes;
use crate::val::{Bytes, Closure, Value};

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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req.clone().into()]).await?;
	let mut res: ApiResponse = res.try_into()?;

	let strategy = strategy.map(|x| x.0).unwrap_or_default();
	let Some(strategy) = output_body_strategy(&req.headers, strategy) else {
		return Err(ApiError::NoOutputStrategy.into());
	};

	match strategy {
		BodyStrategy::Auto | BodyStrategy::Json => {
			res.body = PublicBytes::from(
				format::json::encode(res.body).map_err(|_| ApiError::BodyEncodeFailure)?,
			)
			.into_value();
			res.headers.insert(CONTENT_TYPE, api_format::JSON.try_into()?);
		}
		BodyStrategy::Cbor => {
			res.body = PublicBytes::from(
				format::cbor::encode(res.body).map_err(|_| ApiError::BodyEncodeFailure)?,
			)
			.into_value();
			res.headers.insert(CONTENT_TYPE, api_format::CBOR.try_into()?);
		}
		BodyStrategy::Flatbuffers => {
			res.body = PublicBytes::from(
				format::flatbuffers::encode(&res.body).map_err(|_| ApiError::BodyEncodeFailure)?,
			)
			.into_value();
			res.headers.insert(CONTENT_TYPE, api_format::FLATBUFFERS.try_into()?);
		}
		BodyStrategy::Bytes => {
			res.body = PublicBytes::from(
				convert_public_value_to_internal(res.body)
					.cast_to::<Bytes>()
					.map_err(|_| ApiError::BodyEncodeFailure)?
					.0,
			)
			.into_value();
			res.headers.insert(CONTENT_TYPE, api_format::OCTET_STREAM.try_into()?);
		}
		BodyStrategy::Plain => {
			let text = convert_public_value_to_internal(res.body)
				.cast_to::<String>()
				.map_err(|_| ApiError::BodyEncodeFailure)?;
			res.body = PublicBytes::from(text.into_bytes()).into_value();
			res.headers.insert(CONTENT_TYPE, api_format::PLAIN.try_into()?);
		}
		BodyStrategy::Native => {
			res.headers.insert(CONTENT_TYPE, api_format::NATIVE.try_into()?);
		}
	}

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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, status): (Value, Box<Closure>, i64),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req]).await?;
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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, name, Optional(value)): (Value, Box<Closure>, String, Optional<String>),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req]).await?;
	let mut res: ApiResponse = res.try_into()?;

	let name: HeaderName =
		name.parse().map_err(|e| ApiError::InvalidHeaderName(format!("{}: {}", name, e)))?;
	if let Some(value) = value {
		// Validate header value doesn't contain CRLF (header injection prevention)
		if value.contains("\r\n") || value.contains('\n') {
			return Err(ApiError::HeaderInjectionAttempt(value).into());
		}
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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, headers): (Value, Box<Closure>, BTreeMap<String, Option<String>>),
) -> Result<Value> {
	let res = next.invoke(stk, ctx, opt, doc, vec![req]).await?;
	let mut res: ApiResponse = res.try_into()?;

	// Batch header operations for better performance
	let mut to_insert = Vec::new();
	let mut to_remove = Vec::new();

	for (k, value) in headers {
		let name: HeaderName =
			k.parse().map_err(|e| ApiError::InvalidHeaderName(format!("{}: {}", k, e)))?;
		if let Some(value) = value {
			// Validate header value doesn't contain CRLF (header injection prevention)
			if value.contains("\r\n") || value.contains('\n') {
				return Err(ApiError::HeaderInjectionAttempt(value).into());
			}
			let value: HeaderValue = value.parse().map_err(|e| ApiError::InvalidHeaderValue {
				name: k.clone(),
				value: format!("{}: {}", value, e),
			})?;
			to_insert.push((name, value));
		} else {
			to_remove.push(name);
		}
	}

	// Apply batched operations
	for (name, value) in to_insert {
		res.headers.insert(name, value);
	}
	for name in to_remove {
		res.headers.remove(name);
	}

	Ok(res.into())
}
