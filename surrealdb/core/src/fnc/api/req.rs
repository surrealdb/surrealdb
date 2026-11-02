use anyhow::Result;

use crate::api::err::ApiError;
use crate::api::middleware::common::BodyStrategy;
use crate::api::middleware::req::BodyParser;
use crate::api::request::ApiRequest;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Bytesize, Error};
use crate::fnc::args::{FromPublic, Optional};
use crate::types::PublicValue;
use crate::val::{Closure, Number, Value};

/// Middleware function that parses the request body according to the specified strategy.
///
/// This middleware deserializes the request body based on the `Content-Type` header
/// or the explicitly provided strategy. The parsed body replaces the raw bytes in `$request.body`.
///
/// # Arguments
/// * `req` - The API request object (modified in place)
/// * `next` - The next middleware or handler in the chain
/// * `strategy` - Optional deserialization strategy. If not provided, defaults to `Auto`:
///   - `Auto`: Detects format from `Content-Type` header
///   - `Json`: Always parse as JSON
///   - `Cbor`: Always parse as CBOR
///   - `Flatbuffers`: Always parse as Flatbuffers
///   - `Plain`: Always parse as plain text (UTF-8 string)
///   - `Bytes`: Keep as raw bytes (no parsing)
///   - `Native`: Parse as native SurrealDB format
///
/// # Returns
/// * `Ok(response)` - The response from the next middleware/handler
/// * `Err(e)` - Error if body parsing fails or Content-Type is invalid/missing
///
/// # Example
/// ```surql
/// DEFINE API "/users"
///     FOR post
///         MIDDLEWARE
///             api::req::body("json")
///         THEN {
///             // $request.body is now a parsed object, not raw bytes
///             RETURN {
///                 status: 201,
///                 body: { created: $request.body.name }
///             };
///         };
/// ```
pub async fn body(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(mut req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	parse_body(&mut req, strategy.map(|x| x.0), ctx).await?;
	crate::exec::closure::invoke_in(&next, ctx, opt, doc, vec![req.into()]).await
}

/// `api::req::body`'s work on the request: its body parsed by `strategy`, or
/// by its `Content-Type` when none is named.
pub(crate) async fn parse_body(
	req: &mut ApiRequest,
	strategy: Option<BodyStrategy>,
	ctx: &crate::ctx::Context,
) -> Result<()> {
	let strategy = strategy.unwrap_or_default();
	let mut parser =
		BodyParser::new(req, strategy, ctx.config.parser.max_object_parsing_depth as usize);
	parser.process().await
}

/// Middleware function that rejects requests whose raw body exceeds a size limit.
///
/// The limit applies to the request body as received, before any body parsing:
/// place this middleware ahead of `api::req::body` so oversized payloads are
/// rejected without being decoded. Only raw payloads are measured — a `bytes`
/// body by its byte length and a `string` body by its UTF-8 byte length; any
/// other body value (including no body) passes through unchecked.
///
/// # Arguments
/// * `req` - The API request object
/// * `next` - The next middleware or handler in the chain
/// * `limit` - The maximum body size, as a non-negative integer byte count or a byte-size string
///   such as `"1mb"` or `"512kb"`
///
/// # Returns
/// * `Ok(response)` - The response from the next middleware/handler
/// * `Err(e)` - `ApiError::RequestBodyTooLarge` (HTTP 413) if the body exceeds the limit, or an
///   invalid-arguments error if the limit is not a valid size
///
/// # Example
/// ```surql
/// DEFINE API "/upload"
///     FOR post
///         MIDDLEWARE
///             api::req::max_body("1mb"),
///             api::req::body("json")
///         THEN {
///             RETURN { status: 201, body: { received: $request.body } };
///         };
/// ```
pub async fn max_body(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(req), next, limit): (FromPublic<ApiRequest>, Box<Closure>, Value),
) -> Result<Value> {
	check_body_size(&req, &limit)?;
	crate::exec::closure::invoke_in(&next, ctx, opt, doc, vec![req.into()]).await
}

/// `api::req::max_body`'s check: the request's raw body is within `limit`, a
/// non-negative byte count or a byte-size string.
pub(crate) fn check_body_size(req: &ApiRequest, limit: &Value) -> Result<()> {
	let limit = match limit {
		Value::Number(Number::Int(i)) if *i >= 0 => Bytesize::new(*i as u64),
		Value::String(s) => {
			Bytesize::parse(s.as_str()).map_err(|_| Error::InvalidFunctionArguments {
				name: "api::req::max_body".to_string(),
				message: format!("'{s}' is not a valid byte size (expected e.g. '1mb', '512kb')"),
			})?
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => {
			anyhow::bail!(Error::InvalidFunctionArguments {
				name: "api::req::max_body".to_string(),
				message: "Expected a non-negative integer byte count or a byte-size string"
					.to_string(),
			})
		}
	};

	let size = match req.body {
		PublicValue::Bytes(ref b) => Some(b.len() as u64),
		PublicValue::String(ref s) => Some(s.len() as u64),
		PublicValue::None
		| PublicValue::Null
		| PublicValue::Bool(_)
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
		| PublicValue::Set(_) => None,
	};

	if size.is_some_and(|size| size > limit.0) {
		return Err(ApiError::RequestBodyTooLarge(limit).into());
	}
	Ok(())
}
