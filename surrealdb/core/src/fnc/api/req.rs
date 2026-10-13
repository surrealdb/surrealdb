use anyhow::Result;
use reblessive::tree::Stk;

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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(mut req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	let strategy = strategy.map(|x| x.0).unwrap_or_default();
	let mut parser =
		BodyParser::new(&mut req, strategy, ctx.config.parser.max_object_parsing_depth as usize);
	parser.process().await?;

	crate::legacy::closure_invoke(&next, stk, ctx, opt, doc, vec![req.into()]).await
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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(req), next, limit): (FromPublic<ApiRequest>, Box<Closure>, Value),
) -> Result<Value> {
	let limit = match limit {
		Value::Number(Number::Int(i)) if i >= 0 => Bytesize::new(i as u64),
		Value::String(ref s) => {
			Bytesize::parse(s.as_str()).map_err(|_| Error::InvalidFunctionArguments {
				name: "api::req::max_body".to_string(),
				message: format!("'{s}' is not a valid byte size (expected e.g. '1mb', '512kb')"),
			})?
		}
		_ => {
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
		_ => None,
	};

	if size.is_some_and(|size| size > limit.0) {
		return Err(ApiError::RequestBodyTooLarge(limit).into());
	}

	crate::legacy::closure_invoke(&next, stk, ctx, opt, doc, vec![req.into()]).await
}
