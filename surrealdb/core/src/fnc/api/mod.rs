use anyhow::{Result, bail};
use http::header::{ACCEPT, CONTENT_TYPE};
use reblessive::tree::Stk;

use super::args::Optional;
use crate::api::format as api_format;
use crate::api::invocation::process_api_request_with_stack;
use crate::api::request::ApiRequest;
use crate::catalog::ApiDefinition;
use crate::catalog::providers::ApiProvider;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::FromPublic;
use crate::val::{Closure, Duration, Value};

pub mod req;
pub mod res;

/// Invokes an API endpoint programmatically from within SurrealQL.
///
/// This function allows you to call a defined API endpoint from within SurrealQL code,
/// useful for testing, chaining APIs, or internal API-to-API communication.
///
/// The function:
/// 1. Parses the path and finds the matching API definition
/// 2. Merges the provided request with defaults (Content-Type, Accept headers)
/// 3. Processes the request through the middleware chain
/// 4. Returns the response value
///
/// # Arguments
/// * `path` - The API path to invoke (e.g., "/users/:id")
/// * `req` - Optional request object. If not provided, defaults are used:
///   - `method`: defaults to GET
///   - `headers`: Content-Type and Accept headers are set to native format
///   - `body`: defaults to None
///   - `params`: extracted from path matching
///
/// # Returns
/// * `Ok(response)` - The API response as a SurrealQL value
/// * `Ok(None)` - No matching API definition found
/// * `Err(e)` - Error during processing
///
/// # Example
/// ```surql
/// // Invoke an API endpoint
/// api::invoke("/users/123", {
///     method: "get"
/// });
///
/// // Invoke with a POST request
/// api::invoke("/users", {
///     method: "post",
///     body: { name: "John", age: 30 }
/// });
/// ```
pub async fn invoke(
	(stk, ctx, opt): (&mut Stk, &FrozenContext, &Options),
	(path, Optional(req)): (String, Optional<FromPublic<ApiRequest>>),
) -> Result<Value> {
	let mut req = req.map(|x| x.0).unwrap_or_default();
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let apis = ctx.tx().all_db_apis(ns, db).await?;

	if !path.starts_with('/') {
		// align behaviour with the path provided in DEFINE API statement
		bail!("The string could not be parsed into a path: Segment should start with /");
	}

	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	if !req.headers.contains_key(CONTENT_TYPE) {
		req.headers.insert(CONTENT_TYPE, api_format::NATIVE.try_into()?);
	}

	if !req.headers.contains_key(ACCEPT) {
		// We'll accept anything, but we prefer native.
		req.headers.insert(ACCEPT, "application/vnd.surrealdb.native;q=0.9, */*;q=0.8".try_into()?);
	}

	if let Some((api, params)) = ApiDefinition::find_definition(&apis, segments, req.method) {
		req.params = params.try_into()?;
		match process_api_request_with_stack(stk, ctx, opt, api, req).await {
			Ok(Some(v)) => Ok(v.into()),
			Err(e) => Err(e),
			_ => Ok(Value::None),
		}
	} else {
		Ok(Value::None)
	}
}

/// Middleware function that sets a timeout for API request processing.
///
/// This middleware adds a timeout to the request context, causing the request
/// to fail if it takes longer than the specified duration to complete.
///
/// # Arguments
/// * `req` - The API request object
/// * `next` - The next middleware or handler in the chain
/// * `timeout` - The maximum duration allowed for request processing
///
/// # Returns
/// * `Ok(response)` - The response from the next middleware/handler
/// * `Err(e)` - Error if the request times out or processing fails
///
/// # Example
/// ```surql
/// DEFINE API "/slow-operation"
///     FOR get
///         MIDDLEWARE
///             api::timeout(5s)
///         THEN {
///             // This operation will timeout after 5 seconds
///             RETURN { status: 200, body: "Done" };
///         };
/// ```
pub async fn timeout(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(req, next, timeout): (Value, Box<Closure>, Duration),
) -> Result<Value> {
	let mut ctx = Context::new_isolated(ctx);
	ctx.add_timeout(*timeout)?;
	let ctx = &ctx.freeze();

	next.invoke(stk, ctx, opt, doc, vec![req]).await
}
