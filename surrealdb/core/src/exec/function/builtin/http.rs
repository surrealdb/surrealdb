//! HTTP functions for the engine.
//!
//! These provide HTTP client functionality (GET, POST, PUT, PATCH, DELETE, HEAD).
//! Note: HTTP functions require the "http" feature to be enabled.

use anyhow::Result;

use crate::define_async_function;
use crate::exec::EvalContext;
use crate::exec::function::ScalarFunction;
#[cfg(feature = "http")]
use crate::val::Object;
use crate::val::Value;

// =========================================================================
// Helper functions
// =========================================================================

#[cfg(not(feature = "http"))]
async fn http_disabled() -> Result<Value> {
	Err(anyhow::anyhow!(crate::dbs::capabilities::Error::HttpDisabled))
}

#[cfg(feature = "http")]
fn extract_uri(args: &[Value], fn_name: &str) -> Result<String> {
	match args.first() {
		Some(Value::String(s)) => Ok(s.as_str().to_owned()),
		Some(v) => Err(anyhow::anyhow!(crate::expr::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!(
				"The first argument should be a string containing a valid URI, got: {}",
				v.kind_of()
			),
		})),
		None => Err(anyhow::anyhow!(crate::expr::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: "Missing URI argument".to_string(),
		})),
	}
}

#[cfg(feature = "http")]
fn extract_opts(args: &[Value], index: usize, fn_name: &str) -> Result<Object> {
	match args.get(index) {
		Some(Value::Object(o)) => Ok(o.clone()),
		None => Ok(Object::default()),
		Some(v) => Err(anyhow::anyhow!(crate::expr::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!("Options argument should be an object, got: {}", v.kind_of()),
		})),
	}
}

#[cfg(feature = "http")]
fn extract_body(args: &[Value], index: usize) -> Option<Value> {
	args.get(index).cloned()
}

#[derive(Clone, Copy)]
enum HttpVerb {
	Head,
	Get,
	Put,
	Post,
	Patch,
	Delete,
}

#[cfg(feature = "http")]
impl HttpVerb {
	const fn method(self) -> reqwest::Method {
		match self {
			Self::Head => reqwest::Method::HEAD,
			Self::Get => reqwest::Method::GET,
			Self::Put => reqwest::Method::PUT,
			Self::Post => reqwest::Method::POST,
			Self::Patch => reqwest::Method::PATCH,
			Self::Delete => reqwest::Method::DELETE,
		}
	}

	const fn has_body(self) -> bool {
		matches!(self, Self::Put | Self::Post | Self::Patch)
	}
}

#[cfg(feature = "http")]
async fn http_dispatch(
	ctx: &EvalContext<'_>,
	fn_name: &'static str,
	verb: HttpVerb,
	args: Vec<Value>,
) -> Result<Value> {
	let uri = extract_uri(&args, fn_name)?;
	let (body, opts) = if verb.has_body() {
		(extract_body(&args, 1), extract_opts(&args, 2, fn_name)?)
	} else {
		(None, extract_opts(&args, 1, fn_name)?)
	};

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	http_request(ctx, verb.method(), uri, body, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_dispatch(
	_ctx: &EvalContext<'_>,
	_fn_name: &'static str,
	_verb: HttpVerb,
	_args: Vec<Value>,
) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP Request implementation
// =========================================================================

#[cfg(feature = "http")]
#[allow(unused_variables)]
async fn http_request(
	ctx: &EvalContext<'_>,
	method: reqwest::Method,
	uri: String,
	body: Option<Value>,
	opts: Object,
) -> Result<Value> {
	use http::header::CONTENT_TYPE;

	use crate::err::Error;
	use crate::types::{PublicBytes, PublicValue};
	use crate::val::convert_public::convert_public_value_to_internal;

	// On browser WASM the reqwest futures and response held across the awaits
	// below are not `Send`; the wrapper asserts `Send` for the whole block.
	common::future::assert_send(async move {
		let url = url::Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.clone()))?;

		let client = ctx.exec_ctx.root().ctx.http_client();

		let is_head = matches!(method, reqwest::Method::HEAD);

		// Start the request
		let mut req = client.request(method, url);

		// Add custom headers from opts
		for (k, v) in opts.iter() {
			req = req.header(k.as_str(), v.to_raw_string());
		}

		// Add body if present
		if let Some(b) = body {
			let public_body = crate::val::convert_value_to_public_value(b)?;
			req = if let PublicValue::Bytes(v) = public_body {
				req.body(v.into_inner())
			} else if let PublicValue::String(v) = public_body {
				req.body(v)
			} else if !public_body.is_nullish() {
				req.json(&public_body.into_json_value())
			} else {
				req
			};
		}

		// Send the request
		let res = req.send().await.map_err(Error::from)?;

		if is_head {
			// For HEAD, just check status
			match res.error_for_status() {
				Ok(_) => Ok(Value::None),
				Err(err) => match err.status() {
					Some(s) => Err(anyhow::anyhow!(Error::Http(format!(
						"{} {}",
						s.as_u16(),
						s.canonical_reason().unwrap_or_default(),
					)))),
					None => Err(anyhow::anyhow!(Error::Http(err.to_string()))),
				},
			}
		} else {
			// Decode response

			match res.error_for_status() {
				Ok(res) => match res.headers().get(CONTENT_TYPE) {
					Some(mime) => match mime.to_str() {
						Ok(v) if v.starts_with("application/json") => {
							let bytes = crate::fnc::util::http::read_response_bytes(
								res,
								crate::fnc::util::http::MAX_HTTP_RESPONSE_BYTES,
							)
							.await?;
							let json: serde_json::Value = serde_json::from_slice(&bytes)
								.map_err(|e| Error::Http(format!("Failed to parse JSON: {}", e)))?;
							let val = crate::rpc::format::json::json_to_value(json);
							Ok(convert_public_value_to_internal(val))
						}
						Ok(v) if v.starts_with("application/octet-stream") => {
							let bytes = crate::fnc::util::http::read_response_bytes(
								res,
								crate::fnc::util::http::MAX_HTTP_RESPONSE_BYTES,
							)
							.await?;
							Ok(convert_public_value_to_internal(PublicValue::Bytes(
								PublicBytes::from(bytes),
							)))
						}
						Ok(v) if v.starts_with("text") => {
							let bytes = crate::fnc::util::http::read_response_bytes(
								res,
								crate::fnc::util::http::MAX_HTTP_RESPONSE_BYTES,
							)
							.await?;
							let txt = String::from_utf8_lossy(&bytes).into_owned();
							Ok(convert_public_value_to_internal(PublicValue::String(txt)))
						}
						_ => Ok(Value::None),
					},
					_ => Ok(Value::None),
				},
				Err(err) => match err.status() {
					Some(s) => Err(anyhow::anyhow!(Error::Http(format!(
						"{} {}",
						s.as_u16(),
						s.canonical_reason().unwrap_or_default(),
					)))),
					None => Err(anyhow::anyhow!(Error::Http(err.to_string()))),
				},
			}
		}
	})
	.await
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_async_function!(HttpHead, "http::head", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (uri: String, ?opts: Object) -> Any, |ctx, args| http_dispatch(ctx, "http::head", HttpVerb::Head, args));
define_async_function!(HttpGet, "http::get", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (uri: String, ?opts: Object) -> Any, |ctx, args| http_dispatch(ctx, "http::get", HttpVerb::Get, args));
define_async_function!(HttpPut, "http::put", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (uri: String, ?body: Any, ?opts: Object) -> Any, |ctx, args| http_dispatch(ctx, "http::put", HttpVerb::Put, args));
define_async_function!(HttpPost, "http::post", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (uri: String, ?body: Any, ?opts: Object) -> Any, |ctx, args| http_dispatch(ctx, "http::post", HttpVerb::Post, args));
define_async_function!(HttpPatch, "http::patch", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (uri: String, ?body: Any, ?opts: Object) -> Any, |ctx, args| http_dispatch(ctx, "http::patch", HttpVerb::Patch, args));
define_async_function!(HttpDelete, "http::delete", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (uri: String, ?opts: Object) -> Any, |ctx, args| http_dispatch(ctx, "http::delete", HttpVerb::Delete, args));

// =========================================================================
// Registration
// =========================================================================

// A remote endpoint is outside the transaction's snapshot, so a second call may
// return a different answer, and the write verbs change remote state. Every verb
// declares `READS_EXTERNAL`. The non-safe verbs (put, post, patch, delete) also
// declare `WRITES_EXTERNAL`. Rewrite rules must not duplicate or drop a request,
// and a grouped query must not change how many times a request is sent.
// `head` and `get` only read.
/// The `http::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] =
	&[&HttpHead, &HttpGet, &HttpPut, &HttpPost, &HttpPatch, &HttpDelete];
