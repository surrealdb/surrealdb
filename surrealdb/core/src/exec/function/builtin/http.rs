//! HTTP functions for the streaming executor.
//!
//! These provide HTTP client functionality (GET, POST, PUT, PATCH, DELETE, HEAD).
//! Note: HTTP functions require the "http" feature to be enabled.

use anyhow::Result;

use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
#[cfg(feature = "http")]
use crate::val::Object;
use crate::val::Value;
use crate::{define_async_function, register_functions};

// =========================================================================
// Helper functions
// =========================================================================

#[cfg(not(feature = "http"))]
async fn http_disabled() -> Result<Value> {
	Err(anyhow::anyhow!(crate::err::Error::HttpDisabled))
}

#[cfg(feature = "http")]
fn extract_uri(args: &[Value], fn_name: &str) -> Result<String> {
	match args.first() {
		Some(Value::String(s)) => Ok(s.clone()),
		Some(v) => Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!(
				"The first argument should be a string containing a valid URI, got: {}",
				v.kind_of()
			),
		})),
		None => Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: "Missing URI argument".to_string(),
		})),
	}
}

#[cfg(feature = "http")]
fn extract_opts(args: &[Value], index: usize, fn_name: &str) -> Result<Object> {
	match args.get(index) {
		Some(Value::Object(o)) => Ok(o.clone()),
		Some(Value::None) | None => Ok(Object::default()),
		Some(v) => Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!("Options argument should be an object, got: {}", v.kind_of()),
		})),
	}
}

#[cfg(feature = "http")]
fn extract_body(args: &[Value], index: usize) -> Option<Value> {
	args.get(index).cloned()
}

// =========================================================================
// HTTP HEAD
// =========================================================================

#[cfg(feature = "http")]
async fn http_head_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let uri = extract_uri(&args, "http::head")?;
	let opts = extract_opts(&args, 1, "http::head")?;

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	// Make the request using reqwest directly
	http_request(ctx, reqwest::Method::HEAD, uri, None, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_head_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP GET
// =========================================================================

#[cfg(feature = "http")]
async fn http_get_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let uri = extract_uri(&args, "http::get")?;
	let opts = extract_opts(&args, 1, "http::get")?;

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	http_request(ctx, reqwest::Method::GET, uri, None, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_get_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP PUT
// =========================================================================

#[cfg(feature = "http")]
async fn http_put_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let uri = extract_uri(&args, "http::put")?;
	let body = extract_body(&args, 1);
	let opts = extract_opts(&args, 2, "http::put")?;

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	http_request(ctx, reqwest::Method::PUT, uri, body, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_put_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP POST
// =========================================================================

#[cfg(feature = "http")]
async fn http_post_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let uri = extract_uri(&args, "http::post")?;
	let body = extract_body(&args, 1);
	let opts = extract_opts(&args, 2, "http::post")?;

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	http_request(ctx, reqwest::Method::POST, uri, body, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_post_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP PATCH
// =========================================================================

#[cfg(feature = "http")]
async fn http_patch_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let uri = extract_uri(&args, "http::patch")?;
	let body = extract_body(&args, 1);
	let opts = extract_opts(&args, 2, "http::patch")?;

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	http_request(ctx, reqwest::Method::PATCH, uri, body, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_patch_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP DELETE
// =========================================================================

#[cfg(feature = "http")]
async fn http_delete_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let uri = extract_uri(&args, "http::delete")?;
	let opts = extract_opts(&args, 1, "http::delete")?;

	// Check if URL is allowed
	let url = url::Url::parse(&uri).map_err(|_| crate::err::Error::InvalidUrl(uri.clone()))?;
	ctx.check_allowed_net(&url).await?;

	http_request(ctx, reqwest::Method::DELETE, uri, None, opts).await
}

#[cfg(not(feature = "http"))]
async fn http_delete_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	http_disabled().await
}

// =========================================================================
// HTTP Request implementation
// =========================================================================

#[cfg(feature = "http")]
async fn http_request(
	ctx: &EvalContext<'_>,
	method: reqwest::Method,
	uri: String,
	body: Option<Value>,
	opts: Object,
) -> Result<Value> {
	use std::sync::Arc;
	#[cfg(not(target_family = "wasm"))]
	use std::time::Duration;

	use reqwest::header::CONTENT_TYPE;

	use crate::cnf::SURREALDB_USER_AGENT;
	use crate::err::Error;
	use crate::sql::expression::convert_public_value_to_internal;
	use crate::syn;
	use crate::types::{PublicBytes, PublicValue};

	let url = url::Url::parse(&uri).map_err(|_| Error::InvalidUrl(uri.clone()))?;

	// Build the HTTP client
	#[cfg(not(target_family = "wasm"))]
	let cli = {
		let capabilities = ctx.get_capabilities();
		let capabilities_clone = Arc::clone(&capabilities);

		let redirect_checker = move |rurl: &url::Url| -> Result<(), Error> {
			use std::str::FromStr;

			use crate::dbs::capabilities::NetTarget;

			let host = rurl.host_str().unwrap_or("");
			let target = NetTarget::from_str(host)
				.map_err(|e| Error::InvalidUrl(format!("Invalid host: {}", e)))?;

			if !capabilities_clone.matches_any_allow_net(&target)
				|| capabilities_clone.matches_any_deny_net(&target)
			{
				return Err(Error::NetTargetNotAllowed(rurl.to_string()));
			}
			Ok(())
		};

		let count = *crate::cnf::MAX_HTTP_REDIRECTS;
		let policy =
			reqwest::redirect::Policy::custom(move |attempt: reqwest::redirect::Attempt| {
				match redirect_checker(attempt.url()) {
					Ok(()) => {
						if attempt.previous().len() >= count {
							attempt.stop()
						} else {
							attempt.follow()
						}
					}
					Err(e) => attempt.error(e),
				}
			});

		reqwest::Client::builder()
			.pool_idle_timeout(Duration::from_secs(*crate::cnf::HTTP_IDLE_TIMEOUT_SECS))
			.pool_max_idle_per_host(*crate::cnf::MAX_HTTP_IDLE_CONNECTIONS_PER_HOST)
			.connect_timeout(Duration::from_secs(*crate::cnf::HTTP_CONNECT_TIMEOUT_SECS))
			.tcp_keepalive(Some(Duration::from_secs(60)))
			.http2_keep_alive_interval(Some(Duration::from_secs(30)))
			.http2_keep_alive_timeout(Duration::from_secs(10))
			.redirect(policy)
			.dns_resolver(Arc::new(
				crate::fnc::http::resolver::FilteringResolver::from_capabilities(capabilities),
			))
			.build()?
	};

	#[cfg(target_family = "wasm")]
	let cli = reqwest::Client::builder().build()?;

	let is_head = matches!(method, reqwest::Method::HEAD);

	// Start the request
	let mut req = cli.request(method, url);

	// Add User-Agent header
	if cfg!(not(target_family = "wasm")) {
		req = req.header(reqwest::header::USER_AGENT, &*SURREALDB_USER_AGENT);
	}

	// Add custom headers from opts
	for (k, v) in opts.iter() {
		req = req.header(k.as_str(), v.to_raw_string());
	}

	// Add body if present
	if let Some(b) = body {
		let public_body = crate::val::convert_value_to_public_value(b)?;
		req = match public_body {
			PublicValue::Bytes(v) => req.body(v.into_inner()),
			PublicValue::String(v) => req.body(v),
			_ if !public_body.is_nullish() => req.json(&public_body.into_json_value()),
			_ => req,
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
						let txt = res.text().await.map_err(Error::from)?;
						let val = syn::json(&txt)
							.map_err(|e| Error::Http(format!("Failed to parse JSON: {}", e)))?;
						Ok(convert_public_value_to_internal(val))
					}
					Ok(v) if v.starts_with("application/octet-stream") => {
						let bytes = res.bytes().await.map_err(Error::from)?;
						Ok(convert_public_value_to_internal(PublicValue::Bytes(PublicBytes::from(
							bytes,
						))))
					}
					Ok(v) if v.starts_with("text") => {
						let txt = res.text().await.map_err(Error::from)?;
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
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_async_function!(HttpHead, "http::head", (uri: String, ?opts: Object) -> Any, http_head_impl);
define_async_function!(HttpGet, "http::get", (uri: String, ?opts: Object) -> Any, http_get_impl);
define_async_function!(HttpPut, "http::put", (uri: String, ?body: Any, ?opts: Object) -> Any, http_put_impl);
define_async_function!(HttpPost, "http::post", (uri: String, ?body: Any, ?opts: Object) -> Any, http_post_impl);
define_async_function!(HttpPatch, "http::patch", (uri: String, ?body: Any, ?opts: Object) -> Any, http_patch_impl);
define_async_function!(HttpDelete, "http::delete", (uri: String, ?opts: Object) -> Any, http_delete_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, HttpHead, HttpGet, HttpPut, HttpPost, HttpPatch, HttpDelete,);
}
