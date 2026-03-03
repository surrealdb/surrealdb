use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
#[cfg(not(target_family = "wasm"))]
use dashmap::DashMap;
use reqwest::header::CONTENT_TYPE;
#[cfg(not(target_family = "wasm"))]
use reqwest::redirect::Attempt;
#[cfg(not(target_family = "wasm"))]
use reqwest::redirect::Policy;
use reqwest::{Client, Method, RequestBuilder, Response};
use url::Url;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::sql::expression::convert_public_value_to_internal;
use crate::syn;
use crate::types::{PublicBytes, PublicValue};
use crate::val::{Object, Value};

/// Global HTTP client manager for connection pooling and reuse.
#[cfg(not(target_family = "wasm"))]
static HTTP_CLIENT_MANAGER: tokio::sync::OnceCell<HttpClientManager> =
	tokio::sync::OnceCell::const_new();

/// A manager for HTTP clients that caches them based on the capabilities.
#[cfg(not(target_family = "wasm"))]
struct HttpClientManager {
	/// Map from Capabilities hash -> Client
	clients: DashMap<u64, Arc<Client>>,
}

#[cfg(not(target_family = "wasm"))]
impl HttpClientManager {
	fn new() -> Self {
		Self {
			clients: DashMap::new(),
		}
	}

	/// Get or create a client based on the capabilities.
	async fn get_or_create_client(
		&self,
		capabilities: Arc<crate::dbs::Capabilities>,
		http_cfg: &crate::cnf::HttpClientConfig,
		redirect_checker: Option<
			impl Fn(&Url) -> Result<(), crate::err::Error> + Send + Sync + 'static,
		>,
	) -> Result<Arc<Client>> {
		let capabilities_hash = self.hash_capabilities(&capabilities);

		// Try to get existing client.
		if let Some(client) = self.clients.get(&capabilities_hash) {
			return Ok(Arc::clone(client.value()));
		}

		// Client doesn't exist, create a new one.
		match self.clients.entry(capabilities_hash) {
			dashmap::mapref::entry::Entry::Occupied(entry) => {
				// Another thread created it while we were working.
				Ok(Arc::clone(entry.get()))
			}
			dashmap::mapref::entry::Entry::Vacant(entry) => {
				// We need to create the client
				let mut builder = Client::builder()
					.pool_idle_timeout(Duration::from_secs(http_cfg.idle_timeout_secs))
					.pool_max_idle_per_host(http_cfg.max_idle_connections_per_host)
					.connect_timeout(Duration::from_secs(http_cfg.connect_timeout_secs))
					.tcp_keepalive(Some(Duration::from_secs(60)))
					.http2_keep_alive_interval(Some(Duration::from_secs(30)))
					.http2_keep_alive_timeout(Duration::from_secs(10));

				if let Some(checker) = redirect_checker {
					let count = http_cfg.max_redirects;
					let policy =
						Policy::custom(move |attempt: Attempt| match checker(attempt.url()) {
							Ok(()) => {
								if attempt.previous().len() >= count {
									attempt.stop()
								} else {
									attempt.follow()
								}
							}
							Err(e) => attempt.error(e),
						});
					builder = builder.redirect(policy);
				}

				builder = builder.dns_resolver(Arc::new(
					crate::fnc::http::resolver::FilteringResolver::from_capabilities(capabilities),
				));

				let client = Arc::new(builder.build()?);
				entry.insert(Arc::clone(&client));
				Ok(client)
			}
		}
	}

	/// Hash the capabilities for caching.
	fn hash_capabilities(&self, capabilities: &crate::dbs::Capabilities) -> u64 {
		use std::collections::hash_map::DefaultHasher;
		use std::hash::{Hash, Hasher};

		let mut hasher = DefaultHasher::new();
		capabilities.hash(&mut hasher);
		hasher.finish()
	}
}

#[cfg(not(target_family = "wasm"))]
async fn get_http_client(
	capabilities: Arc<crate::dbs::Capabilities>,
	http_cfg: &crate::cnf::HttpClientConfig,
	redirect_checker: Option<
		impl Fn(&Url) -> Result<(), crate::err::Error> + Send + Sync + 'static,
	>,
) -> Result<Arc<Client>> {
	let manager = HTTP_CLIENT_MANAGER.get_or_init(|| async { HttpClientManager::new() }).await;

	manager.get_or_create_client(capabilities, http_cfg, redirect_checker).await
}

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
					let val = syn::json(&txt)
						.context("Failed to parse JSON response")
						.map_err(|e| Error::Http(e.to_string()))?;
					Ok(val)
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

	// Get or create a shared HTTP client for better connection reuse
	#[cfg(not(target_family = "wasm"))]
	let cli = {
		let capabilities = ctx.get_capabilities();
		let http_cfg = &ctx.config().http_client;
		let capabilities_clone = Arc::clone(&capabilities);
		let redirect_checker = move |url: &Url| -> Result<(), crate::err::Error> {
			use std::str::FromStr;

			use crate::dbs::capabilities::NetTarget;

			let target = NetTarget::from_str(url.host_str().unwrap_or(""))
				.map_err(|e| crate::err::Error::InvalidUrl(format!("Invalid host: {}", e)))?;

			if !capabilities_clone.matches_any_allow_net(&target)
				|| capabilities_clone.matches_any_deny_net(&target)
			{
				return Err(crate::err::Error::NetTargetNotAllowed(url.to_string()));
			}

			Ok(())
		};

		get_http_client(capabilities, http_cfg, Some(redirect_checker)).await?
	};

	#[cfg(target_family = "wasm")]
	let cli = {
		let builder = Client::builder();
		Arc::new(builder.build()?)
	};

	let is_head = matches!(method, Method::HEAD);
	// Start a new HTTP request using the shared client
	let mut req = cli.request(method.clone(), url);
	// Add the User-Agent header
	if cfg!(not(target_family = "wasm")) {
		req = req.header(reqwest::header::USER_AGENT, ctx.config().http_client.user_agent.as_str());
	}
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
