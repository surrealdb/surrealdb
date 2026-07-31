#[cfg(feature = "http")]
use anyhow::Result;
#[cfg(feature = "http")]
use http::Method;
#[cfg(all(feature = "http", not(target_family = "wasm")))]
use reqwest::redirect::{Action, Attempt};
#[cfg(feature = "http")]
use reqwest::{Client, RequestBuilder};
#[cfg(feature = "http")]
use url::Url;

#[cfg(feature = "http")]
use crate::dbs::capabilities::{NetTarget, Targets};
#[cfg(feature = "http")]
use crate::http::config::HttpConfig;

/// The knobs every outbound client is built from. Not gated on the `http`
/// feature: the JWKS fetch client reads them under `jwks` alone, where the
/// client below is absent.
pub(crate) mod config;

#[cfg(feature = "http")]
pub struct HttpClient {
	client: Client,
}

#[cfg(feature = "http")]
impl HttpClient {
	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new(
		allow: Targets<NetTarget>,
		deny: Targets<NetTarget>,
		config: &HttpConfig,
	) -> Result<Self> {
		Self::new_with_redirect_policy(allow, deny, config, |policy| policy.follow())
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new_with_redirect_policy<F>(
		allow: Targets<NetTarget>,
		deny: Targets<NetTarget>,
		config: &HttpConfig,
		policy: F,
	) -> Result<Self>
	where
		F: Fn(Attempt) -> Action + Send + Sync + 'static,
	{
		use std::sync::Arc;
		use std::time::Duration;

		use anyhow::Context as _;
		use http::header::USER_AGENT;
		use http::{HeaderMap, HeaderValue};
		use reqwest::redirect::{Attempt, Policy};

		use crate::dbs::capabilities::NetTarget;
		use crate::net::{FilteringResolver, NetFilter};

		let filter = Arc::new(NetFilter {
			allow,
			deny,
		});

		let filter_clone = Arc::clone(&filter);
		let max_redirects = config.max_http_redirects;
		let redirect_function = move |attempt: Attempt| {
			if attempt.previous().len() >= max_redirects {
				return attempt.stop();
			}

			// Re-validate the redirect target against allow/deny rules using the
			// same port-aware logic as `check_allowed_net`, so that port-specific
			// rules (e.g. `deny_net = ["example.com:6379"]`) are enforced on every
			// hop in the redirect chain.
			let url = attempt.url();
			let host = match url.host() {
				Some(h) => h,
				None => {
					let url_str = url.to_string();
					return attempt.error(crate::err::Error::InvalidUrl(url_str));
				}
			};
			let port = url.port_or_known_default();
			let target = NetTarget::Host(host.to_owned(), port);

			if !filter_clone.allow.matches(&target) || filter_clone.deny.matches(&target) {
				return attempt.error(crate::dbs::capabilities::Error::NetTargetNotAllowed(
					target.to_string(),
				));
			}

			policy(attempt)
		};

		let value = HeaderValue::from_str(&config.surrealdb_user_agent)
			.context("Invalid user agent string")?;

		let mut headers = HeaderMap::new();
		headers.insert(USER_AGENT, value);

		let client = Client::builder()
			.pool_idle_timeout(Duration::from_secs(config.http_idle_timeout_secs))
			.pool_max_idle_per_host(config.max_http_idle_connections_per_host)
			.connect_timeout(Duration::from_secs(config.http_connect_timeout_secs))
			.tcp_keepalive(Some(Duration::from_secs(60)))
			.http2_keep_alive_interval(Some(Duration::from_secs(30)))
			.http2_keep_alive_timeout(Duration::from_secs(10))
			.redirect(Policy::custom(redirect_function))
			.dns_resolver(FilteringResolver::from_net_filter(filter))
			.default_headers(headers)
			.build()?;

		Ok(HttpClient {
			client,
		})
	}

	/// On `wasm32` the client cannot enforce network capabilities itself: the
	/// `fetch` runtime exposes no DNS-resolver hook and no redirect-policy
	/// control (unlike the native `reqwest` client, which installs a
	/// `FilteringResolver` and a per-hop `redirect::Policy`). Enforcement
	/// therefore lives entirely in
	/// [`Context::check_allowed_net`](crate::ctx::Context), which validates the
	/// initial request URL's host against `allow`/`deny` before the request is
	/// made. Redirect hops are followed transparently by the runtime and are
	/// NOT re-validated — see the WASM exception in `SECURITY_GUIDE.md` §11.
	/// `allow`/`deny` are accepted for signature parity with the native
	/// constructor.
	///
	/// The configured `User-Agent` is still applied — reqwest's wasm client
	/// merges client default headers into every request, and the Workers `fetch`
	/// runtime (unlike a browser) permits setting it — so hosts that reject
	/// requests lacking a `User-Agent`, such as the GitHub API, are reachable.
	#[cfg(target_family = "wasm")]
	pub fn new(
		allow: Targets<NetTarget>,
		deny: Targets<NetTarget>,
		config: &HttpConfig,
	) -> Result<Self> {
		let _ = allow;
		let _ = deny;
		let client = Client::builder().user_agent(config.surrealdb_user_agent.as_str()).build()?;
		Ok(HttpClient {
			client,
		})
	}

	pub fn request(&self, method: Method, url: Url) -> RequestBuilder {
		self.client.request(method, url)
	}
}
