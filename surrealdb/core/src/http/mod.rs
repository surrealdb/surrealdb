use anyhow::Result;
use http::Method;
#[cfg(not(target_family = "wasm"))]
use reqwest::redirect::{Action, Attempt};
use reqwest::{Client, RequestBuilder};
use url::Url;

use crate::dbs::capabilities::{NetTarget, Targets};

#[cfg(not(target_family = "wasm"))]
mod resolve;

pub struct HttpClient {
	client: Client,
}

#[cfg(not(target_family = "wasm"))]
struct NetFilter {
	allow: Targets<NetTarget>,
	deny: Targets<NetTarget>,
}

impl HttpClient {
	#[cfg(not(target_family = "wasm"))]
	pub fn new(allow: Targets<NetTarget>, deny: Targets<NetTarget>) -> Result<Self> {
		Self::new_with_redirect_policy(allow, deny, |policy| policy.follow())
	}

	#[cfg(not(target_family = "wasm"))]
	pub fn new_with_redirect_policy<F>(
		allow: Targets<NetTarget>,
		deny: Targets<NetTarget>,
		policy: F,
	) -> Result<Self>
	where
		F: Fn(Attempt) -> Action + Send + Sync + 'static,
	{
		use std::str::FromStr;
		use std::sync::Arc;
		use std::time::Duration;

		use anyhow::Context as _;
		use http::header::USER_AGENT;
		use http::{HeaderMap, HeaderValue};
		use reqwest::redirect::{Attempt, Policy};
		use resolve::FilteringResolver;

		use crate::cnf::SURREALDB_USER_AGENT;
		use crate::dbs::capabilities::NetTarget;

		let filter = Arc::new(NetFilter {
			allow,
			deny,
		});

		let filter_clone = filter.clone();
		let max_redirects = *crate::cnf::MAX_HTTP_REDIRECTS;
		let redirect_function = move |attempt: Attempt| {
			if attempt.previous().len() >= max_redirects {
				return attempt.stop();
			}

			// Check domain name allowlist
			let url = attempt.url();
			let target = match NetTarget::from_str(url.host_str().unwrap_or(""))
				.map_err(|e| crate::err::Error::InvalidUrl(format!("Invalid host: {}", e)))
			{
				Ok(x) => x,
				Err(e) => return attempt.error(e),
			};

			if !filter_clone.allow.matches(&target) || filter_clone.deny.matches(&target) {
				let url = url.to_string();
				return attempt.error(crate::err::Error::NetTargetNotAllowed(url));
			}

			policy(attempt)
		};

		let value =
			HeaderValue::from_str(&SURREALDB_USER_AGENT).context("Invalid user agent string")?;

		let mut headers = HeaderMap::new();
		headers.insert(USER_AGENT, value);

		let client = Client::builder()
			.pool_idle_timeout(Duration::from_secs(*crate::cnf::HTTP_IDLE_TIMEOUT_SECS))
			.pool_max_idle_per_host(*crate::cnf::MAX_HTTP_IDLE_CONNECTIONS_PER_HOST)
			.connect_timeout(Duration::from_secs(*crate::cnf::HTTP_CONNECT_TIMEOUT_SECS))
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

	#[cfg(target_family = "wasm")]
	pub fn new(allow: Targets<NetTarget>, deny: Targets<NetTarget>) -> Result<Self> {
		let _ = allow;
		let _ = deny;
		let client = Client::builder().build()?;
		Ok(HttpClient {
			client,
		})
	}

	pub fn request(&self, method: Method, url: Url) -> RequestBuilder {
		self.client.request(method, url)
	}
}
