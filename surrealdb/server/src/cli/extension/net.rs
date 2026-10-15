//! The one HTTP client the install client uses, for Silo, Sigstore's TUF
//! repository and the Rekor log.
//!
//! - HTTPS only. Plain HTTP is allowed for an endpoint on a loopback address, such as a mirror on
//!   the same machine, and nowhere else.
//! - A redirect is followed only to an HTTPS URL (or, from a loopback HTTP endpoint, within the
//!   same origin), at most [`MAX_REDIRECTS`] times, so a server cannot move a download into the
//!   clear.
//! - An endpoint may not carry credentials (`user:pass@`): they would end up in error messages and
//!   trace spans.
//! - A connection that goes quiet for [`READ_TIMEOUT`] fails; a slow but steady transfer is bounded
//!   only by `total`.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use reqwest::{Client, Url};

/// The most redirects followed for one request.
pub const MAX_REDIRECTS: usize = 5;

/// How long a connection may take to open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a connection may go without receiving any data.
pub const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether `url` names a loopback host: `localhost` or a loopback address.
pub fn is_loopback(url: &Url) -> bool {
	let Some(host) = url.host_str() else {
		return false;
	};
	match host.trim_start_matches('[').trim_end_matches(']').parse::<IpAddr>() {
		Ok(ip) => ip.is_loopback(),
		Err(_) => host.eq_ignore_ascii_case("localhost"),
	}
}

/// Checks an endpoint the client is told to talk to: HTTPS (plain HTTP only
/// on loopback), and no credentials. The error names the endpoint without
/// any credentials it holds.
pub fn check_endpoint(what: &str, endpoint: &str) -> Result<Url> {
	let url = Url::parse(endpoint).with_context(|| format!("The {what} is not a URL"))?;
	if !url.username().is_empty() || url.password().is_some() {
		bail!(
			"The {what} must not hold credentials (`user:password@`); they would be written to \
			 logs and error messages"
		);
	}
	match url.scheme() {
		"https" => {}
		"http" if is_loopback(&url) => {}
		"http" => bail!("The {what} `{url}` must use https; http is allowed only on loopback"),
		other => bail!("The {what} `{url}` uses `{other}`, not https"),
	}
	Ok(url)
}

/// Whether a redirect from `first` (the request's own URL) to `next` may be
/// followed: always to HTTPS, and to HTTP only within `first`'s own loopback
/// origin.
pub fn may_follow(first: &Url, next: &Url) -> bool {
	match next.scheme() {
		"https" => true,
		"http" => {
			is_loopback(first)
				&& first.scheme() == "http"
				&& first.host_str() == next.host_str()
				&& first.port_or_known_default() == next.port_or_known_default()
		}
		_ => false,
	}
}

/// The hardened client. `total` bounds a whole request, however steady;
/// `loopback_http` lets the client talk plain HTTP to a loopback endpoint
/// (checked by [`check_endpoint`]).
pub fn client(total: Duration, loopback_http: bool) -> Result<Client> {
	let policy = reqwest::redirect::Policy::custom(|attempt| {
		let allowed =
			attempt.previous().first().is_some_and(|first| may_follow(first, attempt.url()));
		if allowed && attempt.previous().len() <= MAX_REDIRECTS {
			attempt.follow()
		} else {
			attempt.stop()
		}
	});
	Client::builder()
		.user_agent(concat!("surreal/", env!("CARGO_PKG_VERSION")))
		.connect_timeout(CONNECT_TIMEOUT)
		.read_timeout(READ_TIMEOUT)
		.timeout(total)
		.https_only(!loopback_http)
		.redirect(policy)
		.build()
		.context("Failed to build the HTTP client")
}

#[cfg(test)]
mod tests {
	use super::*;

	fn url(s: &str) -> Url {
		Url::parse(s).unwrap()
	}

	#[test]
	fn redirects_go_only_to_https() {
		let https = url("https://silo.example.com/acme/color/1.0.0.surli");
		assert!(may_follow(&https, &url("https://cdn.example.net/x.surli")));
		assert!(!may_follow(&https, &url("http://silo.example.com/acme/color/1.0.0.surli")));
		assert!(!may_follow(&https, &url("ftp://silo.example.com/x")));

		let local = url("http://127.0.0.1:8080/acme/color/1.0.0.surli");
		assert!(may_follow(&local, &url("http://127.0.0.1:8080/other")));
		assert!(!may_follow(&local, &url("http://127.0.0.1:9090/other")), "another origin");
		assert!(!may_follow(&local, &url("http://evil.example/x")));
	}

	#[test]
	fn endpoints_are_https_or_loopback_and_hold_no_credentials() {
		assert!(check_endpoint("endpoint", "https://silo.example.com").is_ok());
		for local in ["http://localhost:8000", "http://127.0.0.1:8000", "http://[::1]:8000"] {
			assert!(check_endpoint("endpoint", local).is_ok(), "{local}");
		}
		let err = check_endpoint("endpoint", "http://silo.example.com").unwrap_err();
		assert!(err.to_string().contains("must use https"), "{err}");
		let err = check_endpoint("endpoint", "https://user:hunter2@mirror.example").unwrap_err();
		assert!(err.to_string().contains("must not hold credentials"), "{err}");
		assert!(!err.to_string().contains("hunter2"), "{err}");
		assert!(check_endpoint("endpoint", "https://token@mirror.example").is_err());
	}
}
