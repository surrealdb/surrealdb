//! Per-client-address rate limiting for the HTTP authentication endpoints.
//!
//! [`auth_rate_limit_middleware`] guards `POST /signin` and `POST /signup`
//! with a token-bucket limiter, running before the authentication layer so
//! a limited request is rejected before any credential verification —
//! including the HTTP Basic check the auth layer performs — or any other
//! downstream work. Requests with other methods or paths, CORS preflights
//! included, pass through untouched.
//!
//! The limiter keys on the client address for the request, resolved by
//! [`request_key`] and reduced to a per-client key by [`derive_key`]: only
//! the last element of a comma-separated chain is used (appending proxies
//! add the address they observed at the end, so earlier elements are
//! client-supplied), the element must parse as an IP address, and IPv6
//! addresses are grouped by their /64 prefix so rotating within a
//! routinely-delegated prefix cannot mint fresh buckets. Both endpoints draw
//! from one bucket per key, so signin and signup attempts share a budget.
//!
//! For every `--client-ip` strategy except `Forwarded` the address is the
//! one already resolved into the [`ExtractClientIP`] extension (the same
//! value carried in `Session::ip`). `Forwarded` is the exception: it resolves
//! `Session::ip` from the *first* forwarded element, which a client can
//! supply, so the limiter re-reads the raw header and keys on its last
//! element — the one appended by the proxy in front of this server.
//!
//! Requests without a derivable key — `--client-ip none`, an embedder
//! serving the router without socket connect-info, or a header value that
//! is not an IP address — are not limited: a shared fallback bucket would
//! let a single abuser exhaust the budget for every client at once. A
//! deployment therefore only gains protection for requests whose configured
//! address source yields one address per client; the header strategies are
//! only as trustworthy as the proxy that sets the header.
//!
//! The tracking store is written by unauthenticated callers, so it is a
//! bounded cache ([`crate::cnf::HTTP_AUTH_RATE_LIMIT_MAX_TRACKED_CLIENTS`]
//! entries): at capacity, tracking a new address evicts an existing entry in
//! O(1) rather than growing the store or scanning it. Eviction restores the
//! evicted address's full burst, so the configured sustained rate binds an
//! address only while it stays resident — a caller able to cycle more
//! distinct keys than the capacity is bounded by the store's turnover
//! instead.

use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::Method;
use quick_cache::sync::Cache;
use web_time::Instant;

use super::AppState;
use super::client_ip::{self, ClientIp, ExtractClientIP};
use crate::cnf;
use crate::ntw::error::Error as NetError;

/// Minimum interval between `warn!`-level reports for one address. Repeated
/// denials inside the window log at `debug!` so a sustained attack cannot
/// flood the log with one warning per refilled token.
const WARN_SUPPRESS_WINDOW: Duration = Duration::from_secs(60);

/// Outcome of a rate-limit check.
enum Decision {
	Allowed,
	Limited {
		/// Seconds until the next attempt will be admitted.
		retry_after_secs: u64,
		/// Whether this denial should be reported at `warn!` level: set at
		/// most once per [`WARN_SUPPRESS_WINDOW`] per address.
		warn: bool,
	},
}

struct Bucket {
	/// Remaining budget, in attempts. Refilled continuously at the
	/// configured rate, capped at the configured burst.
	tokens: f64,
	/// When `tokens` was last brought up to date.
	refilled: Instant,
	/// When this address was last reported at `warn!` level.
	last_warned: Option<Instant>,
}

struct AuthRateLimiter {
	/// Bounded per-address bucket store. Eviction at capacity is O(1) and
	/// always makes room, so a key miss never scans the store and admission
	/// never depends on how full it is.
	buckets: Cache<IpAddr, Arc<Mutex<Bucket>>>,
	/// Bucket capacity, in attempts.
	burst: f64,
	/// Refill rate, in attempts per second.
	per_sec: f64,
}

impl AuthRateLimiter {
	fn new(burst: u32, per_minute: u32, max_tracked: usize) -> Self {
		Self {
			buckets: Cache::new(max_tracked),
			burst: f64::from(burst),
			per_sec: f64::from(per_minute) / 60.0,
		}
	}

	/// Refill the key's bucket up to `now`, then take one token if available.
	fn check_at(&self, key: IpAddr, now: Instant) -> Decision {
		let bucket = self
			.buckets
			.get_or_insert_with(&key, || {
				Ok::<_, std::convert::Infallible>(Arc::new(Mutex::new(Bucket {
					tokens: self.burst,
					refilled: now,
					last_warned: None,
				})))
			})
			.expect("bucket construction is infallible");
		// A poisoned lock only means another thread panicked mid-update; the
		// bucket state is still a valid snapshot, so recover it.
		let mut bucket = bucket.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		let elapsed = now.saturating_duration_since(bucket.refilled).as_secs_f64();
		bucket.tokens = (bucket.tokens + elapsed * self.per_sec).min(self.burst);
		bucket.refilled = now;
		if bucket.tokens >= 1.0 {
			bucket.tokens -= 1.0;
			Decision::Allowed
		} else {
			let retry_after_secs = ((1.0 - bucket.tokens) / self.per_sec).ceil().max(1.0) as u64;
			let warn = bucket
				.last_warned
				.is_none_or(|at| now.saturating_duration_since(at) >= WARN_SUPPRESS_WINDOW);
			if warn {
				bucket.last_warned = Some(now);
			}
			Decision::Limited {
				retry_after_secs,
				warn,
			}
		}
	}
}

/// Derives the limiter key from the address string resolved by the
/// `--client-ip` strategy.
///
/// Header strategies can carry a comma-separated chain in which every
/// element except the last is client-supplied (an appending proxy adds the
/// address it observed at the end), so only the last element is used. The
/// element must parse as an IP address (an optional port is accepted, and a
/// v4-mapped IPv6 address is reduced to its IPv4 form); anything else yields
/// no key. IPv6 addresses key on their /64 prefix, the unit routinely
/// delegated to a single client.
fn derive_key(raw: &str) -> Option<IpAddr> {
	let candidate = raw.rsplit(',').next()?.trim();
	let ip = candidate
		.parse::<IpAddr>()
		.or_else(|_| candidate.parse::<SocketAddr>().map(|addr| addr.ip()))
		.ok()?;
	Some(match ip {
		IpAddr::V4(v4) => IpAddr::V4(v4),
		IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
			Some(v4) => IpAddr::V4(v4),
			None => {
				let s = v6.segments();
				IpAddr::V6(Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0))
			}
		},
	})
}

/// Resolves the limiter key for `request`, or `None` when the configured
/// address source yields nothing that parses as an IP address.
///
/// Every strategy but `Forwarded` is read from the [`ExtractClientIP`]
/// extension inserted by [`super::client_ip::client_ip_middleware`], which
/// must run earlier in the middleware stack. `Forwarded` resolves
/// `Session::ip` from the first forwarded element, which a client can supply,
/// so the key is taken from the last element of the raw header instead.
fn request_key(request: &Request) -> Option<IpAddr> {
	let strategy = request.extensions().get::<AppState>().map(|state| state.client_ip);
	if let Some(ClientIp::Forwarded) = strategy {
		let raw = request.headers().get(http::header::FORWARDED)?.to_str().ok()?;
		return client_ip::parse_forwarded_for_last(raw).as_deref().and_then(derive_key);
	}
	request
		.extensions()
		.get::<ExtractClientIP>()
		.and_then(|ExtractClientIP(ip)| ip.as_deref())
		.and_then(derive_key)
}

/// Process-wide limiter shared by `/signin` and `/signup`. `None` when
/// disabled by configuration. Configuration is read once, on first use,
/// like every other `cnf` static.
static LIMITER: LazyLock<Option<AuthRateLimiter>> = LazyLock::new(|| {
	if !*cnf::HTTP_AUTH_RATE_LIMIT_ENABLED {
		return None;
	}
	let burst = *cnf::HTTP_AUTH_RATE_LIMIT_BURST;
	let per_minute = *cnf::HTTP_AUTH_RATE_LIMIT_PER_MINUTE;
	let max_tracked = *cnf::HTTP_AUTH_RATE_LIMIT_MAX_TRACKED_CLIENTS;
	if burst == 0 || per_minute == 0 || max_tracked == 0 {
		return None;
	}
	Some(AuthRateLimiter::new(burst, per_minute, max_tracked))
});

/// Admit or reject one authentication attempt from `key` against `endpoint`
/// (`"signin"` / `"signup"`, used only for logging). Returns
/// `Err(NetError::TooManyRequests)` when the address has exhausted its
/// budget.
fn check(limiter: &AuthRateLimiter, key: IpAddr, endpoint: &str) -> Result<(), NetError> {
	match limiter.check_at(key, Instant::now()) {
		Decision::Allowed => Ok(()),
		Decision::Limited {
			retry_after_secs,
			warn,
		} => {
			if warn {
				warn!("Rate limiting authentication attempts from '{key}' on /{endpoint}");
			} else {
				debug!("Rate limited authentication attempt from '{key}' on /{endpoint}");
			}
			Err(NetError::TooManyRequests(retry_after_secs))
		}
	}
}

/// Throttles `POST /signin` and `POST /signup` per client address before the
/// authentication layer performs any credential verification for the
/// request. Every other method and path — CORS preflights included — passes
/// through untouched.
pub(super) async fn auth_rate_limit_middleware(request: Request, next: Next) -> Response {
	if let Err(err) = admit(&request) {
		return err.into_response();
	}
	next.run(request).await
}

/// Applies the limiter to `request` when it is an authentication attempt with
/// a derivable client address and the limiter is enabled. Every other request
/// is admitted untouched.
fn admit(request: &Request) -> Result<(), NetError> {
	let Some(limiter) = LIMITER.as_ref() else {
		return Ok(());
	};
	if request.method() != Method::POST {
		return Ok(());
	}
	let endpoint = match request.uri().path() {
		"/signin" => "signin",
		"/signup" => "signup",
		_ => return Ok(()),
	};
	let Some(key) = request_key(request) else {
		return Ok(());
	};
	check(limiter, key, endpoint)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ip(s: &str) -> IpAddr {
		s.parse().unwrap()
	}

	#[test]
	fn allows_burst_then_limits_with_retry_after() {
		let l = AuthRateLimiter::new(3, 60, 16);
		let now = Instant::now();
		for i in 0..3 {
			assert!(matches!(l.check_at(ip("1.2.3.4"), now), Decision::Allowed), "attempt {i}");
		}
		match l.check_at(ip("1.2.3.4"), now) {
			Decision::Limited {
				retry_after_secs,
				warn,
			} => {
				assert_eq!(retry_after_secs, 1);
				assert!(warn);
			}
			Decision::Allowed => panic!("fourth attempt within the burst window must be limited"),
		}
		// Subsequent denials inside the suppression window do not warn.
		assert!(matches!(
			l.check_at(ip("1.2.3.4"), now),
			Decision::Limited {
				warn: false,
				..
			}
		));
	}

	#[test]
	fn budget_refills_over_time() {
		let l = AuthRateLimiter::new(2, 60, 16);
		let now = Instant::now();
		for _ in 0..2 {
			assert!(matches!(l.check_at(ip("9.9.9.9"), now), Decision::Allowed));
		}
		assert!(matches!(l.check_at(ip("9.9.9.9"), now), Decision::Limited { .. }));
		assert!(matches!(
			l.check_at(ip("9.9.9.9"), now + Duration::from_secs(1)),
			Decision::Allowed
		));
	}

	#[test]
	fn buckets_are_per_address() {
		let l = AuthRateLimiter::new(1, 60, 16);
		let now = Instant::now();
		assert!(matches!(l.check_at(ip("1.1.1.1"), now), Decision::Allowed));
		assert!(matches!(l.check_at(ip("1.1.1.1"), now), Decision::Limited { .. }));
		assert!(matches!(l.check_at(ip("2.2.2.2"), now), Decision::Allowed));
	}

	#[test]
	fn warns_at_most_once_per_suppression_window() {
		let l = AuthRateLimiter::new(1, 60, 16);
		let t0 = Instant::now();
		assert!(matches!(l.check_at(ip("3.3.3.3"), t0), Decision::Allowed));
		// First denial warns and starts the suppression window.
		assert!(matches!(
			l.check_at(ip("3.3.3.3"), t0),
			Decision::Limited {
				warn: true,
				..
			}
		));
		// A token refills and is spent again; the following denial is still
		// inside the window, so it must not warn — admission does not end
		// the window.
		let t1 = t0 + Duration::from_secs(2);
		assert!(matches!(l.check_at(ip("3.3.3.3"), t1), Decision::Allowed));
		assert!(matches!(
			l.check_at(ip("3.3.3.3"), t1),
			Decision::Limited {
				warn: false,
				..
			}
		));
		// Past the window, the next denial warns again.
		let t2 = t0 + Duration::from_secs(61);
		assert!(matches!(l.check_at(ip("3.3.3.3"), t2), Decision::Allowed));
		assert!(matches!(
			l.check_at(ip("3.3.3.3"), t2),
			Decision::Limited {
				warn: true,
				..
			}
		));
	}

	#[test]
	fn ipv6_addresses_share_a_slash64_bucket() {
		let a = derive_key("2001:db8:1:2:aaaa::1").unwrap();
		let b = derive_key("2001:db8:1:2::beef").unwrap();
		assert_eq!(a, b, "addresses inside one /64 must share a key");
		let c = derive_key("2001:db8:1:3::1").unwrap();
		assert_ne!(a, c, "addresses in different /64s must not share a key");

		// Rotating the low 64 bits cannot mint fresh budgets.
		let l = AuthRateLimiter::new(1, 60, 16);
		let now = Instant::now();
		assert!(matches!(l.check_at(a, now), Decision::Allowed));
		assert!(matches!(l.check_at(b, now), Decision::Limited { .. }));
	}

	#[test]
	fn derive_key_uses_only_the_last_chain_element() {
		// An appending proxy puts the address it observed last; everything
		// before it is client-supplied and must not affect the key.
		assert_eq!(derive_key("6.6.6.6, 7.7.7.7"), Some(ip("7.7.7.7")));
		assert_eq!(derive_key("1.1.1.1, 2.2.2.2, 7.7.7.7"), Some(ip("7.7.7.7")));
		// Rotating a spoofed prefix therefore always lands on one bucket.
		let l = AuthRateLimiter::new(1, 60, 16);
		let now = Instant::now();
		let first = derive_key("1.1.1.1, 9.9.9.9").unwrap();
		let second = derive_key("1.1.1.2, 9.9.9.9").unwrap();
		assert!(matches!(l.check_at(first, now), Decision::Allowed));
		assert!(matches!(l.check_at(second, now), Decision::Limited { .. }));
	}

	#[test]
	fn forwarded_key_comes_from_the_proxy_appended_element() {
		// `Forwarded` carries the client-supplied element first, so the key
		// must come from the element the proxy appended.
		let key = client_ip::parse_forwarded_for_last("for=1.1.1.1, for=9.9.9.9")
			.as_deref()
			.and_then(derive_key);
		assert_eq!(key, Some(ip("9.9.9.9")));
		// Rotating the spoofed prefix therefore always lands on one bucket.
		let spoofed = client_ip::parse_forwarded_for_last("for=1.1.1.2, for=9.9.9.9")
			.as_deref()
			.and_then(derive_key);
		assert_eq!(spoofed, key);
		// A quoted v6 identifier keys on its /64 prefix.
		let v6 = client_ip::parse_forwarded_for_last(r#"for="[2001:db8:1:2::1]:443""#)
			.as_deref()
			.and_then(derive_key);
		assert_eq!(v6, Some(ip("2001:db8:1:2::")));
	}

	#[test]
	fn derive_key_parses_addresses_and_rejects_garbage() {
		assert_eq!(derive_key("1.2.3.4"), Some(ip("1.2.3.4")));
		assert_eq!(derive_key(" 1.2.3.4 "), Some(ip("1.2.3.4")));
		assert_eq!(derive_key("1.2.3.4:5555"), Some(ip("1.2.3.4")));
		assert_eq!(derive_key("[2001:db8::1]:443"), Some(ip("2001:db8::")));
		// A v4-mapped v6 address keys as its v4 form.
		assert_eq!(derive_key("::ffff:1.2.3.4"), Some(ip("1.2.3.4")));
		assert_eq!(derive_key("not-an-ip"), None);
		assert_eq!(derive_key(""), None);
		assert_eq!(derive_key("unknown, also-unknown"), None);
	}

	#[test]
	fn tracking_is_bounded_at_capacity() {
		let l = AuthRateLimiter::new(1, 60, 4);
		let now = Instant::now();
		// Far more distinct addresses than the cap: every request still gets
		// a decision and the store never grows past its capacity.
		for i in 0..64u8 {
			let key = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, i));
			assert!(matches!(l.check_at(key, now), Decision::Allowed));
		}
		assert!(l.buckets.len() <= 4, "store grew past capacity: {}", l.buckets.len());
		// The most recently tracked address is still resident, so its spent
		// bucket keeps denying instead of being recreated with a fresh burst.
		assert!(matches!(l.check_at(ip("10.0.0.63"), now), Decision::Limited { .. }));
	}
}
