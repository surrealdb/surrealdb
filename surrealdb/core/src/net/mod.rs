//! Shared, capability-aware networking helpers for outbound HTTP requests.
//!
//! Both outbound clients in the engine must enforce SurrealDB's network
//! capabilities — the general-purpose protected HTTP client (`crate::http`,
//! behind the `http` feature) and the JWKS fetch client (`crate::iam::jwks`,
//! behind the `jwks` feature). Enforcing the allow/deny rules on the requested
//! hostname *string* alone is not sufficient: an allow-listed hostname can
//! resolve (or be made to resolve, e.g. via DNS rebinding) to a loopback,
//! link-local, cloud-metadata or otherwise private address. A direct URL to
//! that IP would be denied by capabilities, but a host-string check performed
//! before DNS resolution would let the request through (SSRF).
//!
//! To close that gap, this module provides [`FilteringResolver`], a
//! `reqwest`-compatible DNS resolver that re-checks every resolved IP address
//! against the configured allow/deny rules and blocks private/special-use
//! addresses unless they are explicitly allowed. It lives here, rather than in
//! `crate::http`, so the JWKS path can reuse it without depending on the `http`
//! feature.
//!
//! The entire module is gated to non-WASM targets because it depends on
//! `tokio`'s system DNS resolver; on WASM the outbound clients are built
//! without a custom resolver.

use std::error::Error;
#[cfg(feature = "http")]
use std::net::IpAddr;
use std::sync::Arc;

use ipnet::IpNet;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::net::lookup_host;

use crate::dbs::capabilities::{NetTarget, Targets};

/// The allow/deny network rules applied to outbound requests, shared between an
/// outbound client's redirect policy and its [`FilteringResolver`].
pub(crate) struct NetFilter {
	pub(crate) allow: Targets<NetTarget>,
	pub(crate) deny: Targets<NetTarget>,
}

/// A `reqwest` DNS resolver that enforces network capabilities at the IP level.
///
/// The hostname is first checked against the allow/deny rules, then resolved,
/// and finally every resolved address is checked again so that an allow-listed
/// hostname cannot be used to reach a private/special-use IP that is not itself
/// explicitly allowed.
pub(crate) struct FilteringResolver {
	pub(crate) filter: Arc<NetFilter>,
}

impl FilteringResolver {
	pub(crate) fn from_net_filter(filter: Arc<NetFilter>) -> Self {
		FilteringResolver {
			filter,
		}
	}
}

impl Resolve for FilteringResolver {
	fn resolve(&self, name: Name) -> Resolving {
		let filter = Arc::clone(&self.filter);
		let name_str = name.as_str().to_string();
		Box::pin(async move {
			// Check the domain name (if any) matches the allowlist. The name is
			// read as a host, never through the rule parser, so a name spelled
			// like a wildcard rule is matched as the one host it is.
			let parsed_host = url::Host::parse(&name_str)
				.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?;
			let name_target = NetTarget::Host(NetTarget::canonical_host(parsed_host), None);
			let name_is_allowed =
				filter.allow.matches(&name_target) && !filter.deny.matches(&name_target);
			// If the domain name itself is not allowed, return an error
			if !name_is_allowed {
				return Err(Box::new(crate::dbs::capabilities::Error::NetTargetNotAllowed(
					name_target.to_string(),
				)) as Box<dyn Error + Send + Sync>);
			}
			// Resolve the addresses
			let addrs: Vec<std::net::SocketAddr> = lookup_host((name_str, 0_u16))
				.await
				.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?
				.collect();
			// Check each resolved address against the deny list and private-IP
			// rules, collecting allowed addresses and tracking the first denied
			// address for error reporting.
			let mut allowed = Vec::new();
			let mut first_denied = None;
			for addr in addrs {
				let ip = addr.ip().to_canonical();
				let target = IpNet::from(ip);
				let ip_target = NetTarget::IPNet(target);
				if filter.deny.matches(&ip_target) {
					// Explicitly denied by configuration.
					if first_denied.is_none() {
						first_denied = Some(target);
					}
				} else if !matches!(filter.allow, Targets::All)
					&& common::net::is_special_use_ip(addr.ip())
					&& !filter.allow.matches(&ip_target)
				{
					// A special-use address that is not explicitly listed in the
					// allow rules is blocked even when the originating hostname
					// was allowed. Skipped when `allow_net = all`.
					//
					// The predicate is shared with the Surrealism DNS shim, which
					// filters what a guest's `wasi:sockets` lookup hands back; the
					// two enforcement points have to answer identically or a module
					// reaches what a query cannot. Its membership is the whole IANA
					// special-purpose registry, which is wider than "private" — the
					// documentation blocks and the multicast ranges are in it too.
					// Deliberately over-broad: refusing an address that is merely
					// unusual costs a connection that almost certainly should not
					// have been made, and admitting one that is not is an SSRF. A
					// deployment that needs one of these names it in `--allow-net`,
					// which is what the clause above checks.
					if first_denied.is_none() {
						first_denied = Some(target);
					}
				} else {
					allowed.push(addr);
				}
			}
			// If all addresses were denied, return a proper error
			if allowed.is_empty()
				&& let Some(denied) = first_denied
			{
				return Err(Box::new(crate::dbs::capabilities::Error::NetTargetNotAllowed(
					denied.to_string(),
				)) as Box<dyn Error + Send + Sync>);
			}
			Ok(Box::new(allowed.into_iter()) as Addrs)
		}) as Resolving
	}
}

/// Resolves a [`NetTarget`] to the target forms its addresses must be checked
/// against.
///
/// A `Host` target is resolved via DNS (port 80 when none is given) and each
/// resolved address yields **two** targets, so callers can check every address a
/// host actually points at against every rule shape:
///
/// * `NetTarget::IPNet` — matched by bare-IP and CIDR rules.
/// * `NetTarget::Host(<ip>, Some(port))` — matched by port-bearing rules (`--deny-net IP:PORT`),
///   which parse to `NetTarget::Host(_, Some(_))` and structurally never match an `IPNet`. Without
///   this form an `IP:PORT` deny rule matched no resolved address at all, so a request to a
///   hostname that resolves to the denied endpoint escaped it.
///
/// An `IPNet` target needs no resolution and yields an empty vector.
///
/// This resolves the *request* target, so an IP or CIDR deny rule is enforced
/// against every address a hostname points at. It does not resolve the *rule*:
/// a deny rule keyed on a hostname is matched only against the literal request
/// host (in `check_allowed_net`, before this resolution), so a request
/// addressed to the IP that hostname resolves to is not caught.
/// Deny an endpoint by IP/CIDR to block it however it is addressed.
#[cfg(feature = "http")]
pub(crate) async fn resolve_net_target(
	target: &NetTarget,
) -> Result<Vec<NetTarget>, std::io::Error> {
	match target {
		NetTarget::Host(h, p) => {
			let port = p.unwrap_or(80);
			let mut out = Vec::new();
			for a in tokio::net::lookup_host((h.to_string(), port)).await? {
				let ip = a.ip().to_canonical();
				out.push(NetTarget::IPNet(ip.into()));
				out.push(NetTarget::Host(net_target_host_from_ip(ip), Some(port)));
			}
			Ok(out)
		}
		NetTarget::IPNet(_) => Ok(vec![]),
		// A wildcard is only ever a rule; a request target is built from one
		// URL host, so there is nothing to resolve.
		NetTarget::Wildcard {
			..
		} => Ok(vec![]),
	}
}

/// Renders an [`IpAddr`] as the [`url::Host`] form a [`NetTarget::Host`] carries,
/// so a resolved address can be compared against a port-bearing rule whose own
/// host was written as an IP literal.
#[cfg(feature = "http")]
fn net_target_host_from_ip(ip: IpAddr) -> url::Host<String> {
	// `to_canonical` unwraps `::ffff:a.b.c.d` so a rule written as the IPv4
	// literal still matches an address delivered in IPv4-mapped form.
	match ip.to_canonical() {
		IpAddr::V4(v4) => url::Host::Ipv4(v4),
		IpAddr::V6(v6) => url::Host::Ipv6(v6),
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::sync::Arc;

	use reqwest::dns::{Name, Resolve};

	use super::{FilteringResolver, NetFilter};
	use crate::dbs::capabilities::{NetTarget, Target, Targets};

	/// Helper: create a `FilteringResolver` with the given allow/deny configuration.
	fn make_resolver(allow: Targets<NetTarget>, deny: Targets<NetTarget>) -> FilteringResolver {
		FilteringResolver::from_net_filter(Arc::new(NetFilter {
			allow,
			deny,
		}))
	}

	/// Verifies that resolved private/loopback IPs are blocked when they are not
	/// explicitly listed in `allow_net`, even if the originating hostname is.
	#[tokio::test]
	async fn test_filtering_resolver_private_ip_via_hostname_blocked() {
		// Allow only the *hostname* "localhost" — NOT the loopback IPs it resolves to.
		let resolver = make_resolver(
			Targets::Some([NetTarget::from_str("localhost").unwrap()].into()),
			Targets::None,
		);
		let name = Name::from_str("localhost").unwrap();
		let result = resolver.resolve(name).await;

		// "localhost" resolves to loopback (127.0.0.1 and/or ::1), both of
		// which are private and not explicitly listed in allow_net as IPs.
		// All resolved addresses must therefore be blocked.
		match result {
			Ok(_) => panic!(
				"Expected FilteringResolver to block private IP resolved from an allowed hostname"
			),
			Err(e) => assert!(
				e.to_string().contains("Access to network target"),
				"Expected a NetTargetNotAllowed error, got: {e}"
			),
		}
	}

	/// Verifies that resolution succeeds when `allow_net = all`, where private
	/// IP filtering is intentionally skipped.
	#[tokio::test]
	async fn test_filtering_resolver_private_ip_allowed_when_allow_all() {
		let resolver = make_resolver(Targets::All, Targets::None);
		let name = Name::from_str("localhost").unwrap();
		let result = resolver.resolve(name).await;
		assert!(result.is_ok(), "Expected resolution to succeed with allow_net = all");
	}

	// An `IP:PORT` deny rule parses to `NetTarget::Host(_, Some(_))`, which
	// structurally never matches a `NetTarget::IPNet`, so before the port-bearing
	// `Host` form was emitted it matched no resolved address and a request to a
	// hostname resolving to the denied endpoint escaped it. `resolve_net_target`
	// now emits each resolved address in port-bearing `Host` form so the rule is
	// enforced against every address the hostname points at.
	#[tokio::test]
	#[cfg(feature = "http")]
	async fn test_net_target_resolve_emits_port_bearing_form() {
		// `localhost:9999` resolves to the same endpoint an `IP:9999` rule names.
		let via_name = super::resolve_net_target(&NetTarget::from_str("localhost:9999").unwrap())
			.await
			.unwrap();
		// System configuration decides whether localhost resolves to v4, v6 or
		// both; requiring one loopback form keeps the test portable.
		let ipv4_rule = NetTarget::from_str("127.0.0.1:9999").unwrap();
		let ipv6_rule = NetTarget::from_str("[::1]:9999").unwrap();
		assert!(
			via_name.iter().any(|t| ipv4_rule.matches(t))
				|| via_name.iter().any(|t| ipv6_rule.matches(t)),
			"a port-bearing IP rule must match a hostname that resolves to it, got: {via_name:?}"
		);
		// An IP-literal target resolves to the same two forms, so a port-bearing
		// IP rule matches the resolved form of its own literal.
		let via_ip = super::resolve_net_target(&NetTarget::from_str("127.0.0.1:9999").unwrap())
			.await
			.unwrap();
		assert!(
			via_ip.iter().any(|t| ipv4_rule.matches(t)),
			"a port-bearing rule must match the resolved form of its own IP literal, got: {via_ip:?}"
		);
		// The boundary, asserted so it stays deliberate: only the *request*
		// target is resolved, never the *rule*. A hostname deny rule is matched
		// against the literal request host in `check_allowed_net`, so it does not
		// match the resolved (IP-form) targets here — a request addressed to the
		// IP a denied hostname resolves to is not caught. Block endpoints by
		// IP/CIDR, not by hostname.
		let hostname_rule = NetTarget::from_str("example.com:9999").unwrap();
		assert!(
			!via_ip.iter().any(|t| hostname_rule.matches(t)),
			"a hostname rule matches only the literal request host, not resolved IPs: {via_ip:?}"
		);
		// Negative control: the rule must stay port-specific. A different port
		// on the same host is not covered.
		let other_port = NetTarget::from_str("127.0.0.1:1234").unwrap();
		assert!(
			!via_ip.iter().any(|t| other_port.matches(t)),
			"a port-bearing rule must not match a different port, got: {via_ip:?}"
		);
		// Bare-IP and CIDR rules keep matching: the `IPNet` form is still emitted.
		let bare = NetTarget::from_str("127.0.0.1").unwrap();
		assert!(
			via_ip.iter().any(|t| bare.matches(t)),
			"a bare-IP rule must still match, got: {via_ip:?}"
		);
	}

	#[tokio::test]
	#[cfg(feature = "http")]
	async fn test_net_target_resolve_async() {
		// This test is dependent on system configuration.
		// Some systems don't configure localhost to have an ipv6 address, and some
		// don't resolve it to ipv4 either. We only require at least one loopback
		// address to be present.
		let r =
			super::resolve_net_target(&NetTarget::from_str("localhost").unwrap()).await.unwrap();
		let has_ipv4 = r.contains(&NetTarget::from_str("127.0.0.1").unwrap());
		let has_ipv6 = r.contains(&NetTarget::from_str("::1/128").unwrap());
		assert!(
			has_ipv4 || has_ipv6,
			"Expected localhost to resolve to at least 127.0.0.1 or ::1, got: {:?}",
			r
		);
	}
}
