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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
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

/// Returns `true` for IP addresses that belong to private, loopback, link-local,
/// or other special-use ranges defined in the IANA Special-Purpose Address
/// Registries (RFC 5735 / RFC 6890 / RFC 4193 / RFC 3513).
fn is_private_ip(ip: IpAddr) -> bool {
	match ip.to_canonical() {
		IpAddr::V4(v4) => is_private_ipv4(v4),
		IpAddr::V6(v6) => {
			v6.is_loopback()       // ::1
				|| v6.is_unspecified() // ::
				// Unique local (fc00::/7)
				|| (v6.segments()[0] & 0xFE00) == 0xFC00
				// Link-local (fe80::/10)
				|| (v6.segments()[0] & 0xFFC0) == 0xFE80
				// Transition addresses that embed an IPv4 destination
				|| embedded_ipv4(&v6).is_some_and(is_private_ipv4)
		}
	}
}

/// Whether an IPv4 address is private, loopback or otherwise special-use.
fn is_private_ipv4(v4: Ipv4Addr) -> bool {
	v4.is_loopback()      // 127.0.0.0/8
		|| v4.is_private()    // 10/8, 172.16/12, 192.168/16
		|| v4.is_link_local() // 169.254.0.0/16
		|| v4.is_broadcast()  // 255.255.255.255
		|| v4.is_unspecified() // 0.0.0.0
		// Shared address space (RFC 6598): 100.64.0.0/10
		|| (u32::from(v4) & 0xFFC0_0000) == 0x6440_0000
}

/// Extracts the IPv4 destination embedded in an IPv6 transition address.
///
/// `IpAddr::to_canonical` only unwraps IPv4-mapped addresses (`::ffff:a.b.c.d`).
/// The transition mechanisms below also carry an IPv4 destination and, on a host
/// with the corresponding connectivity, route to it — so the embedded address is
/// what a private/special-use check must be applied to. Returns `None` for an
/// address that carries no embedded IPv4 destination.
fn embedded_ipv4(v6: &Ipv6Addr) -> Option<Ipv4Addr> {
	let s = v6.segments();
	let v4 = |hi: u16, lo: u16| Ipv4Addr::from(((hi as u32) << 16) | lo as u32);
	match s {
		// NAT64 well-known prefix (RFC 6052): 64:ff9b::/96, low 32 bits = IPv4.
		[0x0064, 0xff9b, 0, 0, 0, 0, hi, lo] => Some(v4(hi, lo)),
		// 6to4 (RFC 3056): 2002::/16, bits 16-47 = IPv4.
		[0x2002, hi, lo, ..] => Some(v4(hi, lo)),
		// Teredo (RFC 4380): 2001:0::/32, low 32 bits = IPv4 XOR 0xffffffff.
		[0x2001, 0x0000, .., hi, lo] => Some(v4(hi ^ 0xffff, lo ^ 0xffff)),
		_ => None,
	}
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
			// Check the domain name (if any) matches the allowlist
			let name_target = NetTarget::from_str(&name_str)
				.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?;
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
				let target = IpNet::from(addr.ip());
				let ip_target = NetTarget::IPNet(target);
				if filter.deny.matches(&ip_target) {
					// Explicitly denied by configuration.
					if first_denied.is_none() {
						first_denied = Some(target);
					}
				} else if !matches!(filter.allow, Targets::All)
					&& is_private_ip(addr.ip())
					&& !filter.allow.matches(&ip_target)
				{
					// A private/special IP that is not explicitly listed in the
					// allow rules is blocked even when the originating hostname
					// was allowed.  Skipped when `allow_net = all`.
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
				let ip = a.ip();
				out.push(NetTarget::IPNet(ip.into()));
				out.push(NetTarget::Host(net_target_host_from_ip(ip), Some(port)));
			}
			Ok(out)
		}
		NetTarget::IPNet(_) => Ok(vec![]),
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
	use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
	use std::str::FromStr;
	use std::sync::Arc;

	use reqwest::dns::{Name, Resolve};

	use super::{FilteringResolver, NetFilter, is_private_ip};
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

	#[test]
	fn test_is_private_ip_loopback() {
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(127, 255, 255, 255))));
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
	}

	#[test]
	fn test_is_private_ip_rfc1918() {
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(172, 31, 255, 255))));
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
	}

	#[test]
	fn test_is_private_ip_link_local() {
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1))));
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))));
		// IPv6 link-local (fe80::/10)
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1))));
	}

	#[test]
	fn test_is_private_ip_shared_address_space() {
		// 100.64.0.0/10 (RFC 6598)
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(100, 127, 255, 255))));
		assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(100, 128, 0, 0))));
	}

	#[test]
	fn test_is_private_ip_unspecified() {
		assert!(is_private_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
	}

	#[test]
	fn test_is_private_ip_ipv6_unique_local() {
		// fc00::/7
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1))));
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1))));
	}

	#[test]
	fn test_is_private_ip_ipv4_mapped_ipv6() {
		// IPv4-mapped IPv6 addresses (::ffff:x.x.x.x) must be treated the same
		// as their IPv4 equivalents after canonicalisation.
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x7f00, 0x0001)))); // ::ffff:127.0.0.1
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc0a8, 0x0101)))); // ::ffff:192.168.1.1
		assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xac10, 0x0001)))); // ::ffff:172.16.0.1
		assert!(!is_private_ip(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0101, 0x0101)))); // ::ffff:1.1.1.1
	}

	// `to_canonical` only unwraps IPv4-mapped addresses, so transition
	// mechanisms that embed an IPv4 destination must be unwrapped explicitly:
	// on a host with the matching connectivity they route to that address, and
	// the IPv6 special-use checks (loopback / unspecified / ULA / link-local)
	// match none of these prefixes.
	#[test]
	fn test_is_private_ip_nat64_embedded_ipv4() {
		// 64:ff9b::7f00:1 -> 127.0.0.1
		assert!(is_private_ip(IpAddr::V6("64:ff9b::7f00:1".parse().unwrap())));
		// 64:ff9b::a9fe:a9fe -> 169.254.169.254 (cloud instance metadata)
		assert!(is_private_ip(IpAddr::V6("64:ff9b::a9fe:a9fe".parse().unwrap())));
		// 64:ff9b::a00:1 -> 10.0.0.1
		assert!(is_private_ip(IpAddr::V6("64:ff9b::a00:1".parse().unwrap())));
		// A NAT64 address embedding a public IPv4 stays allowed.
		// 64:ff9b::101:101 -> 1.1.1.1
		assert!(!is_private_ip(IpAddr::V6("64:ff9b::101:101".parse().unwrap())));
	}

	#[test]
	fn test_is_private_ip_6to4_embedded_ipv4() {
		// 2002:a9fe:a9fe:: -> 169.254.169.254
		assert!(is_private_ip(IpAddr::V6("2002:a9fe:a9fe::".parse().unwrap())));
		// 2002:7f00:1:: -> 127.0.0.1
		assert!(is_private_ip(IpAddr::V6("2002:7f00:1::".parse().unwrap())));
		// 2002:c0a8:101:: -> 192.168.1.1
		assert!(is_private_ip(IpAddr::V6("2002:c0a8:101::".parse().unwrap())));
		// 2002:101:101:: -> 1.1.1.1, public, stays allowed.
		assert!(!is_private_ip(IpAddr::V6("2002:101:101::".parse().unwrap())));
	}

	#[test]
	fn test_is_private_ip_teredo_embedded_ipv4() {
		// Low 32 bits are the IPv4 destination XOR 0xffffffff.
		// 10.0.0.1 -> f5ff:fffe
		assert!(is_private_ip(IpAddr::V6("2001:0:4136:e378:8000:63bf:f5ff:fffe".parse().unwrap())));
		// 127.0.0.1 -> 80ff:fffe
		assert!(is_private_ip(IpAddr::V6("2001:0:4136:e378:8000:63bf:80ff:fffe".parse().unwrap())));
		// 1.1.1.1 -> fefe:fefe, public, stays allowed.
		assert!(!is_private_ip(IpAddr::V6(
			"2001:0:4136:e378:8000:63bf:fefe:fefe".parse().unwrap()
		)));
	}

	#[test]
	fn test_is_private_ip_non_transition_ipv6_unaffected() {
		// `2001:db8::/32` shares Teredo's first segment but not its second, so
		// it must not be misread as carrying an embedded IPv4 destination.
		assert!(!is_private_ip(IpAddr::V6("2001:db8::1".parse().unwrap())));
		// `2003::/16` is adjacent to 6to4's `2002::/16` and carries no embedding.
		assert!(!is_private_ip(IpAddr::V6("2003:7f00:1::".parse().unwrap())));
		// The NAT64 prefix requires the full 64:ff9b:: 96-bit prefix; a
		// same-first-segment address with a different second segment does not
		// embed anything.
		assert!(!is_private_ip(IpAddr::V6("64:ff9c::7f00:1".parse().unwrap())));
	}

	#[test]
	fn test_is_private_ip_public_addresses() {
		// Public IPs must not be flagged as private
		assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
		assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
		assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))));
		assert!(!is_private_ip(IpAddr::V6(Ipv6Addr::new(
			0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888
		))));
		// 100.128.0.0 is NOT in shared address space (RFC 6598 ends at 100.127.255.255)
		assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1))));
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
