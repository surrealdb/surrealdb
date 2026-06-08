use std::error::Error;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use ipnet::IpNet;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::net::lookup_host;

use super::NetFilter;
use crate::dbs::capabilities::{NetTarget, Targets};

/// Returns `true` for IP addresses that belong to private, loopback, link-local,
/// or other special-use ranges defined in the IANA Special-Purpose Address
/// Registries (RFC 5735 / RFC 6890 / RFC 4193 / RFC 3513).
fn is_private_ip(ip: IpAddr) -> bool {
	match ip.to_canonical() {
		IpAddr::V4(v4) => {
			v4.is_loopback()      // 127.0.0.0/8
				|| v4.is_private()    // 10/8, 172.16/12, 192.168/16
				|| v4.is_link_local() // 169.254.0.0/16
				|| v4.is_broadcast()  // 255.255.255.255
				|| v4.is_unspecified() // 0.0.0.0
				// Shared address space (RFC 6598): 100.64.0.0/10
				|| (u32::from(v4) & 0xFFC0_0000) == 0x6440_0000
		}
		IpAddr::V6(v6) => {
			v6.is_loopback()       // ::1
				|| v6.is_unspecified() // ::
				// Unique local (fc00::/7)
				|| (v6.segments()[0] & 0xFE00) == 0xFC00
				// Link-local (fe80::/10)
				|| (v6.segments()[0] & 0xFFC0) == 0xFE80
		}
	}
}

pub struct FilteringResolver {
	pub filter: Arc<NetFilter>,
}

impl FilteringResolver {
	pub fn from_net_filter(filter: Arc<NetFilter>) -> Self {
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
				return Err(
					Box::new(crate::err::Error::NetTargetNotAllowed(name_target.to_string()))
						as Box<dyn Error + Send + Sync>,
				);
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
				return Err(Box::new(crate::err::Error::NetTargetNotAllowed(denied.to_string()))
					as Box<dyn Error + Send + Sync>);
			}
			Ok(Box::new(allowed.into_iter()) as Addrs)
		}) as Resolving
	}
}

#[cfg(test)]
mod tests {
	use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
	use std::str::FromStr;
	use std::sync::Arc;

	use reqwest::dns::{Name, Resolve};

	use super::{FilteringResolver, NetFilter, is_private_ip};
	use crate::dbs::capabilities::{NetTarget, Targets};

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
}
