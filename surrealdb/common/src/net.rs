//! Which IP addresses an outbound connection may not reach on a hostname's say-so.
//!
//! Two enforcement points ask this question and must answer it identically: the
//! engine's filtering resolver, which re-checks every address a SurrealQL
//! `http::*` call resolves to, and the Surrealism DNS shim, which filters what a
//! guest's `wasi:sockets` lookup hands back. Both exist to stop a name under
//! someone else's control from pointing at loopback, a link-local metadata
//! endpoint, or anything else the operator never meant to expose. A predicate
//! that differed between them would let a module reach what a query cannot, so
//! it lives here and neither keeps a copy.
//!
//! Membership is deliberately over-broad. Answering "special-use" for an address
//! that is merely unusual costs a connection that almost certainly should not
//! have been made; answering "ordinary" for one that is not is an SSRF.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` belongs to a special-use range that a resolved hostname must
/// not reach on its own.
///
/// Covers the IANA Special-Purpose Address Registries (RFC 5735 / 6890 / 4193 /
/// 3513): loopback, unspecified, private, link-local (which is where cloud
/// metadata endpoints live), shared address space, broadcast, multicast and the
/// documentation ranges.
///
/// The address is canonicalised first, so an IPv4-mapped form such as
/// `::ffff:127.0.0.1` is judged as the IPv4 address it carries. IPv6 transition
/// addresses that embed an IPv4 destination are judged on that destination too,
/// because a host with the matching connectivity routes to it.
pub fn is_special_use_ip(ip: IpAddr) -> bool {
	match ip.to_canonical() {
		IpAddr::V4(v4) => is_special_use_ipv4(v4),
		IpAddr::V6(v6) => {
			v6.is_loopback()
				|| v6.is_unspecified()
				|| v6.is_multicast()
				// Unique local, fc00::/7.
				|| (v6.segments()[0] & 0xFE00) == 0xFC00
				// Link-local, fe80::/10.
				|| (v6.segments()[0] & 0xFFC0) == 0xFE80
				// Documentation, 2001:db8::/32.
				|| (v6.segments()[0] == 0x2001 && v6.segments()[1] == 0x0db8)
				|| embedded_ipv4_of(&v6).is_some_and(is_special_use_ipv4)
		}
	}
}

/// The IPv4 address a connection to `ip` actually reaches, when `ip` is an IPv6
/// spelling that carries one.
///
/// Covers the IPv4-mapped form and the transition mechanisms in
/// `embedded_ipv4_of`. `None` for an address that is already IPv4, and for an
/// IPv6 address that names no IPv4 destination.
///
/// A caller deciding whether a rule authorises a connect has to ask this first:
/// a rule naming the outer IPv6 range says nothing about where the packet
/// lands, so `2000::/3` means "the public IPv6 internet" and not "every IPv4
/// address as well".
pub fn embedded_ipv4(ip: IpAddr) -> Option<Ipv4Addr> {
	match ip {
		IpAddr::V4(_) => None,
		IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
			Some(v4) => Some(v4),
			None => embedded_ipv4_of(&v6),
		},
	}
}

/// The IPv4 half of [`is_special_use_ip`], applied to an address that is
/// already canonical.
fn is_special_use_ipv4(v4: Ipv4Addr) -> bool {
	v4.is_loopback()        // 127.0.0.0/8
		|| v4.is_private()      // 10/8, 172.16/12, 192.168/16
		|| v4.is_link_local()   // 169.254.0.0/16
		|| v4.is_broadcast()    // 255.255.255.255
		|| v4.is_unspecified()  // 0.0.0.0
		|| v4.is_multicast()    // 224.0.0.0/4
		|| v4.is_documentation() // 192.0.2/24, 198.51.100/24, 203.0.113/24
		// Shared address space (RFC 6598): 100.64.0.0/10.
		|| (u32::from(v4) & 0xFFC0_0000) == 0x6440_0000
}

/// The IPv4 destination an IPv6 transition address carries, if it carries one.
///
/// [`IpAddr::to_canonical`] unwraps only IPv4-mapped addresses
/// (`::ffff:a.b.c.d`). The mechanisms below also name an IPv4 destination and,
/// on a host with the corresponding connectivity, route to it — so that is the
/// address a special-use check has to be applied to.
fn embedded_ipv4_of(v6: &Ipv6Addr) -> Option<Ipv4Addr> {
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ordinary_public_addresses_are_not_special() {
		for ip in [
			"1.1.1.1",
			"8.8.8.8",
			"93.184.216.34",
			"2606:2800:220:1:248:1893:25c8:1946",
			"2001:4860:4860::8888",
		] {
			assert!(!is_special_use_ip(ip.parse().unwrap()), "{ip}");
		}
	}

	#[test]
	fn the_special_use_ranges_are_covered() {
		for ip in [
			"127.0.0.1",       // loopback
			"127.255.255.255", // loopback, top of the /8
			"0.0.0.0",         // unspecified
			"10.1.2.3",        // private
			"172.16.0.1",      // private
			"172.31.255.255",  // private, top of the /12
			"192.168.1.1",     // private
			"169.254.0.1",     // link-local
			"169.254.169.254", // link-local, cloud metadata
			"100.64.0.1",      // shared address space
			"100.127.255.255", // shared address space, top of the /10
			"255.255.255.255", // broadcast
			"224.0.0.1",       // multicast
			"239.255.255.250", // multicast, SSDP
			"192.0.2.1",       // documentation, TEST-NET-1
			"198.51.100.1",    // documentation, TEST-NET-2
			"203.0.113.1",     // documentation, TEST-NET-3
			"::1",             // loopback
			"::",              // unspecified
			"fc00::1",         // unique local
			"fd00::1",         // unique local
			"fe80::1",         // link-local
			"ff02::1",         // multicast
			"2001:db8::1",     // documentation
		] {
			assert!(is_special_use_ip(ip.parse().unwrap()), "{ip}");
		}
	}

	/// Each range has an edge, and the address just outside it is ordinary.
	/// A predicate that over-reached here would refuse traffic nobody asked it
	/// to, which is how an over-broad filter turns into an outage.
	#[test]
	fn the_ranges_stop_where_they_stop() {
		for ip in [
			"100.128.0.1",     // shared address space ends at 100.127.255.255
			"192.0.3.1",       // one /24 past TEST-NET-1
			"198.51.101.1",    // one /24 past TEST-NET-2
			"203.0.114.1",     // one /24 past TEST-NET-3
			"223.255.255.255", // one address below the multicast /4
			"2001:4860::1",    // shares Teredo's first segment, not its second
			"2003:7f00:1::",   // adjacent to 6to4's 2002::/16, embeds nothing
			"64:ff9c::7f00:1", // NAT64 needs the full 64:ff9b:: /96 prefix
		] {
			assert!(!is_special_use_ip(ip.parse().unwrap()), "{ip}");
		}
	}

	/// The mapped form must not be a way around the IPv4 judgement, which is
	/// why the predicate canonicalises before deciding.
	#[test]
	fn ipv4_mapped_addresses_are_judged_as_ipv4() {
		for ip in [
			"::ffff:127.0.0.1",
			"::ffff:169.254.169.254",
			"::ffff:192.168.1.1",
			"::ffff:172.16.0.1",
		] {
			assert!(is_special_use_ip(ip.parse().unwrap()), "{ip}");
		}
		for ip in ["::ffff:93.184.216.34", "::ffff:1.1.1.1"] {
			assert!(!is_special_use_ip(ip.parse().unwrap()), "{ip}");
		}
	}

	/// The IPv4 destination is what a connect reaches, so that is what a rule
	/// has to name — whichever IPv6 spelling carries it.
	#[test]
	fn the_carried_destination_is_reported() {
		for (ip, expected) in [
			("::ffff:127.0.0.1", Some("127.0.0.1")),
			("64:ff9b::7f00:1", Some("127.0.0.1")),
			("2002:a9fe:a9fe::", Some("169.254.169.254")),
			("2001:0:0:0:0:0:f5ff:fdfc", Some("10.0.2.3")),
			("2606:2800:220:1:248:1893:25c8:1946", None),
			("93.184.216.34", None),
		] {
			assert_eq!(
				embedded_ipv4(ip.parse().unwrap()),
				expected.map(|e| e.parse().unwrap()),
				"{ip}"
			);
		}
	}

	/// `to_canonical` unwraps only the IPv4-mapped form, so each transition
	/// mechanism is unwrapped explicitly: on a host with the matching
	/// connectivity they route to the address they carry, and none of the IPv6
	/// checks above match their prefixes. A mechanism carrying an ordinary
	/// public destination stays reachable.
	#[test]
	fn transition_addresses_are_judged_on_their_embedded_destination() {
		for (ip, special, what) in [
			// NAT64, 64:ff9b::/96.
			("64:ff9b::7f00:1", true, "127.0.0.1"),
			("64:ff9b::a9fe:a9fe", true, "169.254.169.254"),
			("64:ff9b::a00:1", true, "10.0.0.1"),
			("64:ff9b::101:101", false, "1.1.1.1"),
			("64:ff9b::5db8:d822", false, "93.184.216.34"),
			// 6to4, 2002::/16.
			("2002:a9fe:a9fe::", true, "169.254.169.254"),
			("2002:7f00:1::", true, "127.0.0.1"),
			("2002:c0a8:101::", true, "192.168.1.1"),
			("2002:101:101::", false, "1.1.1.1"),
			// Teredo, 2001:0::/32 — the low 32 bits are the destination XOR
			// 0xffffffff.
			("2001:0:4136:e378:8000:63bf:f5ff:fffe", true, "10.0.0.1"),
			("2001:0:4136:e378:8000:63bf:80ff:fffe", true, "127.0.0.1"),
			("2001:0:4136:e378:8000:63bf:fefe:fefe", false, "1.1.1.1"),
		] {
			assert_eq!(is_special_use_ip(ip.parse().unwrap()), special, "{ip} carries {what}");
		}
	}
}
