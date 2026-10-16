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
//!
//! The same two enforcement points also share the meaning of a wildcard host
//! rule, `*.<domain>[:<port>]`: [`split_wildcard_host`] decides which spellings
//! are one, and [`host_is_under`] and [`suffix_covers`] decide what one
//! matches. A rule that admitted a name at one point and refused it at the
//! other would again let a module reach what a query cannot.

use std::fmt;
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

/// Why a network target containing `*` was refused as a wildcard host rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WildcardHostError {
	/// `*` or `*.` with no domain after it, which would match every host.
	NoDomain,
	/// A `*` anywhere other than one leading `*.` label, e.g. `a.*.com` or
	/// `*foo.com`.
	MisplacedWildcard,
	/// The part after `*.` is not a bare `<domain>[:<port>]`: it carries a
	/// path, a CIDR mask, user info, brackets or another URL component.
	NotAHostPattern,
	/// The part after `*.` is an IP address. Addresses are matched by CIDR, not
	/// by label.
	IpAddress,
	/// The domain after `*.` has a single label, e.g. `*.com`, which would
	/// match a whole top-level domain.
	SingleLabel,
	/// The domain after `*.` has an empty label, e.g. `*.foo..bar`.
	EmptyLabel,
	/// The port after the domain is not a number in `0..=65535`.
	InvalidPort,
}

impl fmt::Display for WildcardHostError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let reason = match self {
			Self::NoDomain => "a wildcard must be followed by a domain, as in '*.example.com'",
			Self::MisplacedWildcard => {
				"a wildcard is only allowed as the whole leftmost label, as in '*.example.com'"
			}
			Self::NotAHostPattern => {
				"a wildcard host takes the form '*.<domain>[:<port>]', with no path, mask or other URL part"
			}
			Self::IpAddress => {
				"a wildcard cannot be applied to an IP address; use a CIDR block such as '10.0.0.0/8'"
			}
			Self::SingleLabel => {
				"a wildcard must be followed by a domain of at least two labels, as in '*.example.com'"
			}
			Self::EmptyLabel => "a wildcard host cannot contain an empty label",
			Self::InvalidPort => "a wildcard host has an invalid port",
		};
		f.write_str(reason)
	}
}

impl std::error::Error for WildcardHostError {}

/// Splits a wildcard host rule `*.<domain>[:<port>]` into its domain and port.
///
/// Returns `Ok(None)` when `entry` contains no `*`: it is not a wildcard rule
/// and the caller parses it as an ordinary host, IP or CIDR. Every other entry
/// containing a `*` is either a well-formed wildcard or an error, so a `*` can
/// never fall through to an exact host rule that matches nothing.
///
/// The returned domain is as written. The caller normalises it through its URL
/// host parser (lowercase, IDNA to punycode), rejects an IP address with
/// [`WildcardHostError::IpAddress`], and passes the result to
/// [`check_wildcard_suffix`].
pub fn split_wildcard_host(entry: &str) -> Result<Option<(&str, Option<u16>)>, WildcardHostError> {
	if !entry.contains('*') {
		return Ok(None);
	}
	if entry == "*" || entry == "*." {
		return Err(WildcardHostError::NoDomain);
	}
	let Some(rest) = entry.strip_prefix("*.") else {
		return Err(WildcardHostError::MisplacedWildcard);
	};
	if rest.contains('*') {
		return Err(WildcardHostError::MisplacedWildcard);
	}
	if rest.contains(['/', '?', '#', '@', '[', ']', '\\']) || rest.chars().any(char::is_whitespace)
	{
		return Err(WildcardHostError::NotAHostPattern);
	}
	let (domain, port) = match rest.split_once(':') {
		None => (rest, None),
		Some((domain, port)) => {
			let port = port.parse::<u16>().map_err(|_| WildcardHostError::InvalidPort)?;
			(domain, Some(port))
		}
	};
	if domain.is_empty() || domain == "." {
		return Err(WildcardHostError::NoDomain);
	}
	Ok(Some((domain, port)))
}

/// Checks a normalised wildcard suffix — lowercase ASCII, without a trailing
/// dot — and returns it unchanged.
///
/// At least two labels are required, so `*.com` is refused. Nothing beyond
/// that is consulted: `*.co.uk` is accepted, and naming a public suffix is the
/// operator's choice.
pub fn check_wildcard_suffix(suffix: &str) -> Result<&str, WildcardHostError> {
	if suffix.split('.').any(str::is_empty) {
		return Err(WildcardHostError::EmptyLabel);
	}
	if !suffix.contains('.') {
		return Err(WildcardHostError::SingleLabel);
	}
	Ok(suffix)
}

/// A DNS name without the trailing dot of its fully-qualified spelling, so
/// `example.com.` and `example.com` compare equal.
pub fn trim_root_dot(name: &str) -> &str {
	name.strip_suffix('.').unwrap_or(name)
}

/// Whether `host` lies strictly below `suffix`: it ends in `.<suffix>` and has
/// at least one non-empty label before it.
///
/// `suffix` is a normalised wildcard suffix (see [`check_wildcard_suffix`]).
/// `host` is compared ASCII-case-insensitively, after dropping a trailing dot.
/// Both sides are expected in the ASCII (punycode) form a URL host parser
/// produces; a Unicode spelling compares unequal and so fails closed.
///
/// With `suffix = "google.com"`: `www.google.com` and `a.b.google.com` are
/// below it; `google.com`, `evilgoogle.com` and `google.com.evil.com` are not.
pub fn host_is_under(host: &str, suffix: &str) -> bool {
	let host = trim_root_dot(host).as_bytes();
	let suffix = suffix.as_bytes();
	// At least one byte of label plus the separating dot.
	if host.len() < suffix.len() + 2 {
		return false;
	}
	let split = host.len() - suffix.len();
	let (head, tail) = host.split_at(split);
	tail.eq_ignore_ascii_case(suffix)
		&& head.last() == Some(&b'.')
		&& head.len() >= 2
		&& head[head.len() - 2] != b'.'
		&& head[0] != b'.'
}

/// Whether the wildcard `*.<outer>` matches every host the wildcard
/// `*.<inner>` matches: `inner` is `outer` itself or lies below it.
///
/// Both are normalised wildcard suffixes. `*.example.com` covers `*.example.com`
/// and `*.api.example.com`, and not `*.com` or `*.notexample.com`.
pub fn suffix_covers(outer: &str, inner: &str) -> bool {
	inner.eq_ignore_ascii_case(outer) || host_is_under(inner, outer)
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

	#[test]
	fn a_wildcard_matches_hosts_strictly_below_it_at_a_label_boundary() {
		for host in ["www.google.com", "a.b.google.com", "WWW.Google.COM", "www.google.com."] {
			assert!(host_is_under(host, "google.com"), "{host}");
		}
		for host in [
			"google.com",
			"google.com.",
			"evilgoogle.com",
			"google.com.evil.com",
			".google.com",
			"a..google.com",
			"com",
			"",
		] {
			assert!(!host_is_under(host, "google.com"), "{host}");
		}
	}

	#[test]
	fn a_wildcard_suffix_covers_itself_and_narrower_suffixes() {
		assert!(suffix_covers("example.com", "example.com"));
		assert!(suffix_covers("example.com", "api.example.com"));
		assert!(suffix_covers("example.com", "EU.API.example.com"));
		assert!(!suffix_covers("api.example.com", "example.com"));
		assert!(!suffix_covers("example.com", "notexample.com"));
		assert!(!suffix_covers("example.com", "com"));
	}

	#[test]
	fn wildcard_host_spellings_split_or_are_refused() {
		assert_eq!(split_wildcard_host("example.com"), Ok(None));
		assert_eq!(split_wildcard_host("10.0.0.0/8"), Ok(None));
		assert_eq!(split_wildcard_host("*.example.com"), Ok(Some(("example.com", None))));
		assert_eq!(split_wildcard_host("*.example.com:443"), Ok(Some(("example.com", Some(443)))));
		assert_eq!(split_wildcard_host("*.example.com."), Ok(Some(("example.com.", None))));

		for (entry, err) in [
			("*", WildcardHostError::NoDomain),
			("*.", WildcardHostError::NoDomain),
			("*.:80", WildcardHostError::NoDomain),
			("a.*.com", WildcardHostError::MisplacedWildcard),
			("*foo.com", WildcardHostError::MisplacedWildcard),
			("foo.*", WildcardHostError::MisplacedWildcard),
			("**.foo.com", WildcardHostError::MisplacedWildcard),
			("*.*.foo.com", WildcardHostError::MisplacedWildcard),
			("10.*.0.0/16", WildcardHostError::MisplacedWildcard),
			("*.10.0.0.0/8", WildcardHostError::NotAHostPattern),
			("*.foo.com/path", WildcardHostError::NotAHostPattern),
			("*.user@foo.com", WildcardHostError::NotAHostPattern),
			("*.[::1]", WildcardHostError::NotAHostPattern),
			("*.foo.com:http", WildcardHostError::InvalidPort),
			("*.foo.com:70000", WildcardHostError::InvalidPort),
			("*.foo.com:80:80", WildcardHostError::InvalidPort),
		] {
			assert_eq!(split_wildcard_host(entry), Err(err), "{entry}");
		}
	}

	#[test]
	fn a_wildcard_suffix_needs_two_non_empty_labels() {
		assert_eq!(check_wildcard_suffix("example.com"), Ok("example.com"));
		assert_eq!(check_wildcard_suffix("co.uk"), Ok("co.uk"));
		assert_eq!(check_wildcard_suffix("com"), Err(WildcardHostError::SingleLabel));
		assert_eq!(check_wildcard_suffix("foo..com"), Err(WildcardHostError::EmptyLabel));
		assert_eq!(check_wildcard_suffix(".com"), Err(WildcardHostError::EmptyLabel));
	}
}
