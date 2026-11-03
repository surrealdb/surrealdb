//! The PostgreSQL `inet` and `cidr` types: reading an address with an optional prefix, writing it
//! in PostgreSQL's canonical text, and the order PostgreSQL sorts them in.
//!
//! An `inet` keeps the host bits it was written with and drops a prefix that covers the whole
//! address (`/32` or `/128`). A `cidr` refuses host bits set to the right of its prefix and always
//! writes the prefix. Input without a prefix has the full-length prefix.

use std::cmp::Ordering;

/// The address family of a network value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
	V4,
	V6,
}

impl Family {
	/// The number of address bits, which is also the longest prefix.
	pub fn max_bits(self) -> u8 {
		match self {
			Family::V4 => 32,
			Family::V6 => 128,
		}
	}

	/// The number of address octets the family uses.
	pub fn octets(self) -> usize {
		match self {
			Family::V4 => 4,
			Family::V6 => 16,
		}
	}

	/// The PostgreSQL family code of the binary `inet` format: `AF_INET` (2) or `AF_INET6` (3).
	pub fn pg_code(self) -> u8 {
		match self {
			Family::V4 => 2,
			Family::V6 => 3,
		}
	}
}

/// An address with its prefix. Only the first `family.octets()` bytes of `address` are used; the
/// rest are zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Network {
	pub family: Family,
	pub address: [u8; 16],
	/// The prefix length, between 0 and `family.max_bits()`.
	pub bits: u8,
}

/// A network text that does not read, or a `cidr` with host bits set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkError {
	pub reason: String,
}

impl std::fmt::Display for NetworkError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.reason)
	}
}

impl Network {
	/// The text PostgreSQL writes for an `inet`: the address, then `/bits` unless the prefix is
	/// the full length.
	pub fn inet_text(&self) -> String {
		let address = format_address(self.family, &self.address);
		if self.bits == self.family.max_bits() {
			address
		} else {
			format!("{address}/{}", self.bits)
		}
	}

	/// The text PostgreSQL writes for a `cidr`: the address and `/bits`, always.
	pub fn cidr_text(&self) -> String {
		format!("{}/{}", format_address(self.family, &self.address), self.bits)
	}
}

/// Read an address with an optional prefix, as PostgreSQL's `inet` input does. Surrounding
/// whitespace is ignored. Host bits are kept.
pub fn parse(text: &str) -> Result<Network, NetworkError> {
	let refuse = || NetworkError {
		reason: format!("invalid network address \"{text}\""),
	};
	let trimmed = text.trim();
	let (address_text, prefix_text) = match trimmed.split_once('/') {
		Some((address, prefix)) => (address, Some(prefix)),
		None => (trimmed, None),
	};
	let (family, address) = if address_text.contains(':') {
		(Family::V6, parse_v6(address_text).ok_or_else(refuse)?)
	} else {
		let (address, components) = parse_v4(address_text).ok_or_else(refuse)?;
		// An IPv4 address with fewer than four components, its missing ones zero, is read only
		// with a prefix, as in `10/8` or `192.168.1/24`.
		if components < 4 && prefix_text.is_none() {
			return Err(refuse());
		}
		(Family::V4, address)
	};
	let bits = match prefix_text {
		None => family.max_bits(),
		Some(digits) => {
			let valid = !digits.is_empty()
				&& digits.len() <= 3
				&& digits.bytes().all(|b| b.is_ascii_digit());
			let bits = digits.parse::<u8>().ok().filter(|_| valid).ok_or_else(refuse)?;
			if bits > family.max_bits() {
				return Err(refuse());
			}
			bits
		}
	};
	Ok(Network {
		family,
		address,
		bits,
	})
}

/// Read a `cidr`: as [`parse`], and refuse an address with a host bit set to the right of its
/// prefix. An IPv4 address without a prefix takes its [classful prefix](classful_prefix).
pub fn parse_cidr(text: &str) -> Result<Network, NetworkError> {
	let network = match classful_prefix(text.trim()) {
		Some(bits) => parse(&format!("{}/{bits}", text.trim()))?,
		None => parse(text)?,
	};
	if host_bits_set(&network) {
		return Err(NetworkError {
			reason: format!(
				"invalid cidr value \"{}\": Value has bits set to right of mask",
				text.trim()
			),
		});
	}
	Ok(network)
}

/// The prefix PostgreSQL's `cidr` input gives an IPv4 address written without one: the mask of its
/// class in the classful numbering (8 bits below 128, 16 below 192, 24 below 224, 8 for a
/// multicast address, 32 from 240, and 4 for a lone `224`), widened to cover every octet written.
/// `None` when the text has a prefix or is not such an address.
fn classful_prefix(text: &str) -> Option<u8> {
	if text.contains(['/', ':']) {
		return None;
	}
	let (address, components) = parse_v4(text)?;
	let first = *address.first()?;
	let class = if first >= 240 {
		32
	} else if first >= 224 {
		8
	} else if first >= 192 {
		24
	} else if first >= 128 {
		16
	} else {
		8
	};
	let written = u8::try_from(components * 8).ok()?;
	let bits = class.max(written);
	Some(if bits == 8 && first == 224 {
		4
	} else {
		bits
	})
}

/// Whether an address has a bit set to the right of its prefix.
pub fn host_bits_set(network: &Network) -> bool {
	(network.bits..network.family.max_bits()).any(|index| bit(&network.address, index))
}

/// The order PostgreSQL sorts networks in: the family (IPv4 first), then the address bits both
/// prefixes cover, then the shorter prefix, then the whole address.
pub fn compare(a: &Network, b: &Network) -> Ordering {
	if a.family != b.family {
		return a.family.pg_code().cmp(&b.family.pg_code());
	}
	let shared = a.bits.min(b.bits);
	let by_shared = compare_bits(&a.address, &b.address, shared);
	if by_shared != Ordering::Equal {
		return by_shared;
	}
	let by_prefix = a.bits.cmp(&b.bits);
	if by_prefix != Ordering::Equal {
		return by_prefix;
	}
	compare_bits(&a.address, &b.address, a.family.max_bits())
}

/// A key whose byte order is the order of [`compare`]: the family code, the address with the bits
/// to the right of the prefix cleared, the prefix length, then the whole address.
///
/// Two networks agree on the bits their shorter prefix covers exactly when their cleared addresses
/// agree on those bits, and where they do the network with the shorter prefix has the smaller
/// cleared address or an equal one, so the cleared address decides as the shared bits and then
/// the prefix lengths do.
pub fn sort_key(network: &Network) -> Vec<u8> {
	let octets = network.family.octets();
	let mut key = Vec::with_capacity(2 + 2 * octets);
	key.push(network.family.pg_code());
	key.extend((0..octets).map(|index| {
		let octet = network.address[index];
		let covered = usize::from(network.bits).saturating_sub(index * 8).min(8);
		let mask = if covered == 0 {
			0
		} else {
			0xffu8 << (8 - covered)
		};
		octet & mask
	}));
	key.push(network.bits);
	key.extend_from_slice(&network.address[..octets]);
	key
}

/// The first `count` bits of `a` against those of `b`, most significant first.
fn compare_bits(a: &[u8; 16], b: &[u8; 16], count: u8) -> Ordering {
	(0..count)
		.map(|index| bit(a, index).cmp(&bit(b, index)))
		.find(|ordering| *ordering != Ordering::Equal)
		.unwrap_or(Ordering::Equal)
}

/// The bit at `index`, counted from the most significant bit of the first octet.
fn bit(address: &[u8; 16], index: u8) -> bool {
	let octet = usize::from(index / 8);
	let mask = 0x80u8 >> (index % 8);
	address.get(octet).is_some_and(|byte| byte & mask != 0)
}

/// Read a dotted IPv4 address of one to four components, the missing trailing ones zero, with
/// the number of components given. Each octet is decimal, at most three digits, with no leading
/// zero.
fn parse_v4(text: &str) -> Option<([u8; 16], usize)> {
	let parts: Vec<&str> = text.split('.').collect();
	let components = parts.len();
	if !(1..=4).contains(&components) {
		return None;
	}
	let mut address = [0u8; 16];
	for (slot, part) in address.iter_mut().zip(parts) {
		let well_formed = !part.is_empty()
			&& part.len() <= 3
			&& part.bytes().all(|b| b.is_ascii_digit())
			&& (part.len() == 1 || !part.starts_with('0'));
		if !well_formed {
			return None;
		}
		*slot = part.parse::<u8>().ok()?;
	}
	Some((address, components))
}

/// Read an IPv6 address: hexadecimal groups, at most one `::`, and optionally a dotted quad as
/// the last 32 bits.
fn parse_v6(text: &str) -> Option<[u8; 16]> {
	let (head, tail, has_gap) = match text.find("::") {
		Some(index) => {
			let tail = &text[index + 2..];
			if tail.contains("::") {
				return None;
			}
			(&text[..index], tail, true)
		}
		None => (text, "", false),
	};
	let head_words = v6_words(head, !has_gap)?;
	let tail_words = v6_words(tail, true)?;
	let explicit = head_words.len() + tail_words.len();
	let words: Vec<u16> = if has_gap {
		// A `::` stands for at least one group of zeros.
		if explicit > 7 {
			return None;
		}
		let zeros = 8 - explicit;
		head_words.into_iter().chain(std::iter::repeat_n(0, zeros)).chain(tail_words).collect()
	} else {
		if explicit != 8 {
			return None;
		}
		head_words
	};
	let mut address = [0u8; 16];
	for (index, word) in words.iter().enumerate() {
		let [high, low] = word.to_be_bytes();
		address[index * 2] = high;
		address[index * 2 + 1] = low;
	}
	Some(address)
}

/// The groups of one side of a `::`. A dotted quad, when `allow_v4` holds, is its last two
/// groups, and it must be the last piece of the address.
fn v6_words(part: &str, allow_v4: bool) -> Option<Vec<u16>> {
	if part.is_empty() {
		return Some(Vec::new());
	}
	let pieces: Vec<&str> = part.split(':').collect();
	let last = pieces.len() - 1;
	let mut words = Vec::new();
	for (index, piece) in pieces.iter().enumerate() {
		if piece.contains('.') {
			if !allow_v4 || index != last {
				return None;
			}
			let (quad, 4) = parse_v4(piece)? else {
				return None;
			};
			words.push(u16::from_be_bytes([quad[0], quad[1]]));
			words.push(u16::from_be_bytes([quad[2], quad[3]]));
		} else {
			let well_formed = !piece.is_empty()
				&& piece.len() <= 4
				&& piece.bytes().all(|b| b.is_ascii_hexdigit());
			if !well_formed {
				return None;
			}
			words.push(u16::from_str_radix(piece, 16).ok()?);
		}
	}
	Some(words)
}

/// Write an address as PostgreSQL does: dotted quad for IPv4, and for IPv6 lower-case hex groups
/// with the longest run of two or more zero groups shortened to `::`. An IPv4-mapped address
/// (`::ffff:a.b.c.d`) and an IPv4-compatible one (`::a.b.c.d`) end in a dotted quad.
pub fn format_address(family: Family, address: &[u8; 16]) -> String {
	match family {
		Family::V4 => format!("{}.{}.{}.{}", address[0], address[1], address[2], address[3]),
		Family::V6 => format_v6(address),
	}
}

fn format_v6(bytes: &[u8; 16]) -> String {
	let words: [u16; 8] =
		std::array::from_fn(|index| u16::from_be_bytes([bytes[index * 2], bytes[index * 2 + 1]]));
	// The first longest run of zero words, if it is two words or longer.
	let mut best: Option<(usize, usize)> = None;
	let mut current: Option<(usize, usize)> = None;
	for (index, word) in words.iter().enumerate() {
		if *word == 0 {
			current = Some(match current {
				Some((base, len)) => (base, len + 1),
				None => (index, 1),
			});
		} else if let Some(run) = current.take()
			&& best.is_none_or(|(_, len)| run.1 > len)
		{
			best = Some(run);
		}
	}
	if let Some(run) = current
		&& best.is_none_or(|(_, len)| run.1 > len)
	{
		best = Some(run);
	}
	let best = best.filter(|(_, len)| *len >= 2);

	let mapped_or_compatible =
		|base: usize, len: usize| base == 0 && (len == 6 || (len == 5 && words[5] == 0xffff));
	let mut out = String::new();
	for (index, word) in words.iter().enumerate() {
		if let Some((base, len)) = best
			&& index >= base
			&& index < base + len
		{
			if index == base {
				out.push(':');
			}
			continue;
		}
		if index != 0 {
			out.push(':');
		}
		if index == 6
			&& let Some((base, len)) = best
			&& mapped_or_compatible(base, len)
		{
			out.push_str(&format!("{}.{}.{}.{}", bytes[12], bytes[13], bytes[14], bytes[15]));
			break;
		}
		out.push_str(&format!("{word:x}"));
	}
	if let Some((base, len)) = best
		&& base + len == 8
	{
		out.push(':');
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_cidr_without_a_prefix_takes_its_classful_prefix() {
		let cidr = |text: &str| parse_cidr(text).expect("a cidr").cidr_text();
		assert_eq!(cidr("10"), "10.0.0.0/8");
		assert_eq!(cidr("192.168.1"), "192.168.1.0/24");
		assert_eq!(cidr("128.1"), "128.1.0.0/16");
		assert_eq!(cidr("128"), "128.0.0.0/16");
		assert_eq!(cidr("10.1.2.3"), "10.1.2.3/32");
		assert!(parse_cidr("192.168.1.5/24").is_err());
	}

	#[test]
	fn an_abbreviated_ipv4_address_is_read_with_a_prefix() {
		assert_eq!(parse("10/8").expect("abbreviated").cidr_text(), "10.0.0.0/8");
		assert_eq!(parse_cidr("192.168.1/24").expect("abbreviated").cidr_text(), "192.168.1.0/24");
		assert!(parse("10").is_err());
		assert!(parse("192.168.1").is_err());
		assert!(parse("::10.1/96").is_err());
	}

	fn inet(text: &str) -> String {
		parse(text).expect("an inet").inet_text()
	}

	#[test]
	fn the_sort_key_orders_as_compare_does() {
		let texts = [
			"::1",
			"::/0",
			"10.0.0.2",
			"10.0.0.0/8",
			"9.255.255.255",
			"10.0.0.0/16",
			"10.0.0.1",
			"10.1.0.0/16",
			"10.0.255.255/8",
			"0.0.0.0/0",
			"192.168.1.5/24",
			"192.168.1.0/24",
			"192.168.0.0/16",
			"2001:db8::/32",
			"2001:db8::1",
		];
		let networks: Vec<Network> = texts.iter().map(|t| parse(t).expect("a network")).collect();
		for a in &networks {
			for b in &networks {
				assert_eq!(sort_key(a).cmp(&sort_key(b)), compare(a, b), "{a:?} against {b:?}");
			}
		}
	}

	#[test]
	fn inet_keeps_host_bits_and_drops_a_full_length_prefix() {
		assert_eq!(inet("192.168.1.5"), "192.168.1.5");
		assert_eq!(inet("192.168.1.5/32"), "192.168.1.5");
		assert_eq!(inet("192.168.1.5/24"), "192.168.1.5/24");
		assert_eq!(inet("  10.0.0.1/0  "), "10.0.0.1/0");
		assert_eq!(inet("::1/128"), "::1");
		assert_eq!(inet("2001:db8::1/64"), "2001:db8::1/64");
	}

	#[test]
	fn ipv6_is_written_in_postgresql_canonical_form() {
		assert_eq!(inet("2001:0db8:0000:0000:0000:ff00:0042:8329"), "2001:db8::ff00:42:8329");
		assert_eq!(inet("2001:DB8::1"), "2001:db8::1");
		assert_eq!(inet("::"), "::");
		assert_eq!(inet("1:0:0:2:0:0:0:3"), "1:0:0:2::3");
		// Runs of one zero group are not shortened.
		assert_eq!(inet("1:0:2:0:3:0:4:5"), "1:0:2:0:3:0:4:5");
		assert_eq!(inet("::ffff:1.2.3.4"), "::ffff:1.2.3.4");
		assert_eq!(inet("::1.2.3.4"), "::1.2.3.4");
		assert_eq!(inet("1:2:3:4:5:6:7::"), "1:2:3:4:5:6:7:0");
		// The longest run is shortened; the shorter run of two is written out.
		assert_eq!(inet("fe80::1:0:0:0:1"), "fe80:0:0:1::1");
	}

	#[test]
	fn invalid_addresses_are_refused() {
		for text in [
			"",
			"10.0.0",
			"10.0.0.256",
			"10.0.0.01",
			"10.0.0.1.5",
			"10.0.0.1/33",
			"10.0.0.1/",
			"10.0.0.1/-1",
			"10.0.0.1/+1",
			"10.0.0.1/1000",
			"10..0.1",
			"::1::2",
			":1::2",
			"1:::2",
			"1:2:3:4:5:6:7:8:9",
			"1:2:3:4:5:6:7:8::",
			"12345::1",
			"g::1",
			"::1.2.3.4.5",
			"1.2.3.4::1",
			"::1/129",
			"example.com",
		] {
			assert!(parse(text).is_err(), "{text:?} should be refused");
		}
	}

	#[test]
	fn cidr_always_writes_its_prefix_and_refuses_host_bits() {
		let net = parse_cidr("10.1.2.0/24").expect("a cidr");
		assert_eq!(net.cidr_text(), "10.1.2.0/24");
		assert_eq!(parse_cidr("10.1.2.3").expect("a host").cidr_text(), "10.1.2.3/32");
		assert_eq!(parse_cidr("2001:db8::/32").expect("v6").cidr_text(), "2001:db8::/32");
		assert_eq!(parse_cidr("0.0.0.0/0").expect("default").cidr_text(), "0.0.0.0/0");
		let err = parse_cidr("10.1.2.3/24").unwrap_err();
		assert!(err.reason.contains("bits set to right of mask"), "{err}");
		assert!(parse_cidr("2001:db8::1/32").is_err());
	}

	#[test]
	fn compare_orders_by_family_then_network_then_prefix_then_address() {
		let n = |text: &str| parse(text).expect("a network");
		let less = |a: &str, b: &str| compare(&n(a), &n(b)) == Ordering::Less;
		// IPv4 sorts before IPv6 whatever the addresses.
		assert!(less("255.255.255.255", "::"));
		// Networks sort by their shared bits first: 10.0.0.0/8 precedes 10.0.0.0/16.
		assert!(less("10.0.0.0/8", "10.0.0.0/16"));
		// Same network, shorter prefix first, so 10.1.0.0/16 precedes 10.1.2.3/32.
		assert!(less("10.1.0.0/16", "10.1.2.3/32"));
		// Equal networks and prefixes order by the whole address.
		assert!(less("10.1.2.3/24", "10.1.2.4/24"));
		assert_eq!(compare(&n("10.1.2.3/24"), &n("10.1.2.3/24")), Ordering::Equal);
		// The shared bits decide before the prefix: 10.0.0.0/24 follows 10.0.1.0/16, which covers
		// the same first 16 bits and has the shorter prefix.
		assert_eq!(compare(&n("10.0.0.0/24"), &n("10.0.1.0/16")), Ordering::Greater);
		// A smaller network sorts first even with a longer prefix on the later one.
		assert!(less("9.0.0.0/8", "10.0.0.0/16"));
		assert!(less("9.255.255.255", "10.0.0.0"));
	}

	#[test]
	fn host_bits_is_false_for_a_clean_network_and_true_otherwise() {
		assert!(!host_bits_set(&parse("10.0.0.0/8").expect("net")));
		assert!(host_bits_set(&parse("10.0.0.1/8").expect("net")));
		assert!(!host_bits_set(&parse("10.0.0.1/32").expect("host")));
	}
}
