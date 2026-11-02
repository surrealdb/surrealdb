//! The binary format of `inet` and `cidr`: the address family, the prefix length, a flag that is
//! 1 for `cidr`, the address length, then the address octets. The family is 2 for IPv4 and 3 for
//! IPv6, and the address length is 4 or 16 to match it.

use super::{CodecError, codec_error};

/// The family code of an IPv4 address (`AF_INET`).
pub const AF_INET: u8 = 2;
/// The family code of an IPv6 address (`AF_INET6`).
pub const AF_INET6: u8 = 3;

/// The parts of a binary `inet` or `cidr` value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkBinary {
	pub family: u8,
	pub bits: u8,
	pub is_cidr: bool,
	pub address: Vec<u8>,
}

/// Encode an address of 4 octets (IPv4) or 16 octets (IPv6) with its prefix length.
pub fn encode(address: &[u8], bits: u8, is_cidr: bool) -> Result<Vec<u8>, CodecError> {
	let (family, max_bits) = match address.len() {
		4 => (AF_INET, 32),
		16 => (AF_INET6, 128),
		other => {
			return Err(codec_error(format!("a network address has 4 or 16 octets, not {other}")));
		}
	};
	if bits > max_bits {
		return Err(codec_error(format!("prefix length {bits} exceeds {max_bits} bits")));
	}
	let mut out = Vec::with_capacity(4 + address.len());
	out.extend_from_slice(&[family, bits, u8::from(is_cidr), address.len() as u8]);
	out.extend_from_slice(address);
	Ok(out)
}

/// Decode a binary `inet` or `cidr` value, checking that the family, the address length, the
/// prefix length, and the `cidr` flag agree.
pub fn decode(bytes: &[u8]) -> Result<NetworkBinary, CodecError> {
	let [family, bits, is_cidr, address_length, address @ ..] = bytes else {
		return Err(codec_error("a network value has a 4-byte header"));
	};
	let (max_bits, octets) = match *family {
		AF_INET => (32, 4),
		AF_INET6 => (128, 16),
		other => return Err(codec_error(format!("unknown network family {other}"))),
	};
	if usize::from(*address_length) != octets || address.len() != octets {
		return Err(codec_error(format!(
			"a family {family} address has {octets} octets, not {}",
			address.len()
		)));
	}
	if *bits > max_bits {
		return Err(codec_error(format!("prefix length {bits} exceeds {max_bits} bits")));
	}
	let is_cidr = match *is_cidr {
		0 => false,
		1 => true,
		other => return Err(codec_error(format!("the cidr flag must be 0 or 1, not {other}"))),
	};
	Ok(NetworkBinary {
		family: *family,
		bits: *bits,
		is_cidr,
		address: address.to_vec(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn an_ipv4_inet_encodes_its_header_then_its_octets() {
		let bytes = encode(&[192, 168, 1, 5], 24, false).expect("encodes");
		assert_eq!(bytes, vec![2, 24, 0, 4, 192, 168, 1, 5]);
		assert_eq!(
			decode(&bytes).expect("decodes"),
			NetworkBinary {
				family: AF_INET,
				bits: 24,
				is_cidr: false,
				address: vec![192, 168, 1, 5],
			}
		);
	}

	#[test]
	fn an_ipv6_cidr_round_trips_with_the_flag_set() {
		let mut address = [0u8; 16];
		address[0] = 0x20;
		address[1] = 0x01;
		address[15] = 1;
		let bytes = encode(&address, 64, true).expect("encodes");
		assert_eq!(&bytes[..4], &[3, 64, 1, 16]);
		let decoded = decode(&bytes).expect("decodes");
		assert_eq!(decoded.family, AF_INET6);
		assert!(decoded.is_cidr);
		assert_eq!(decoded.address, address.to_vec());
	}

	#[test]
	fn an_address_of_another_length_is_refused_on_encode() {
		assert!(encode(&[1, 2, 3], 8, false).is_err());
		assert!(encode(&[], 0, false).is_err());
	}

	#[test]
	fn a_prefix_beyond_the_family_is_refused_on_both_sides() {
		assert!(encode(&[10, 0, 0, 1], 33, false).is_err());
		assert!(decode(&[2, 33, 0, 4, 10, 0, 0, 1]).is_err());
		assert!(decode(&[3, 129, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
	}

	#[test]
	fn a_malformed_header_is_refused() {
		assert!(decode(&[]).is_err());
		assert!(decode(&[2, 24, 0]).is_err());
		assert!(decode(&[9, 24, 0, 4, 1, 2, 3, 4]).is_err(), "unknown family");
		assert!(decode(&[2, 24, 0, 16, 1, 2, 3, 4]).is_err(), "length disagrees with family");
		assert!(decode(&[2, 24, 0, 4, 1, 2, 3]).is_err(), "short address");
		assert!(decode(&[2, 24, 0, 4, 1, 2, 3, 4, 5]).is_err(), "long address");
		assert!(decode(&[2, 24, 2, 4, 1, 2, 3, 4]).is_err(), "cidr flag other than 0 or 1");
	}
}
