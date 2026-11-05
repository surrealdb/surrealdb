//! The binary format of `macaddr` (6 octets) and `macaddr8` (8 octets): the octets as they are,
//! with no header.

use super::{CodecError, codec_error};

/// Encode a 6-octet `macaddr`.
pub fn encode_macaddr(octets: [u8; 6]) -> Vec<u8> {
	octets.to_vec()
}

/// Decode a binary `macaddr`, which is exactly 6 bytes.
pub fn decode_macaddr(bytes: &[u8]) -> Result<[u8; 6], CodecError> {
	fixed(bytes, "macaddr")
}

/// Encode an 8-octet `macaddr8`.
pub fn encode_macaddr8(octets: [u8; 8]) -> Vec<u8> {
	octets.to_vec()
}

/// Decode a binary `macaddr8`, which is exactly 8 bytes.
pub fn decode_macaddr8(bytes: &[u8]) -> Result<[u8; 8], CodecError> {
	fixed(bytes, "macaddr8")
}

fn fixed<const N: usize>(bytes: &[u8], name: &str) -> Result<[u8; N], CodecError> {
	<[u8; N]>::try_from(bytes)
		.map_err(|_| codec_error(format!("a {name} value is {N} bytes, not {}", bytes.len())))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn macaddr_is_its_six_octets() {
		let octets = [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03];
		assert_eq!(encode_macaddr(octets), octets.to_vec());
		assert_eq!(decode_macaddr(&octets).expect("decodes"), octets);
		assert!(decode_macaddr(&octets[..5]).is_err());
		assert!(decode_macaddr(&[0; 7]).is_err());
	}

	#[test]
	fn macaddr8_is_its_eight_octets() {
		let octets = [0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03];
		assert_eq!(encode_macaddr8(octets), octets.to_vec());
		assert_eq!(decode_macaddr8(&octets).expect("decodes"), octets);
		assert!(decode_macaddr8(&octets[..6]).is_err());
		assert!(decode_macaddr8(&[0; 9]).is_err());
	}
}
