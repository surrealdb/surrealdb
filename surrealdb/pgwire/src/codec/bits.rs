//! The binary format of `bit` and `bit varying`: the number of bits as a 4-byte big-endian
//! signed integer, then the bits packed most significant first into `ceil(bits / 8)` bytes. The
//! unused low bits of the last byte are zero on encode and ignored on decode.

use super::{CodecError, codec_error};

/// Encode a text of `0` and `1` as a binary bit string.
pub fn encode(bits: &str) -> Result<Vec<u8>, CodecError> {
	if let Some(bad) = bits.chars().find(|c| *c != '0' && *c != '1') {
		return Err(codec_error(format!("\"{bad}\" is not a binary digit")));
	}
	let length = i32::try_from(bits.len())
		.map_err(|_| codec_error("a bit string is longer than 2147483647 bits"))?;
	let mut out = length.to_be_bytes().to_vec();
	let mut byte = 0u8;
	for (index, digit) in bits.bytes().enumerate() {
		if digit == b'1' {
			byte |= 0x80 >> (index % 8);
		}
		if index % 8 == 7 {
			out.push(byte);
			byte = 0;
		}
	}
	if !bits.len().is_multiple_of(8) {
		out.push(byte);
	}
	Ok(out)
}

/// Decode a binary bit string to its text of `0` and `1`.
pub fn decode(bytes: &[u8]) -> Result<String, CodecError> {
	let Some((length_bytes, data)) = bytes.split_first_chunk::<4>() else {
		return Err(codec_error("a bit string has a 4-byte length"));
	};
	let length = usize::try_from(i32::from_be_bytes(*length_bytes))
		.map_err(|_| codec_error("a bit string has a negative length"))?;
	if data.len() != length.div_ceil(8) {
		return Err(codec_error(format!(
			"a bit string of {length} bits needs {} bytes, not {}",
			length.div_ceil(8),
			data.len()
		)));
	}
	Ok((0..length)
		.map(|index| {
			if data[index / 8] & (0x80 >> (index % 8)) != 0 {
				'1'
			} else {
				'0'
			}
		})
		.collect())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_bit_string_is_its_length_then_its_packed_bits() {
		assert_eq!(encode("101").expect("encodes"), vec![0, 0, 0, 3, 0b1010_0000]);
		assert_eq!(encode("").expect("encodes"), vec![0, 0, 0, 0]);
		assert_eq!(encode("1111111100000001").expect("encodes"), vec![0, 0, 0, 16, 0xff, 0x01]);
		assert_eq!(encode("100000000").expect("encodes"), vec![0, 0, 0, 9, 0x80, 0x00]);
	}

	#[test]
	fn decode_recovers_the_text_of_every_round_trip() {
		for text in ["", "0", "1", "10110", "11111111", "100000001", "0101010101010101010"] {
			let bytes = encode(text).expect("encodes");
			assert_eq!(decode(&bytes).expect("decodes"), text);
		}
	}

	#[test]
	fn decode_ignores_the_unused_bits_of_the_last_byte() {
		let bytes = [0, 0, 0, 3, 0b1011_1111];
		assert_eq!(decode(&bytes).expect("decodes"), "101");
	}

	#[test]
	fn a_text_that_is_not_bits_is_refused() {
		assert!(encode("102").is_err());
		assert!(encode("1 0").is_err());
		assert!(encode("x1").is_err());
	}

	#[test]
	fn a_malformed_binary_value_is_refused() {
		assert!(decode(&[0, 0, 0]).is_err(), "no full length");
		assert!(decode(&[0, 0, 0, 9, 0x80]).is_err(), "too few bytes for nine bits");
		assert!(decode(&[0, 0, 0, 1, 0x80, 0x00]).is_err(), "too many bytes for one bit");
		assert!(decode(&[0xff, 0xff, 0xff, 0xff]).is_err(), "negative length");
	}
}
