//! The MySQL `BIT(n)` type on the wire, for `n` from 1 to 64. Both protocols send the value as
//! big-endian bytes, `ceil(n / 8)` of them, most significant byte first. The text protocol sends
//! the same raw bytes as the binary protocol, not a digit string.

/// The bytes of a `BIT(width)` value: `ceil(width / 8)` big-endian bytes. `None` when the width
/// is outside 1 to 64 or the value needs more than `width` bits.
pub fn bit_bytes(value: u64, width: u32) -> Option<Vec<u8>> {
	if !(1..=64).contains(&width) || !fits(value, width) {
		return None;
	}
	let length = usize::try_from(width.div_ceil(8)).ok()?;
	Some(value.to_be_bytes()[8 - length..].to_vec())
}

/// The value of `BIT(width)` from its bytes. `None` when the width is outside 1 to 64, the
/// length is not `ceil(width / 8)`, or the value has bits beyond `width`.
pub fn bit_from_bytes(bytes: &[u8], width: u32) -> Option<u64> {
	if !(1..=64).contains(&width) {
		return None;
	}
	let length = usize::try_from(width.div_ceil(8)).ok()?;
	if bytes.len() != length {
		return None;
	}
	let value = bytes.iter().fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
	fits(value, width).then_some(value)
}

/// Whether `value` needs no more than `width` bits.
fn fits(value: u64, width: u32) -> bool {
	width >= 64 || value >> width == 0
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bytes_are_big_endian_with_ceiling_length() {
		assert_eq!(bit_bytes(1, 1), Some(vec![0x01]));
		assert_eq!(bit_bytes(0xAB, 8), Some(vec![0xAB]));
		assert_eq!(bit_bytes(0x1FF, 9), Some(vec![0x01, 0xFF]));
		assert_eq!(bit_bytes(0x0102_0304, 32), Some(vec![1, 2, 3, 4]));
		assert_eq!(bit_bytes(u64::MAX, 64), Some(vec![0xFF; 8]));
		assert_eq!(bit_bytes(0, 24), Some(vec![0, 0, 0]));
	}

	#[test]
	fn bytes_refuse_a_value_wider_than_the_type() {
		assert_eq!(bit_bytes(256, 8), None);
		assert_eq!(bit_bytes(2, 1), None);
		assert_eq!(bit_bytes(1, 0), None);
		assert_eq!(bit_bytes(1, 65), None);
	}

	#[test]
	fn value_reads_the_bytes_it_writes() {
		for (value, width) in [(1, 1), (0xAB, 8), (0x1FF, 9), (u64::MAX, 64), (0, 40)] {
			let bytes = bit_bytes(value, width).expect("the value fits");
			assert_eq!(bit_from_bytes(&bytes, width), Some(value), "{value} in {width}");
		}
	}

	#[test]
	fn value_refuses_a_wrong_length_or_extra_bits() {
		assert_eq!(bit_from_bytes(&[0x01, 0x00], 8), None);
		assert_eq!(bit_from_bytes(&[0x01], 9), None);
		assert_eq!(bit_from_bytes(&[0x02], 1), None);
		assert_eq!(bit_from_bytes(&[0x01], 0), None);
	}
}
