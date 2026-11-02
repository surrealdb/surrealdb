//! The 32-bit PostgreSQL identifier types on the wire: `oid`, the `reg*` aliases, `xid`, and
//! `cid`. The binary form is the value as a big-endian `uint32`. The text form is the decimal
//! digits of the value, which the caller writes with `to_string`.

/// The binary form of a 32-bit identifier.
pub fn oid_binary(value: u32) -> [u8; 4] {
	value.to_be_bytes()
}

/// The identifier of a binary value of this family. `None` when the value is not four bytes.
pub fn oid_from_binary(bytes: &[u8]) -> Option<u32> {
	let array: [u8; 4] = bytes.try_into().ok()?;
	Some(u32::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn binary_is_big_endian_u32() {
		assert_eq!(oid_binary(16_384), [0, 0, 0x40, 0]);
		assert_eq!(oid_binary(u32::MAX), [0xff; 4]);
		assert_eq!(oid_from_binary(&oid_binary(u32::MAX)), Some(u32::MAX));
		assert_eq!(oid_from_binary(&[0; 3]), None);
		assert_eq!(oid_from_binary(&[0; 5]), None);
	}
}
