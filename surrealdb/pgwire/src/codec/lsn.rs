//! The binary form of the PostgreSQL `pg_lsn` type: the 64-bit position as a big-endian
//! `uint64`. The text form is the declared type's own, in
//! `surrealdb_sqlplan::sql_types::text_form::lsn`.

/// The binary form of a log sequence number.
pub fn lsn_binary(lsn: u64) -> [u8; 8] {
	lsn.to_be_bytes()
}

/// The log sequence number of a binary `pg_lsn` value. `None` when the value is not eight bytes.
pub fn lsn_from_binary(bytes: &[u8]) -> Option<u64> {
	let array: [u8; 8] = bytes.try_into().ok()?;
	Some(u64::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn binary_is_big_endian() {
		assert_eq!(
			lsn_binary(0x0123_4567_89AB_CDEF),
			[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
		);
		assert_eq!(lsn_from_binary(&lsn_binary(u64::MAX)), Some(u64::MAX));
		assert_eq!(lsn_from_binary(&[0; 4]), None);
	}
}
