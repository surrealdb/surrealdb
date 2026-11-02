//! The binary form of the PostgreSQL `money` type: the amount in cents as a big-endian `int8`.
//! The text form is the declared type's own, in
//! `surrealdb_sqlplan::sql_types::text_form::money`.

/// The binary form of an amount given in cents.
pub fn money_binary(cents: i64) -> [u8; 8] {
	cents.to_be_bytes()
}

/// The cents of a binary `money` value. `None` when the value is not eight bytes.
pub fn money_from_binary(bytes: &[u8]) -> Option<i64> {
	let array: [u8; 8] = bytes.try_into().ok()?;
	Some(i64::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn binary_is_big_endian_cents() {
		assert_eq!(money_binary(123_456), [0, 0, 0, 0, 0, 1, 226, 64]);
		assert_eq!(money_binary(-1), [0xff; 8]);
		assert_eq!(money_from_binary(&money_binary(-123_456)), Some(-123_456));
		assert_eq!(money_from_binary(&[0; 7]), None);
		assert_eq!(money_from_binary(&[0; 9]), None);
	}
}
