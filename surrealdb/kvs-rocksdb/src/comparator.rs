use std::cmp::Ordering;

pub const NAME: &str = "surrealdb.TimestampComparator";
pub const TIMESTAMP_SIZE: usize = size_of::<u64>();

/// Compare full keys with timestamps keys (key bytes + timestamp suffix).
///
/// Keys are first compared without their timestamp suffix. If the key
/// portions are equal, timestamps are compared in descending order
/// so that newer versions sort before older ones for the same key.
pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
	// Compare the keys lexicographically
	let ord = compare_without_ts(a, true, b, true);
	// If user keys are not equal, return the result
	if ord != Ordering::Equal {
		return ord;
	}
	// Extract the timestamp suffix
	let a = &a[a.len() - TIMESTAMP_SIZE..];
	let b = &b[b.len() - TIMESTAMP_SIZE..];
	// Compare the timestamps
	compare_ts(a, b).reverse()
}

/// Compare raw timestamp slices numerically in ascending order.
pub fn compare_ts(a: &[u8], b: &[u8]) -> Ordering {
	// Convert the timestamp slices to u64s
	let a = u64::from_le_bytes(a.try_into().expect("timestamp must be 8 bytes"));
	let b = u64::from_le_bytes(b.try_into().expect("timestamp must be 8 bytes"));
	// Compare the timestamps only
	a.cmp(&b)
}

/// Compare user keys with the timestamp suffix optionally stripped.
pub fn compare_without_ts(mut a: &[u8], a_has_ts: bool, mut b: &[u8], b_has_ts: bool) -> Ordering {
	// Strip the timestamp suffix if it exists
	if a_has_ts {
		a = &a[..a.len() - TIMESTAMP_SIZE];
	}
	if b_has_ts {
		b = &b[..b.len() - TIMESTAMP_SIZE];
	}
	// Compare the keys only
	a.cmp(b)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn make_key(key: &[u8], ts: u64) -> Vec<u8> {
		let mut v = key.to_vec();
		v.extend_from_slice(&ts.to_le_bytes());
		v
	}

	#[test]
	fn same_key_newer_first() {
		let a = make_key(b"hello", 10);
		let b = make_key(b"hello", 5);
		assert_eq!(compare(&a, &b), Ordering::Less, "newer timestamp should sort first");
	}

	#[test]
	fn different_keys_ordered_lexicographically() {
		let a = make_key(b"aaa", 100);
		let b = make_key(b"bbb", 1);
		assert_eq!(compare(&a, &b), Ordering::Less);
	}

	#[test]
	fn compare_ts_ascending() {
		let a = 5u64.to_le_bytes();
		let b = 10u64.to_le_bytes();
		assert_eq!(compare_ts(&a, &b), Ordering::Less);
	}

	#[test]
	fn compare_without_ts_strips_correctly() {
		let a = make_key(b"key", 999);
		let b = make_key(b"key", 1);
		assert_eq!(compare_without_ts(&a, true, &b, true), Ordering::Equal);
	}

	#[test]
	fn same_key_same_ts_is_equal() {
		let a = make_key(b"hello", 42);
		let b = make_key(b"hello", 42);
		assert_eq!(compare(&a, &b), Ordering::Equal);
	}

	#[test]
	fn max_timestamp_sorts_first() {
		let a = make_key(b"key", u64::MAX);
		let b = make_key(b"key", 0);
		assert_eq!(compare(&a, &b), Ordering::Less, "u64::MAX should sort before 0 (newer first)");
	}

	#[test]
	fn empty_key_with_timestamp() {
		let a = make_key(b"", 10);
		let b = make_key(b"", 5);
		assert_eq!(compare(&a, &b), Ordering::Less, "empty user keys still order by ts descending");
	}

	#[test]
	fn compare_without_ts_no_ts_flags() {
		let a = b"plain_key_a";
		let b = b"plain_key_b";
		assert_eq!(
			compare_without_ts(a, false, b, false),
			Ordering::Less,
			"without ts flags, raw byte comparison"
		);
		assert_eq!(compare_without_ts(a, false, a, false), Ordering::Equal);
	}

	#[test]
	fn compare_ts_equal() {
		let a = 100u64.to_le_bytes();
		let b = 100u64.to_le_bytes();
		assert_eq!(compare_ts(&a, &b), Ordering::Equal);
	}

	#[test]
	fn compare_ts_max_vs_zero() {
		let a = u64::MAX.to_le_bytes();
		let b = 0u64.to_le_bytes();
		assert_eq!(compare_ts(&a, &b), Ordering::Greater);
	}
}
