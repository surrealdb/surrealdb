//! Range partitioning for parallel key-space scans.
//!
//! Splits a `[start, end)` byte-key range into disjoint, contiguous
//! sub-ranges so that independent workers can each scan a slice of the
//! range against a shared snapshot. The partition is computed by
//! interpolating across the first byte position where `start` and `end`
//! differ — for SurrealDB keys, the common prefix encodes the
//! table+category and the variable bytes encode the record/index id, so
//! sharding on the first differing byte produces roughly equal-sized
//! shards over the id space.

/// Upper bound on the number of parallel shards used by a read-only
/// `count()`. Independently of how many CPUs are available, going beyond
/// this many shards tends to produce shards small enough that the per-shard
/// fixed costs (iterator setup, snapshot lookup, block index walk) dominate.
pub(super) const COUNT_PARALLEL_MAX_SHARDS: usize = 16;

/// Split `[start, end)` into up to `desired` disjoint, contiguous sub-ranges
/// for parallel scans. The returned shards always cover exactly the input
/// range — the first shard's lower bound is `start` and the last shard's
/// upper bound is `end`. Intermediate boundaries are placed by interpolating
/// across the first byte position where `start` and `end` differ, which is
/// the byte that actually carries the entropy in a SurrealDB range scan
/// (the common prefix is the table+category, and IDs vary in the bytes
/// after it).
///
/// Falls back to a single shard `(start, end)` when the range is too narrow
/// to split meaningfully — i.e. the input is empty, one side is a prefix of
/// the other, or the first differing byte differs by less than 2.
pub(super) fn shard_range(start: &[u8], end: &[u8], desired: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
	if desired <= 1 || start >= end {
		return vec![(start.to_vec(), end.to_vec())];
	}
	// Walk past the bytes shared by both bounds.
	let common_max = start.len().min(end.len());
	let mut p = 0;
	while p < common_max && start[p] == end[p] {
		p += 1;
	}
	// If one side is a prefix of the other, we can't interpolate a
	// monotonic boundary out of a single shared byte position.
	if p >= start.len() || p >= end.len() {
		return vec![(start.to_vec(), end.to_vec())];
	}
	let sb = start[p] as u32;
	let eb = end[p] as u32;
	// Need at least one byte value strictly between sb and eb to place a
	// boundary, so require a span of at least 2.
	if eb < sb + 2 {
		return vec![(start.to_vec(), end.to_vec())];
	}
	let span = eb - sb;
	let n = desired.min(span as usize);
	if n <= 1 {
		return vec![(start.to_vec(), end.to_vec())];
	}
	let prefix = &start[..p];
	let mut shards = Vec::with_capacity(n);
	let mut prev: Vec<u8> = start.to_vec();
	for i in 1..n {
		let b = sb + (span * i as u32 / n as u32);
		// Skip degenerate boundaries that would equal `start[..=p]`: when
		// `start` has further bytes after position `p`, `prefix+[sb]` is a
		// strict prefix of `start` and therefore compares less than it.
		if b == sb {
			continue;
		}
		let mut boundary = prefix.to_vec();
		boundary.push(b as u8);
		// Ensure boundaries are strictly increasing and stay inside `end`.
		if boundary.as_slice() > prev.as_slice() && boundary.as_slice() < end {
			shards.push((prev.clone(), boundary.clone()));
			prev = boundary;
		}
	}
	shards.push((prev, end.to_vec()));
	shards
}

#[cfg(test)]
mod tests {
	use super::shard_range;

	/// Assert that the shards form a contiguous, monotonically-increasing
	/// partition of `[start, end)`.
	fn assert_valid(shards: &[(Vec<u8>, Vec<u8>)], start: &[u8], end: &[u8]) {
		assert!(!shards.is_empty(), "expected at least one shard");
		assert_eq!(shards.first().unwrap().0.as_slice(), start, "first shard lo == start");
		assert_eq!(shards.last().unwrap().1.as_slice(), end, "last shard hi == end");
		for shard in shards {
			assert!(shard.0 < shard.1, "shard lo < hi: {:?} >= {:?}", shard.0, shard.1);
		}
		for pair in shards.windows(2) {
			assert_eq!(pair[0].1, pair[1].0, "adjacent shards must touch exactly");
		}
	}

	#[test]
	fn empty_range_returns_single_shard() {
		let s = shard_range(b"abc", b"abc", 8);
		assert_eq!(s.len(), 1);
	}

	#[test]
	fn reversed_range_returns_single_shard() {
		let s = shard_range(b"z", b"a", 8);
		assert_eq!(s.len(), 1);
	}

	#[test]
	fn one_shard_when_desired_is_one() {
		let s = shard_range(b"\x00", b"\xFF", 1);
		assert_eq!(s.len(), 1);
		assert_valid(&s, b"\x00", b"\xFF");
	}

	#[test]
	fn full_byte_range_splits_into_desired_shards() {
		let s = shard_range(b"\x00", b"\xFF", 8);
		assert_eq!(s.len(), 8);
		assert_valid(&s, b"\x00", b"\xFF");
	}

	#[test]
	fn narrow_span_falls_back_to_single_shard() {
		// span == 1 (sb=0x05, eb=0x06), no room for an intermediate byte.
		let s = shard_range(b"\x05", b"\x06", 8);
		assert_eq!(s.len(), 1);
	}

	#[test]
	fn prefix_only_falls_back_to_single_shard() {
		// `start` is a prefix of `end`: no differing byte to interpolate.
		let s = shard_range(b"ab", b"abc", 8);
		assert_eq!(s.len(), 1);
	}

	#[test]
	fn shared_prefix_then_diverge() {
		// Common prefix "tbl\0*", IDs span [0x00..0xFF).
		let mut start = b"tbl\0*".to_vec();
		start.push(0x00);
		let mut end = b"tbl\0*".to_vec();
		end.push(0xFF);
		let s = shard_range(&start, &end, 16);
		assert_eq!(s.len(), 16);
		assert_valid(&s, &start, &end);
		// Every shard boundary must keep the table prefix intact.
		for (lo, hi) in &s {
			assert!(lo.starts_with(b"tbl\0*"));
			assert!(hi.starts_with(b"tbl\0*"));
		}
	}

	#[test]
	fn start_has_extra_bytes_after_pivot() {
		// start = prefix+[0x10, ...extra], end = prefix+[0x80].
		// No shard boundary may be <= start.
		let start = b"\x10\xAA\xBB".to_vec();
		let end = b"\x80".to_vec();
		let s = shard_range(&start, &end, 8);
		assert_valid(&s, &start, &end);
	}

	#[test]
	fn shards_cap_at_span() {
		// span == 3, so even desired=16 yields at most 3 shards.
		let s = shard_range(b"\x05", b"\x08", 16);
		assert!(s.len() <= 3);
		assert_valid(&s, b"\x05", b"\x08");
	}
}
